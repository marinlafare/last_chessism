// Real app, Research page and sidebar; API, auth and preset writes are mocked.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const read = name => readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url), 'utf8').then(JSON.parse)
const [base, scopes, charts] = await Promise.all(['appearance_registry', 'appearance_scopes', 'appearance_charts'].map(read))
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: Object.fromEntries(
  Object.entries(scopes.scopes).map(([scope, spec]) => [scope, { ...spec, targets: { ...scopes.common_targets, ...spec.targets } }])) }
const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-research-hover-')), profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9284'], { stdio: ['ignore', 'pipe', 'pipe'] })
let socket
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('Browser startup timeout')), 15000)
    const output = chunk => {
      const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/)
      if (match) { clearTimeout(timer); resolve(match[1]) }
    }
    browser.stdout.on('data', output); browser.stderr.on('data', output)
    browser.once('error', error => { clearTimeout(timer); reject(error) })
  })
  socket = new WebSocket(endpoint.replace(/\/$/, '') + '/session')
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject })
  const pending = new Map(); let id = 0
  socket.onmessage = ({ data }) => {
    const message = JSON.parse(data), request = pending.get(message.id)
    if (!request) return
    pending.delete(message.id); clearTimeout(request.timer)
    message.type === 'error' ? request.reject(Error(JSON.stringify(message))) : request.resolve(message.result)
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const key = ++id, timer = setTimeout(() => { pending.delete(key); reject(Error('Timeout: ' + method)) }, 20000)
    pending.set(key, { resolve, reject, timer }); socket.send(JSON.stringify({ id: key, method, params }))
  })
  await send('session.new', { capabilities: {} })
  await send('script.addPreloadScript', { functionDeclaration: `() => { window.researchHoverRegistry = ${JSON.stringify(registry)} }` })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1100 } })
  const load = () => send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/research-hover.html', wait: 'complete' })
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw Error(JSON.stringify(response))
    return response.result.value
  }
  const assert = async expression => { if (!await evaluate(expression)) throw Error('Failed: ' + expression) }
  const assertInlineBrand = () => assert(`(() => {
    const brand=document.querySelector('.rail-brand').getBoundingClientRect();
    const photo=document.querySelector('.left-rail-icon').getBoundingClientRect();
    const name=document.querySelector('.rail-brand span').getBoundingClientRect();
    const inline=photo.width>34 && photo.width<=72 && Math.abs(photo.width-photo.height)<1 && photo.right<name.left
      && Math.abs(photo.top+photo.height/2-name.top-name.height/2)<1
      && Math.abs((photo.left+name.right)/2-brand.left-brand.width/2)<1
      && (innerWidth<=720 || name.right-photo.left>=brand.width*.9);
    if (!inline) throw Error(JSON.stringify({brand,photo,name}));
    return inline;
  })()`)
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) {
      if (await evaluate(expression)) return
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw Error('Timeout: ' + expression + '; ' + await evaluate(`JSON.stringify({ ...researchHoverTest,
      cards: [...document.querySelectorAll('.research-hub-card')].map(n=>({id:n.dataset.styleId,hover:n.matches(':hover'),bg:getComputedStyle(n).backgroundColor,border:getComputedStyle(n).borderTopColor,rect:n.getBoundingClientRect().toJSON()})),
      css:document.querySelector('[data-appearance-overrides]')?.textContent })`))
  }
  const selector = id => `[data-style-scope="research"][data-style-id="${id}"]`
  const style = (id, property) => `getComputedStyle(document.querySelector(${JSON.stringify(selector(id))})).${property}`
  const click = text => evaluate(`[...document.querySelectorAll('button')].find(n=>n.textContent===${JSON.stringify(text)}).click()`)
  const open = async id => {
    await evaluate(`document.querySelector(${JSON.stringify(selector(id))}).dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:1100,clientY:400}))`)
    await waitFor(`document.querySelector('.appearance-editor header strong')?.textContent===${JSON.stringify(scopes.scopes.research.targets[id].label)}`)
  }
  const set = async (property, value) => {
    await evaluate(`(() => {
      const n=document.querySelector('#appearance-'+${JSON.stringify(property)});
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,${JSON.stringify(String(value))});
      n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}));
    })()`)
    // RGB edits intentionally retain the current alpha. Make test swatches
    // opaque with the real control, rather than relying on the theme's alpha.
    await evaluate(`(() => {
      const n=document.querySelector('#appearance-'+${JSON.stringify(property)}).closest('fieldset').querySelector('input[type=range]');
      if(!n)return;
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,'100');
      n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}));
    })()`)
  }
  const move = async (id = null) => {
    const point = id ? JSON.parse(await evaluate(`JSON.stringify((() => {
      const n=document.querySelector(${JSON.stringify(selector(id))});n.scrollIntoView({block:'center'});
      const r=n.getBoundingClientRect();return {x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)};
    })())`)) : { x: 1435, y: 5 }
    await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [{ type: 'pointerMove', origin: 'viewport', ...point }] }] })
  }
  await load()
  await waitFor("document.querySelectorAll('.research-hub-card').length===4 && document.querySelector('.appearance-account-controls button')?.disabled===false")
  await assert(`!document.querySelector('a[href="/hegel"]') && document.querySelectorAll('.rail-nav .rail-btn').length===7`)
  await waitFor(`document.querySelector('.left-rail-icon')?.naturalWidth>0`)
  await assertInlineBrand()
  await send('browsingContext.setViewport', { context, viewport: { width: 1000, height: 650 } })
  await evaluate(`document.querySelector('.rail-nav').lastElementChild.scrollIntoView({block:'nearest'})`)
  await assert(`document.querySelector('.rail-nav').lastElementChild.getBoundingClientRect().bottom <= document.querySelector('.superadmin-account-controls').getBoundingClientRect().top`)
  await assert(`document.querySelector('.superadmin-account-controls').getBoundingClientRect().width>100 && document.documentElement.scrollWidth<=innerWidth+1`)
  await assertInlineBrand()
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1100 } })
  await assert(`(() => { const box=document.querySelector('.research-hub-hero').getBoundingClientRect(), parent=document.querySelector('.research-hub-main').getBoundingClientRect();
    return Math.abs(box.width-parent.width*.5)<1 && Math.abs(box.left+box.width/2-parent.left-parent.width/2)<1 })()`)
  await click('Edit appearance')
  await open('intro.panel')
  await assert(`document.querySelector('#appearance-researchIntroWidth').value==='50'`)
  for (const property of base.universalProperties) await assert(`!!document.querySelector('#appearance-${property}')`)
  await set('researchIntroWidth', 65)
  await set('margin', 12); await set('marginLeft', 28)
  await assert(`${style('intro.panel','marginTop')}==='12px' && ${style('intro.panel','marginLeft')}==='28px' && ${style('intro.panel','marginRight')}==='12px'`)
  await set('margin', 8)
  await assert(`${style('intro.panel','margin')}==='8px'`)
  await assert(`Math.abs(document.querySelector('.research-hub-hero').getBoundingClientRect().width-document.querySelector('.research-hub-main').getBoundingClientRect().width*.65)<1`)
  await evaluate("document.querySelector('[aria-label=\"Close appearance popup\"]').click()")
  const navigation = await evaluate("getComputedStyle(document.querySelector('.rail-btn.active')).cssText + getComputedStyle(document.querySelector('.rail-btn.active')).color")
  for (const [id, spec] of Object.entries(scopes.scopes.research.targets)) {
    await open(id)
    for (const property of spec.properties.filter(p => p.startsWith('hover'))) {
      await assert(`!!document.querySelector('#appearance-${property}')`)
    }
  }
  for (const area of ['coefficient', 'matrices', 'algorithms', 'salience']) {
    const id = `area.${area}.panel`
    await open(id); await set('hoverBackground', '#123456'); await set('hoverBorderColor', '#abcdef')
    await evaluate("document.querySelector('[aria-label=\"Close appearance popup\"]').click()")
    await move(id)
    await waitFor(`${style(id, 'backgroundColor')}==='rgb(18, 52, 86)' && ${style(id, 'borderTopColor')}==='rgb(171, 205, 239)'`)
    await assert(`Array.from(document.querySelectorAll('.research-hub-card')).filter(n=>n.dataset.styleId!==${JSON.stringify(id)}).every(n=>getComputedStyle(n).backgroundColor!=='rgb(18, 52, 86)')`)
    await move(); await waitFor(`${style(id, 'backgroundColor')}!=='rgb(18, 52, 86)'`)
  }
  for (const id of ['intro.heading', 'area.coefficient.eyebrow', 'area.matrices.title', 'area.algorithms.text', 'area.salience.action']) {
    await open(id); await set('hoverColor', '#fedcba'); await set('hoverBackground', '#345678')
    await evaluate("document.querySelector('[aria-label=\"Close appearance popup\"]').click()")
    await move(id)
    await waitFor(`${style(id, 'color')}==='rgb(254, 220, 186)' && ${style(id, 'backgroundColor')}==='rgb(52, 86, 120)'`)
    await move(); await waitFor(`${style(id, 'color')}!=='rgb(254, 220, 186)'`)
  }
  for (const id of ['intro.panel', 'areas.panel']) {
    await open(id); await set('borderWidth', 2); await set('hoverBorderColor', '#abcdef'); await set('hoverBackground', '#123456')
    await evaluate("document.querySelector('[aria-label=\"Close appearance popup\"]').click()")
    await move(id); await waitFor(`${style(id, 'borderTopColor')}==='rgb(171, 205, 239)' && ${style(id, 'backgroundColor')}==='rgb(18, 52, 86)'`)
    await move()
  }
  await assert(`getComputedStyle(document.querySelector('.rail-btn.active')).cssText + getComputedStyle(document.querySelector('.rail-btn.active')).color===${JSON.stringify(navigation)}`)
  for (const [target, color] of [['account.online', '#224466'], ['account.logout', '#553322']]) {
    await evaluate(`document.querySelector('[data-style-scope="navigation"][data-style-id="${target}"]').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:100,clientY:600}))`)
    await waitFor(`document.querySelector('.appearance-editor header strong')?.textContent===${JSON.stringify(scopes.scopes.navigation.targets[target].label)}`)
    for (const property of ['background', 'hoverBackground', 'color', 'hoverColor', 'fontFamily', 'fontSize', 'textAlign', 'borderColor', 'hoverBorderColor', 'borderWidth', 'radius', 'padding']) {
      await assert(`!!document.querySelector('#appearance-${property}')`)
    }
    await set('background', color)
    await evaluate(`(() => {const n=document.querySelector('#appearance-textAlign');Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype,'value').set.call(n,'left');n.dispatchEvent(new Event('change',{bubbles:true}))})()`)
    await evaluate("document.querySelector('[aria-label=\"Close appearance popup\"]').click()")
    await assert(`getComputedStyle(document.querySelector('[data-style-id="${target}"]')).textAlign==='left'`)
  }
  await assert(`getComputedStyle(document.querySelector('.admin-online-status span:last-child')).textAlign==='left'`)
  await click('Save styles'); await waitFor("!document.querySelector('.appearance-account-controls').textContent.includes('*')")
  await load(); await waitFor("document.querySelector('.appearance-account-controls button')?.disabled===false")
  await assert(`${style('intro.panel','margin')}==='8px' && getComputedStyle(document.querySelector('.research-hub-hero')).getPropertyValue('--research-intro-width').trim()==='65%'`)
  await click('Edit appearance'); await open('intro.panel')
  await set('margin', 20); await set('marginRight', 40)
  await evaluate("document.querySelector('#appearance-margin').closest('fieldset').querySelector('button').click()")
  await assert(`${style('intro.panel','margin')}==='8px'`)
  await evaluate("document.querySelector('[aria-label=\"Close appearance popup\"]').click()")
  await move('area.algorithms.text'); await waitFor(`${style('area.algorithms.text', 'color')}==='rgb(254, 220, 186)'`)
  await assert(`getComputedStyle(document.querySelector('.admin-online-status')).backgroundColor==='rgb(34, 68, 102)' && getComputedStyle(document.querySelector('.superadmin-logout')).backgroundColor==='rgb(85, 51, 34)'`)
  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 844 } })
  await assert(`document.documentElement.scrollWidth<=innerWidth+1`)
  await assertInlineBrand()
  await evaluate(`document.querySelector('.admin-online-status').click()`)
  await waitFor(`location.pathname==='/settings' && document.querySelector('.admin-online-status').getAttribute('aria-current')==='page'`)
  await evaluate(`document.querySelector('.superadmin-logout').click()`)
  await waitFor(`document.querySelector('.superadmin-gate')`)
  await assert("researchHoverTest.errors.length===0 && researchHoverTest.requests.every(r=>['/appearance','/auth/me','/auth/logout'].some(p=>r.path.endsWith(p)))")
  console.log('PASS: Research hover controls, sidebar account styling and alignment, mocked save/reload, enlarged inline brand, short/mobile layouts, Hegel removed, settings and logout; no live API access.')
} finally {
  socket?.close()
  if (browser.exitCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM')
    await Promise.race([stopped, new Promise(resolve => setTimeout(resolve, 3000))])
    if (browser.exitCode === null) { browser.kill('SIGKILL'); await stopped }
  }
  await rm(temporary, { recursive: true, force: true })
}
