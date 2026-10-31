// Real ranking components, synthetic responses only; no live DB/cloud access.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const readRegistry = name => readFile(new URL(`../../chessism_api/${name}`, import.meta.url), 'utf8').then(JSON.parse)
const [base, scopes, charts] = await Promise.all(['appearance_registry.json', 'appearance_scopes.json', 'appearance_charts.json'].map(readRegistry))
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: {} }
for (const [scope, spec] of Object.entries(scopes.scopes)) registry.scopes[scope] = {
  ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets },
}
const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-ranking-chart-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9282'], { stdio: ['ignore', 'pipe', 'pipe'] })
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
    pending.delete(message.id)
    message.type === 'error' ? request.reject(Error(JSON.stringify(message))) : request.resolve(message.result)
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const key = ++id; pending.set(key, { resolve, reject }); socket.send(JSON.stringify({ id: key, method, params }))
  })
  await send('session.new', { capabilities: {} })
  await send('script.addPreloadScript', { functionDeclaration: `() => { window.rankingRegistry = ${JSON.stringify(registry)} }` })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  const load = () => send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/ranking-chart.html', wait: 'complete' })
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw Error(JSON.stringify(response))
    return response.result.value
  }
  const assert = async expression => { if (!await evaluate(expression)) throw Error('Failed: ' + expression) }
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) {
      if (await evaluate(expression)) return
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw Error('Timeout: ' + expression + '; ' + await evaluate('document.body.innerText'))
  }
  const click = selector => evaluate(`document.querySelector(${JSON.stringify(selector)}).click()`)
  const ready = () => waitFor("document.querySelector('.character-ratings-chart')?.getAttribute('aria-busy') === 'false'")
  await send('browsingContext.setViewport', { context, viewport: { width: 1280, height: 900 } })
  await load()
  await waitFor("document.querySelectorAll('.character-rating-row:not(.loading)').length === 14")
  await assert("rankingTest.requests.length === 1 && rankingTest.requests[0].mode === 'blitz' && !document.querySelector('.character-ratings-chart')")
  const listHeight = await evaluate("document.querySelector('.main-characters-carousel').getBoundingClientRect().height")
  await click('.character-view-toggle'); await ready()
  await assert(`Math.abs(document.querySelector('.main-characters-carousel').getBoundingClientRect().height - ${listHeight}) < 1`)
  await assert("document.querySelector('.character-list-view').inert && getComputedStyle(document.querySelector('.character-list-view')).visibility === 'hidden'")
  await assert("document.querySelectorAll('.character-chart-player').length === 10 && document.querySelectorAll('.character-chart-bar').length === 29 && document.querySelectorAll('.character-chart-missing').length === 1")
  await assert("Array.from(document.querySelectorAll('.character-chart-player'),n=>n.dataset.player).join(',') === 'titled0,titled1,titled2,titled3,titled4,titled5,titled6,titled7,titled8,titled9'")
  await assert("Array.from(document.querySelector('.character-chart-player').querySelectorAll('[data-rating-mode]'),n=>n.dataset.ratingMode).join(',') === 'rapid,blitz,bullet'")
  await assert("(() => {const bars=[...document.querySelector('.character-chart-player').querySelectorAll('.character-chart-bar')],center=n=>n.x.baseVal.value+n.width.baseVal.value/2,baseline=document.querySelector('.character-chart-player-tick').y1.baseVal.value; return center(bars[1]) < center(bars[0]) && center(bars[0]) < center(bars[2]) && bars[1].x.baseVal.value+bars[1].width.baseVal.value > bars[2].x.baseVal.value && Math.abs(bars[0].width.baseVal.value/bars[1].width.baseVal.value - .75/.55)<.001 && bars.every(n=>Math.abs(n.y.baseVal.value+n.height.baseVal.value-baseline)<.001) && new Set(bars.map(n=>getComputedStyle(n).fill)).size === 3})()")
  await assert("(() => {const b=document.querySelector('[data-player=titled0] [data-rating-mode=blitz]').getBoundingClientRect(); return document.elementFromPoint(b.left+b.width*.2,b.bottom-10)?.dataset.ratingMode==='blitz' && document.elementFromPoint(b.left+b.width*.8,b.bottom-10)?.dataset.ratingMode==='bullet'})()")
  await assert("!document.querySelector('.character-chart-hitbox') && document.querySelectorAll('.character-chart-divider').length === 11")
  await assert("document.querySelectorAll('.character-chart-player .home-games-avatar').length === 10 && document.querySelector('.character-chart-player image').getAttribute('href') === '/chessism.jpg'")
  await waitFor("document.querySelector('[data-player=titled1] .home-games-avatar').dataset.avatarState === 'fallback'")
  await evaluate("document.querySelector('[data-player=titled2] image').dispatchEvent(new PointerEvent('pointermove',{bubbles:true,clientX:200,clientY:300}))")
  await waitFor("document.querySelector('[role=tooltip]')?.textContent.includes('default image for a player without a profile photo')")
  await assert("document.querySelector('[role=tooltip]').textContent === 'titled2\nThis is our default image for a player without a profile photo.'".replace('\n', '\\n'))
  await evaluate("document.querySelector('[data-player=titled0] image').dispatchEvent(new PointerEvent('pointermove',{bubbles:true,clientX:200,clientY:300}))")
  await waitFor("document.querySelector('[role=tooltip]')?.textContent === 'Long Player Name For Wrapping'")
  for (const [mode, expected, side] of [['blitz', 'Blitz: 3,200', 'left'], ['bullet', 'Bullet: 3,400', 'right'], ['rapid', 'Rapid: 3,600', 'center']]) {
    const selector = `[data-player=titled0] .character-chart-bar[data-rating-mode=${mode}]`
    const point = JSON.parse(await evaluate(`JSON.stringify((() => { const r=document.querySelector('${selector}').getBoundingClientRect(); return {x:Math.round(${side === 'left' ? 'r.left+r.width*.2' : side === 'right' ? 'r.right-r.width*.2' : 'r.left+r.width/2'}), y:Math.round(r.top+2)} })())`))
    await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [{ type: 'pointerMove', origin: 'viewport', ...point }] }] })
    await waitFor(`document.querySelector('[role=tooltip]')?.textContent === '${expected}'`)
    await assert(`document.querySelectorAll('.character-chart-bar:hover').length === 1 && document.querySelector('${selector}').matches(':hover')`)
    await assert(`Array.from(document.querySelectorAll('[data-player=titled0] .character-chart-bar')).every(n => n.dataset.ratingMode === '${mode}' ? getComputedStyle(n).stroke === 'rgb(255, 255, 255)' : getComputedStyle(n).stroke === 'rgb(3, 6, 9)')`)
  }
  await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [{ type: 'pointerMove', x: 10, y: 10 }] }] })
  await assert("document.querySelector('.character-chart-player').getAttribute('href') === '/?player=titled0' && !document.querySelector('.character-ratings-chart figcaption') && !document.querySelector('.character-chart-name') && ![...document.querySelectorAll('.character-chart-axis')].some(n=>n.textContent==='Player')")
  const ticks = await evaluate("Array.from(document.querySelectorAll('.character-chart-tick text'),n=>n.textContent).join(',')")
  await click('[aria-label="Next ten players"]'); await waitFor("document.querySelector('.character-chart-navigation svg').dataset.page === '1'")
  await assert(`document.querySelectorAll('.character-chart-player').length === 4 && document.querySelector('.character-chart-player').dataset.player === 'titled10' && document.querySelector('[aria-label="Next ten players"]').disabled && Array.from(document.querySelectorAll('.character-chart-tick text'),n=>n.textContent).join(',') === '${ticks}' && rankingTest.requests.length === 3`)
  await click('.character-view-toggle'); await click('.character-view-toggle'); await ready()
  await assert(`Math.abs(document.querySelector('.main-characters-carousel').getBoundingClientRect().height - ${listHeight}) < 1`)
  await assert("rankingTest.requests.length === 3 && document.querySelector('.character-chart-navigation svg').dataset.page === '1'")
  await click('.character-medal-toggle'); await ready()
  await assert("document.querySelectorAll('.character-chart-player').length === 2 && document.querySelector('.character-chart-player').dataset.player === 'untitled0' && document.querySelectorAll('.character-chart-bar').length === 6")
  await click('.character-medal-toggle'); await ready()
  await assert("rankingTest.requests.length === 7 && document.querySelector('.character-chart-navigation svg').dataset.page === '0'")
  await evaluate("document.querySelector('[data-player=titled0] .character-chart-bar[data-rating-mode=blitz]').focus()")
  await waitFor("document.querySelector('[role=tooltip]')?.textContent === 'Blitz: 3,200'")
  await evaluate("document.querySelector('.character-chart-player').focus()")
  await send('input.performActions', { context, actions: [{ type: 'key', id: 'keyboard', actions: [{ type: 'keyDown', value: '\uE007' }, { type: 'keyUp', value: '\uE007' }] }] })
  await waitFor("rankingTest.selected.at(-1) === 'titled0'")
  await evaluate("document.querySelectorAll('.character-chart-player')[1].dispatchEvent(new MouseEvent('click',{bubbles:true,cancelable:true,button:0}))")
  await assert("rankingTest.selected.at(-1) === 'titled1'")
  // The chart/list toggle sits immediately left of the professional medal.
  const controlsAligned = "(() => {const icon=document.querySelector('.character-view-toggle').getBoundingClientRect(),medal=document.querySelector('.character-medal-toggle').getBoundingClientRect(); return icon.right <= medal.left && medal.left-icon.right <= 12 && Math.abs(icon.top+icon.height/2-medal.top-medal.height/2) < 1})()"
  await assert(controlsAligned)
  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 844 } })
  await waitFor("Math.abs(document.querySelector('.character-chart-navigation svg').getBoundingClientRect().width - document.querySelector('.character-chart-navigation svg').viewBox.baseVal.width) < 1")
  await assert("document.documentElement.scrollWidth <= innerWidth && document.querySelectorAll('.character-chart-player').length === 10")
  const mobileHeight = await evaluate("document.querySelector('.main-characters-carousel').getBoundingClientRect().height")
  await click('.character-view-toggle')
  await assert(`Math.abs(document.querySelector('.main-characters-carousel').getBoundingClientRect().height - ${mobileHeight}) < 1 && !document.querySelector('.character-list-view').inert`)
  await click('.character-view-toggle'); await ready()
  await waitFor("Math.abs(document.querySelector('.character-chart-navigation svg').getBoundingClientRect().height - document.querySelector('.character-chart-navigation svg').viewBox.baseVal.height) < 1")
  await assert(`Math.abs(document.querySelector('.main-characters-carousel').getBoundingClientRect().height - ${mobileHeight}) < 1`)
  await assert(controlsAligned)
  await evaluate("document.querySelector('.character-chart-player').blur(); document.querySelector('.character-chart-player').dispatchEvent(new PointerEvent('pointerleave',{bubbles:true}))")
  if (process.env.RANKING_SCREENSHOT) {
    const screenshot = await send('browsingContext.captureScreenshot', { context })
    await writeFile(process.env.RANKING_SCREENSHOT, Buffer.from(screenshot.data, 'base64'))
  }
  const photosFit = "[...document.querySelectorAll('.character-chart-player')].every(player=>{const photo=player.querySelector('.home-games-avatar').getBoundingClientRect(),tick=player.querySelector('.character-chart-player-tick').getBoundingClientRect(),svg=document.querySelector('.character-chart-navigation svg').getBoundingClientRect();return photo.top>tick.bottom && photo.left>=svg.left && photo.right<=svg.right && photo.bottom<=svg.bottom+.1 && (tick.top-svg.top)/svg.height>.7})"
  await assert(photosFit)
  console.log('PASS ten overlapping groups, mode-prefixed rating tooltips, names only on photos, taller plot, stable panel height, paging, filters, selection and mobile layout')
  await send('browsingContext.setViewport', { context, viewport: { width: 1280, height: 900 } })
  await evaluate("[...document.querySelectorAll('button')].find(n=>n.textContent==='Edit appearance').click()")
  await evaluate("document.querySelector('.character-ratings-chart').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:600,clientY:200}))")
  await waitFor("document.querySelector('#appearance-padding') && document.querySelector('#appearance-radius')")
  const setField = (property, value) => evaluate(`(() => {
    const input = document.querySelector('#appearance-' + ${JSON.stringify(property)});
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, ${JSON.stringify(String(value))});
    input.dispatchEvent(new Event('input', { bubbles: true })); input.dispatchEvent(new Event('change', { bubbles: true }));
  })()`)
  await assert("!document.querySelector('#appearance-chartColor1') && !!document.querySelector('[data-appearance-target=\"characters.chart.tooltip\"]')")
  await setField('padding', 12); await setField('radius', 16); await setField('related-background', '#123456')
  await waitFor("getComputedStyle(document.querySelector('.character-ratings-chart')).paddingTop === '12px' && getComputedStyle(document.querySelector('.character-ratings-chart')).borderRadius === '16px' && getComputedStyle(document.querySelector('[role=tooltip]')).backgroundColor === 'rgb(18, 52, 86)'")
  const edit = async selector => {
    await click('[aria-label="Close appearance popup"]')
    await evaluate(`document.querySelector(${JSON.stringify(selector)}).dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:600,clientY:200}))`)
    await waitFor("!!document.querySelector('.appearance-editor')")
  }
  for (const [mode, hex, rgb] of [['blitz', '#123456', 'rgb(18, 52, 86)'], ['bullet', '#345678', 'rgb(52, 86, 120)'], ['rapid', '#56789a', 'rgb(86, 120, 154)']]) {
    await edit(`.character-chart-bar[data-rating-mode=${mode}]`)
    await setField('fill', hex)
    await waitFor(`getComputedStyle(document.querySelector('.character-chart-bar[data-rating-mode=${mode}]')).fill === '${rgb}' && getComputedStyle(document.querySelector('[data-legend-mode=${mode}] rect')).fill === '${rgb}'`)
    await assert(`Array.from(document.querySelectorAll('.character-chart-bar')).filter(n=>n.dataset.ratingMode!=='${mode}').every(n=>getComputedStyle(n).fill!=='${rgb}')`)
    await assert("document.querySelector('#appearance-barOpacity').min === '0' && document.querySelector('#appearance-barOpacity').max === '1' && document.querySelector('#appearance-barOpacity').value === '1'")
    await setField('barOpacity', .35)
    await waitFor(`Array.from(document.querySelectorAll('.character-chart-bar[data-rating-mode=${mode}], [data-legend-mode=${mode}] rect')).every(n=>getComputedStyle(n).opacity === '0.35')`)
    await assert(`Array.from(document.querySelectorAll('.character-chart-bar')).filter(n=>n.dataset.ratingMode!=='${mode}').every(n=>getComputedStyle(n).opacity==='1') && getComputedStyle(document.querySelector('.character-ratings-chart')).opacity==='1' && getComputedStyle(document.querySelector('.character-chart-player .home-games-avatar')).opacity==='1'`)
    await click('[aria-label="Restore saved bar opacity (0–1)"]')
    await waitFor(`getComputedStyle(document.querySelector('.character-chart-bar[data-rating-mode=${mode}]')).opacity==='1' && getComputedStyle(document.querySelector('[data-legend-mode=${mode}] rect')).opacity==='1'`)
  }
  await edit('.character-chart-player .home-games-avatar')
  await setField('photoCornerRadius', 12); await setField('imageOpacity', .5)
  await waitFor("getComputedStyle(document.querySelector('.character-chart-player .home-games-avatar')).opacity === '0.5' && getComputedStyle(document.querySelector('.character-chart-player .home-games-avatar-frame')).rx === '12px'")
  await edit('.character-chart-tick text')
  await setField('fill', '#abcdef')
  await waitFor("getComputedStyle(document.querySelector('.character-chart-tick text')).fill === 'rgb(171, 205, 239)'")
  await edit('.character-chart-divider')
  await setField('stroke', '#fedcba')
  await waitFor("getComputedStyle(document.querySelector('.character-chart-divider')).stroke === 'rgb(254, 220, 186)'")
  console.log('PASS independent mode colors and opacity mirrored by legend, opacity restore, chart padding/radius, tooltip, photo opacity/radius, tick labels and dividers')
  // Re-mount the fixture (not the live app) to discard its draft and caches.
  await evaluate('rankingTest.reset()')
  await waitFor("document.querySelectorAll('.character-rating-row:not(.loading)').length === 14")
  await evaluate('rankingTest.failBullet=true'); await click('.character-view-toggle'); await ready()
  await assert("document.querySelector('.character-chart-status').textContent.includes('bullet ratings unavailable') && document.querySelectorAll('.character-chart-bar').length === 19")
  await evaluate('rankingTest.failBullet=false'); await click('.character-chart-status button'); await ready()
  await assert("rankingTest.requests.length === 4 && document.querySelectorAll('.character-chart-bar').length === 29 && !document.querySelector('.character-chart-status')")
  await evaluate('rankingTest.empty=true'); await click('.character-medal-toggle'); await ready()
  await assert("!document.querySelector('.character-chart-player') && document.querySelector('[aria-label=\"Next ten players\"]').disabled && document.querySelector('.character-chart-empty').textContent.includes('No players') && rankingTest.errors.length === 0")
  await evaluate('rankingTest.empty=false; rankingTest.count=52'); await click('.character-medal-toggle'); await ready()
  await assert("document.querySelectorAll('.character-rating-row:not(.loading)').length === 50 && !!document.querySelector('.character-pagination')")
  const paginatedHeight = await evaluate("document.querySelector('.main-characters-carousel').getBoundingClientRect().height")
  await click('.character-view-toggle')
  await assert(`Math.abs(document.querySelector('.main-characters-carousel').getBoundingClientRect().height - ${paginatedHeight}) < 1`)
  await click('.character-view-toggle'); await ready()
  await assert(`Math.abs(document.querySelector('.main-characters-carousel').getBoundingClientRect().height - ${paginatedHeight}) < 1`)
  console.log('PASS partial mode failure, targeted retry and empty ranking state; no live API calls')
} finally {
  socket?.close()
  if (browser.exitCode === null) {
    const stopped = once(browser, 'exit')
    browser.kill('SIGTERM')
    await Promise.race([stopped, new Promise(resolve => setTimeout(resolve, 3000))])
    if (browser.exitCode === null) { browser.kill('SIGKILL'); await stopped }
  }
  await rm(temporary, { recursive: true, force: true })
}
