// All fetches are mocked by the fixture. This never contacts the live cloud API.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-stop-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9253'], { stdio: ['ignore', 'pipe', 'pipe'] })
let socket
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Browser startup timeout')), 15000)
    const output = chunk => {
      const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/)
      if (match) { clearTimeout(timer); resolve(match[1]) }
    }
    browser.stdout.on('data', output); browser.stderr.on('data', output)
    browser.once('error', error => { clearTimeout(timer); reject(error) })
  })
  socket = new WebSocket(endpoint.replace(/\/$/, '') + '/session')
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject })
  const requests = new Map(); let nextId = 0
  socket.onmessage = ({ data }) => {
    const response = JSON.parse(data), pending = requests.get(response.id)
    if (pending) { requests.delete(response.id); response.type === 'error' ? pending.reject(new Error(JSON.stringify(response))) : pending.resolve(response.result) }
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId; requests.set(id, { resolve, reject }); socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const read = name => readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url), 'utf8').then(JSON.parse)
  const [base, scopes, charts] = await Promise.all(['appearance_registry', 'appearance_scopes', 'appearance_charts'].map(read))
  const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: Object.fromEntries(
    Object.entries(scopes.scopes).map(([scope, spec]) => [scope, { ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets } }])) }
  await send('script.addPreloadScript', { functionDeclaration: `() => { window.cloudStyleRegistry=${JSON.stringify(registry)} }` })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  await send('browsingContext.setViewport', { context, viewport: { width: 1280, height: 1000 } })
  await send('browsingContext.navigate', { context, url: (process.env.CLOUD_UI_TEST_URL || 'http://localhost:6789') + '/tests/fixtures/cloudAnalysis.html?appearance=1', wait: 'complete' })
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const assert = async expression => { if (!await evaluate(expression)) throw new Error('Failed: ' + expression) }
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) {
      if (await evaluate(expression)) return
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw new Error('Timeout: ' + expression)
  }
  await waitFor("!!document.querySelector('.cloud-job-stop')")
  await assert("!document.querySelector('.cloud-job-dismiss') && document.querySelector('.cloud-job').lastElementChild.textContent.includes('History')")
  await evaluate("document.querySelector('.cloud-job-stop').focus(); document.querySelector('.cloud-job-stop').click()")
  await waitFor("document.querySelector('.cloud-stop-confirmation')")
  await assert("document.activeElement.dataset.styleId==='cloud.stop.cancel' && document.querySelector('.cloud-stop-confirmation').getAttribute('aria-modal')==='true'")
  await assert("!window.cloudTest.requests.some(r => r.path.endsWith('/stop-and-cleanup'))")
  await evaluate("document.querySelector('[data-style-id=\"cloud.stop.cancel\"]').click()")
  await waitFor("!document.querySelector('.cloud-stop-confirmation') && document.activeElement.classList.contains('cloud-job-stop')")
  await evaluate("document.querySelector('.cloud-job-stop').click()")
  await waitFor("document.querySelector('.cloud-stop-confirmation')")
  await evaluate("document.activeElement.dispatchEvent(new KeyboardEvent('keydown',{key:'Tab',shiftKey:true,bubbles:true,cancelable:true}))")
  await assert("document.activeElement.dataset.styleId==='cloud.stop.yes'")
  await evaluate("document.activeElement.dispatchEvent(new KeyboardEvent('keydown',{key:'Tab',bubbles:true,cancelable:true}))")
  await assert("document.activeElement.dataset.styleId==='cloud.stop.cancel'")
  await evaluate("document.activeElement.dispatchEvent(new KeyboardEvent('keydown',{key:'Escape',bubbles:true,cancelable:true}))")
  await waitFor("!document.querySelector('.cloud-stop-confirmation')")
  await assert("!window.cloudTest.requests.some(r => r.path.endsWith('/stop-and-cleanup'))")
  // Completion while the popup is open invalidates the confirmation.
  await evaluate("document.querySelector('.cloud-job-stop').click()")
  await waitFor("document.querySelector('.cloud-stop-confirmation')")
  await evaluate("cloudTest.job.status='cancelled'")
  await waitFor("!document.querySelector('.cloud-stop-confirmation')")
  await assert("!window.cloudTest.requests.some(r => r.path.endsWith('/stop-and-cleanup'))")
  await evaluate("cloudTest.job.status='running'")
  await waitFor("document.querySelector('.cloud-job-stop')")
  // Arrow navigation changes focus only; native Enter opens the focused VM.
  const key = value => send('input.performActions', {context,actions:[{type:'key',id:'keyboard',actions:[{type:'keyDown',value},{type:'keyUp',value}]}]})
  await evaluate("document.querySelectorAll('.cloud-vm-tile')[0].focus()")
  await key('\uE014')
  await assert("document.activeElement===document.querySelectorAll('.cloud-vm-tile')[1] && !document.querySelector('.cloud-vm-details')")
  await key('\uE007')
  await waitFor("document.querySelector('.cloud-vm-details header strong')?.textContent==='VM 2'")
  await key('\uE012')
  await assert("document.activeElement===document.querySelectorAll('.cloud-vm-tile')[0]")
  await key('\uE007')
  await waitFor("document.querySelector('.cloud-vm-details header strong')?.textContent==='VM 1'")
  // Independent VM/heading styling, including the universal margin controls.
  await evaluate("[...document.querySelectorAll('button')].find(n=>n.textContent==='Edit appearance').click()")
  for (const target of ['cloud.vm.toggle', 'cloud.job.vms', 'local.toggle', 'local.heading']) {
    await evaluate(`document.querySelector('[data-style-id="${target}"]').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:700,clientY:300}))`)
    await waitFor(`document.querySelector('.appearance-editor header strong')?.textContent===${JSON.stringify(scopes.scopes.positions.targets[target].label)}`)
    for (const property of ['borderColor', 'hoverBorderColor', 'radius', 'hoverBackground', ...base.universalProperties]) {
      await assert(`!!document.querySelector('#appearance-${property}')`)
    }
    if (target==='cloud.vm.toggle') {
      await assert("!!document.querySelector('#appearance-vmSelectedBorderColor') && !!document.querySelector('#appearance-vmSelectedBorderWidth')")
      await evaluate(`(() => {const n=document.querySelector('#appearance-vmSelectedBorderColor');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,'#abcdef');n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}))})()`)
      await assert("getComputedStyle(document.querySelector('.cloud-vm-tile[aria-expanded=true]')).outlineColor==='rgb(171, 205, 239)'")
    }
    await evaluate(`(() => {const n=document.querySelector('#appearance-hoverBorderColor');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,'#123456');n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}))})()`)
    await evaluate("document.querySelector('[aria-label=\"Close appearance popup\"]').click()")
    const point = JSON.parse(await evaluate(`JSON.stringify((()=>{const n=document.querySelector('[data-style-id="${target}"]');n.scrollIntoView({block:'center'});const r=n.getBoundingClientRect();return {x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())`))
    await send('input.performActions', {context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[{type:'pointerMove',origin:'viewport',...point}]}]})
    await waitFor(`getComputedStyle(document.querySelector('[data-style-id="${target}"]')).borderTopColor==='rgb(18, 52, 86)'`)
  }
  await evaluate("document.querySelector('.cloud-job-stop').click()")
  await waitFor("document.querySelector('.cloud-stop-confirmation')")
  await evaluate("cloudTest.holdStop=true;const yes=document.querySelector('[data-style-id=\"cloud.stop.yes\"]');yes.click();yes.click()")
  await waitFor("cloudTest.finishStop && !document.querySelector('.cloud-stop-confirmation')")
  await assert("window.cloudTest.requests.filter(r => r.path.endsWith('/stop-and-cleanup')).length===1")
  await evaluate("cloudTest.finishStop()")
  await waitFor("document.querySelector('.cloud-job-stop-status')?.textContent.includes('Stop')")
  await assert("document.querySelector('.cloud-job').dataset.styleId === 'cloud.job.stopping' && document.querySelector('.cloud-stop-indicator').dataset.styleId === 'cloud.job.stop-indicator'")
  await assert("getComputedStyle(document.querySelector('.cloud-stop-indicator')).backgroundColor === 'rgb(239, 68, 68)'")
  await assert("getComputedStyle(document.querySelector('.cloud-stop-indicator')).animationName === (matchMedia('(prefers-reduced-motion: reduce)').matches ? 'none' : 'cloud-stop-pulse')")
  await assert("document.querySelector('.cloud-stop-indicator').getBoundingClientRect().right > document.querySelector('.cloud-job-stop-status').getBoundingClientRect().right")
  await assert("window.cloudTest.requests.filter(r => r.path.endsWith('/stop-and-cleanup')).length === 1 && !document.querySelector('.cloud-job-stop') && !document.querySelector('.cloud-job-dismiss')")
  await evaluate("window.cloudTest.job.stop_cleanup.phase = 'cleaning'")
  await waitFor("document.querySelector('.cloud-job-stop-status')?.textContent.includes('Clean')")
  await evaluate("window.cloudTest.job.status = 'cancelled'; window.cloudTest.job.stop_cleanup.phase = 'complete'; window.cloudTest.job.stop_cleanup.finished_at = new Date().toISOString()")
  await waitFor("!!document.querySelector('.cloud-job-dismiss')")
  await assert("!document.querySelector('.cloud-stop-indicator') && document.querySelector('.cloud-job').dataset.styleId === 'cloud.job'")
  // The old automatic dismissal happened after ten seconds.
  await new Promise(resolve => setTimeout(resolve, 11000))
  await assert("!!document.querySelector('.cloud-job-dismiss') && !document.querySelector('.cloud-job-fading')")
  await evaluate("document.querySelector('.cloud-job-dismiss').click()")
  await waitFor("!document.querySelector('.cloud-job')")
  await assert("JSON.parse(localStorage.getItem('chessism.cloud-progress.review.v1')).dismissed.includes('cloud-test')")
  await evaluate("Object.assign(window.cloudTest.job, {id:'finished-test', created_at:new Date().toISOString(), status:'complete', imported:2082, stop_cleanup:null, runs:[{id:'done-run',status:'complete',batch_jobs:[]}]})")
  await waitFor("!!document.querySelector('.cloud-job-dismiss')")
  await assert("!document.querySelector('.cloud-job-stop') && document.querySelector('.cloud-job').lastElementChild.textContent.includes('History')")
  console.log('PASS: confirmation, stop phases, retained completion, explicit dismissal, saved acknowledgement; no live API writes.')
} finally {
  socket?.close()
  const stopped = once(browser, 'exit')
  browser.kill('SIGTERM')
  await Promise.race([stopped, new Promise(resolve => setTimeout(resolve, 3000))])
  if (browser.exitCode === null) { browser.kill('SIGKILL'); await stopped }
  await rm(temporary, { recursive: true, force: true })
}
