// Real System Status and polling; every data request and recovery POST is mocked.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const read = name => readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url), 'utf8').then(JSON.parse)
const [base, scopes, charts] = await Promise.all(['appearance_registry', 'appearance_scopes', 'appearance_charts'].map(read))
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: Object.fromEntries(
  Object.entries(scopes.scopes).map(([scope, spec]) => [scope, { ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets } }])) }
const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-cloud-recovery-')), profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9291'], { stdio: ['ignore', 'pipe', 'pipe'] })
let socket
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('Browser startup timeout')), 15000)
    const output = chunk => { const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/); if (match) { clearTimeout(timer); resolve(match[1]) } }
    browser.stdout.on('data', output); browser.stderr.on('data', output)
    browser.once('error', error => { clearTimeout(timer); reject(error) })
  })
  socket = new WebSocket(endpoint.replace(/\/$/, '') + '/session')
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject })
  const pending = new Map(); let id = 0
  socket.onmessage = ({ data }) => {
    const response = JSON.parse(data), call = pending.get(response.id)
    if (!call) return
    pending.delete(response.id); clearTimeout(call.timer)
    response.type === 'error' ? call.reject(Error(JSON.stringify(response))) : call.resolve(response.result)
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const key = ++id, timer = setTimeout(() => { pending.delete(key); reject(Error('Timeout: ' + method)) }, 20000)
    pending.set(key, { resolve, reject, timer }); socket.send(JSON.stringify({ id: key, method, params }))
  })
  await send('session.new', { capabilities: {} })
  await send('script.addPreloadScript', { functionDeclaration: `() => { window.cloudRecoveryRegistry=${JSON.stringify(registry)} }` })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  const evaluate = async expression => {
    const result = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (result.type === 'exception') throw Error(JSON.stringify(result))
    return result.result.value
  }
  const assert = async expression => { if (!await evaluate(expression)) throw Error('Failed: ' + expression) }
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) { if (await evaluate(`Boolean(${expression})`)) return; await new Promise(resolve => setTimeout(resolve, 100)) }
    throw Error('Timeout: ' + expression + '; ' + await evaluate('JSON.stringify(cloudRecoveryTest.errors)'))
  }
  const button = `document.querySelector('.cloud-job-recovery-button')`
  const posts = `cloudRecoveryTest.requests.filter(r=>r.method==='POST')`
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1000 } })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/cloudRecovery.html', wait: 'complete' })
  await waitFor(`${button} && !${button}.disabled`)
  await assert(`${posts}.length===0 && document.querySelector('.cloud-job-recovery-message').textContent.includes('Synthetic Google Storage timeout')`)
  await evaluate(`document.querySelector('.cloud-recovery-history summary').click()`)
  await assert(`document.querySelector('.cloud-recovery-history').open && document.querySelectorAll('.cloud-recovery-history tbody tr').length===3`)
  await assert(`document.querySelector('.cloud-recovery-history').textContent.includes('2m 5s') && document.querySelector('.cloud-recovery-history').textContent.includes('Synthetic failure retained')`)
  await evaluate(`cloudRecoveryTest.holdResume=true;cloudRecoveryTest.holdRefresh=true;${button}.click();${button}.click()`)
  await waitFor(`${button}.disabled && ${button}.getAttribute('aria-busy')==='true' && cloudRecoveryTest.finishResume`)
  await assert(`${posts}.length===1 && ${posts}[0].path==='/analysis/cloud/synthetic-job/resume'`)
  await evaluate('cloudRecoveryTest.finishResume()')
  await waitFor('cloudRecoveryTest.finishRefresh')
  await assert(`${button}.disabled && document.querySelector('.cloud-job-recovery-message').textContent.includes('checking status')`)
  await evaluate(`cloudRecoveryTest.job.status='running';cloudRecoveryTest.job.imported+=5000;cloudRecoveryTest.holdRefresh=false;cloudRecoveryTest.finishRefresh()`)
  await waitFor(`!${button} && document.querySelector('[data-cloud-activity]').textContent==='Active'`)
  await assert(`document.querySelector('.cloud-status-fen-count').textContent.includes('3,732,000') && ${posts}.length===1`)
  for (const status of ['queued', 'failed', 'complete', 'cancelled']) {
    await evaluate(`cloudRecoveryTest.job.status=${JSON.stringify(status)};cloudRecoveryTest.renderPage()`)
    await waitFor(`document.querySelector('[data-cloud-activity]')?.textContent!=='Checking…'`)
    await assert(`!${button} && ${posts}.length===1`)
  }
  // Explicit recovery is also available for a waiting Cloud Run job.
  await evaluate(`cloudRecoveryTest.holdResume=false;cloudRecoveryTest.job.status='waiting';cloudRecoveryTest.job.selection={backend:'cloud_run',n_cpus:100};cloudRecoveryTest.renderPage()`)
  await waitFor(`${button} && !${button}.disabled && document.querySelector('[data-cloud-machines]').textContent.includes('Serverless')`)
  await evaluate(`cloudRecoveryTest.online=false;cloudRecoveryTest.renderPage()`)
  await waitFor(`${button}?.disabled && document.querySelector('[data-cloud-controller]')?.textContent==='Offline'`)
  await evaluate(`${button}.click()`)
  await assert(`${posts}.length===1`)
  await evaluate(`cloudRecoveryTest.online=true;cloudRecoveryTest.resumeStatus=503;cloudRecoveryTest.job.status='paused';cloudRecoveryTest.renderPage()`)
  await waitFor(`${button} && !${button}.disabled`)
  await evaluate(`${button}.click()`)
  await waitFor(`document.querySelector('.cloud-job-recovery [role="alert"]')?.textContent==='Synthetic recovery rejected' && !${button}.disabled`)
  await assert(`${posts}.length===2`)
  // A failed status refresh must disable recovery against a stale snapshot.
  await evaluate(`cloudRecoveryTest.failStatus=true;${button}.click()`)
  await waitFor(`document.querySelector('[data-cloud-activity]')?.textContent==='Stale' && ${button}.disabled`)
  await assert(`${posts}.length===3`)
  await evaluate(`cloudRecoveryTest.failStatus=false;cloudRecoveryTest.renderPage()`)
  await waitFor(`${button} && !${button}.disabled`)
  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 844 } })
  await assert(`document.documentElement.scrollWidth<=innerWidth+1`)
  await evaluate(`[...document.querySelectorAll('button')].find(n=>n.textContent==='Edit appearance').click()`)
  for (const [target, spec] of Object.entries(scopes.scopes.system_status.targets).filter(([id])=>id.startsWith('cloud.recovery.'))) {
    await evaluate(`document.querySelector('[data-style-id="${target}"]').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:100,clientY:300}))`)
    await waitFor(`document.querySelector('.appearance-editor header strong')?.textContent===${JSON.stringify(spec.label)}`)
    for (const property of spec.properties) await assert(`!!document.querySelector('#appearance-${property}')`)
  }
  await assert(`cloudRecoveryTest.errors.length===0 && ${posts}.every(r=>r.path==='/analysis/cloud/synthetic-job/resume')`)
  console.log('PASS: recovery history and pause durations, paused/waiting recovery, one POST per click, locked pending/refresh states, fresh progress, offline/stale/error handling, terminal states, mobile and Edit styles. All cloud actions mocked.')
} finally {
  socket?.close()
  if (browser.exitCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM')
    await Promise.race([stopped, new Promise(resolve => setTimeout(resolve, 3000))])
    if (browser.exitCode === null) { browser.kill('SIGKILL'); await stopped }
  }
  await rm(temporary, { recursive: true, force: true })
}
