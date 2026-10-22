// Mocked component and appearance API only; never starts/stops a real job.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-step-progress-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9262'], { stdio: ['ignore', 'pipe', 'pipe'] })
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
    if (pending) {
      requests.delete(response.id); clearTimeout(pending.timer)
      response.type === 'error' ? pending.reject(new Error(JSON.stringify(response))) : pending.resolve(response.result)
    }
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId
    const timer = setTimeout(() => { requests.delete(id); reject(new Error('Timeout: ' + method)) }, 20000)
    requests.set(id, { resolve, reject, timer }); socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const load = async name => JSON.parse(await readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url), 'utf8'))
  const [base, scopes, charts] = await Promise.all(['appearance_registry', 'appearance_scopes', 'appearance_charts'].map(load))
  const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: {
    positions: { ...scopes.scopes.positions, targets: { ...scopes.common_targets, ...scopes.scopes.positions.targets } },
  } }
  await send('script.addPreloadScript', { functionDeclaration: `() => { window.stepRegistry = ${JSON.stringify(registry)} }` })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  await send('browsingContext.setViewport', { context, viewport: { width: 1200, height: 1000 } })
  await send('browsingContext.navigate', { context, url: (process.env.CLOUD_UI_TEST_URL || 'http://localhost:6789') + '/tests/fixtures/cloudStepProgress.html', wait: 'complete' })
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const assert = async expression => { if (!await evaluate(expression)) throw new Error('Failed: ' + expression) }
  const waitFor = async expression => {
    for (let attempt = 0; attempt < 100; attempt++) {
      if (await evaluate(`Boolean(${expression})`)) return
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw new Error('Timeout: ' + expression + '\n' + await evaluate('JSON.stringify(window.stepTest)'))
  }
  await waitFor("document.querySelectorAll('[role=progressbar]').length === 10")
  await assert("Number(document.querySelector('[role=progressbar]').getAttribute('aria-valuenow')) === 50000 / 6000000 * 100")
  await assert("document.querySelector('.cloud-step-detail').textContent.includes('FENs reserved')")
  await evaluate("window.stepTest.job.sequence.phase = 'publishing'; window.stepTest.render()")
  await waitFor("document.querySelector('.cloud-step--current .cloud-step-bar').classList.contains('is-indeterminate')")
  await assert("!document.querySelector('.cloud-step--current .cloud-step-bar').hasAttribute('aria-valuenow')")
  await assert("getComputedStyle(document.querySelector('.cloud-step--current .cloud-step-bar-fill')).animationName === (matchMedia('(prefers-reduced-motion: reduce)').matches ? 'none' : 'cloud-step-travel')")
  await evaluate("window.stepTest.online = false; window.stepTest.render()")
  await waitFor("!document.querySelector('.cloud-step-bar.is-active')")
  await assert("document.querySelector('.cloud-step--current .cloud-step-status').textContent.includes('Waiting')")
  await evaluate("window.stepTest.online = true; window.stepTest.job.status = 'paused'; window.stepTest.render()")
  await waitFor("document.querySelector('.cloud-step--attention')")
  await assert("!document.querySelector('.cloud-step-bar.is-active')")
  await evaluate("[...document.querySelectorAll('button')].find(el => el.textContent === 'Edit appearance').click()")
  await evaluate("document.querySelector('[data-style-id=\"cloud.step.progress.publish\"]').dispatchEvent(new MouseEvent('contextmenu', { bubbles: true, cancelable: true, clientX: 500, clientY: 300 }))")
  await waitFor("document.querySelector('.appearance-editor')")
  await assert("['Progress fill', 'Hover progress fill', 'Track background', 'Hover track background', 'Bar height'].every(label => document.querySelector('.appearance-editor').textContent.includes(label))")
  const setField = (property, value) => evaluate(`(() => {
    const input = document.querySelector('#appearance-' + ${JSON.stringify(property)});
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, ${JSON.stringify(String(value))});
    input.dispatchEvent(new Event('input', { bubbles: true })); input.dispatchEvent(new Event('change', { bubbles: true }));
  })()`)
  await setField('chartColor1', '#123456')
  await waitFor("getComputedStyle(document.querySelector('[data-style-id=\"cloud.step.progress.publish\"] .cloud-step-bar-fill')).backgroundColor === 'rgb(18, 52, 86)'")
  await setField('background', '#345678')
  await setField('radius', 12)
  await setField('summaryBarHeight', 16)
  await setField('padding', 3)
  await waitFor("getComputedStyle(document.querySelector('[data-style-id=\"cloud.step.progress.publish\"]')).paddingTop === '3px'")
  await assert("(() => { const bar = document.querySelector('[data-style-id=\"cloud.step.progress.publish\"]'); const css = getComputedStyle(bar); return css.backgroundColor === 'rgb(52, 86, 120)' && css.borderRadius === '12px' && bar.getBoundingClientRect().height >= 22 })()")
  await assert("getComputedStyle(document.querySelector('[data-style-id=\"cloud.step.progress.reserve\"] .cloud-step-bar-fill')).backgroundColor !== 'rgb(18, 52, 86)'")
  await evaluate("document.querySelector('.appearance-editor button[aria-label=Close]')?.click()")
  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 844 } })
  await assert("[...document.querySelectorAll('.cloud-step-bar')].every(el => el.getBoundingClientRect().width > 0 && el.getBoundingClientRect().right <= innerWidth)")
  await evaluate("window.stepTest.job.status = 'running'; window.stepTest.job.stop_cleanup = { phase: 'cleaning' }; window.stepTest.render()")
  await waitFor("document.querySelectorAll('[role=progressbar]').length === 9")
  await assert("document.querySelector('.cloud-step--current .cloud-step-bar').dataset.styleId === 'cloud.step.progress.stop-cleaning'")
  await assert("window.stepTest.errors.length === 0 && window.stepTest.requests.every(request => request.path.endsWith('/appearance') && request.method === 'GET')")
  console.log('PASS: real counters, unknown progress, paused/offline states, independent Edit styles, mobile layout and cancellation; no live API access.')
} finally {
  socket?.close()
  const stopped = once(browser, 'exit')
  browser.kill('SIGTERM')
  await Promise.race([stopped, new Promise(resolve => setTimeout(resolve, 3000))])
  if (browser.exitCode === null) { browser.kill('SIGKILL'); await stopped }
  await rm(temporary, { recursive: true, force: true })
}
