import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'
const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-matrix-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', process.env.MATRIX_BROWSER_PORT || '9241'], { stdio: ['ignore', 'pipe', 'pipe'] })
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
let socket
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Firefox startup timed out')), 15000)
    const output = (chunk) => {
      const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/)
      if (match) { clearTimeout(timeout); resolve(match[1]) }
    }
    browser.stdout.on('data', output); browser.stderr.on('data', output)
    browser.once('error', (error) => { clearTimeout(timeout); reject(error) })
  })
  socket = new WebSocket(endpoint.replace(/\/$/, '') + '/session')
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject })
  const requests = new Map()
  let nextId = 0
  socket.onmessage = ({ data }) => {
    const response = JSON.parse(data)
    if (requests.has(response.id)) {
      const pending = requests.get(response.id); requests.delete(response.id)
      response.type === 'error' ? pending.reject(new Error(JSON.stringify(response))) : pending.resolve(response.result)
    }
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId; requests.set(id, { resolve, reject }); socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  await send('browsingContext.setViewport', { context, viewport: { width: 1280, height: 900 } })
  await send('browsingContext.navigate', { context, url: (process.env.MATRIX_TEST_URL || 'http://localhost:6789') + '/tests/fixtures/matrixConstructor.html', wait: 'complete' })
  const evaluate = async (expression) => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const waitFor = async (expression) => {
    for (let attempt = 0; attempt < 50; attempt++) {
      if (await evaluate(expression)) return
      await pause(100)
    }
    throw new Error('Timeout: ' + expression + '\n' + await evaluate('document.body.innerText'))
  }
  const assert = async (expression) => { if (!await evaluate(expression)) throw new Error('Failed: ' + expression) }
  const click = (name) => evaluate(`Array.from(document.querySelectorAll('button')).find(b => b.textContent === ${JSON.stringify(name)}).click()`)
  await waitFor("Boolean(Array.from(document.querySelectorAll('button')).find(b => b.textContent === 'Save definition' && !b.disabled))")
  await click('Save definition')
  await waitFor("document.querySelectorAll('.matrix-artifact').length === 1")
  await assert("document.body.textContent.includes('No matrix files or background job were created.') && !window.matrixTest.requests.some(({ url }) => url.includes('estimate') || url.includes('job'))")
  await assert("window.matrixTest.requests.filter(({ method, url }) => method === 'GET' && url.includes('/matrices?')).length === 1")
  await click('Live preview')
  await waitFor("document.querySelectorAll('tbody tr').length === 50")
  await assert("document.querySelector('dialog').open && document.querySelector('tbody').textContent.includes('N/A') && document.querySelector('tbody').textContent.includes('bullet')")
  await assert("!document.querySelector('[aria-label=\"Matrix pagination\"]') && document.querySelector('dialog').textContent.includes('NOT A SAVED SNAPSHOT')")
  await evaluate("const control = document.querySelectorAll('dialog select')[1]; control.value = '25'; control.dispatchEvent(new Event('change', { bubbles: true }))")
  await waitFor("document.querySelectorAll('tbody tr').length === 25")
  await click('refresh sample')
  await waitFor("document.querySelectorAll('tbody tr').length === 25")
  await click('full screen')
  await assert("document.querySelector('dialog').classList.contains('is-fullscreen')")
  await evaluate("document.querySelector('dialog').dispatchEvent(new Event('cancel', { cancelable: true }))")
  await waitFor("!document.querySelector('dialog')")
  await click('use as starting point')
  await assert("document.querySelector('.matrix-definition-build-row input').value.endsWith(' copy')")
  await evaluate("document.querySelector('.matrix-artifact-tools button').click()")
  await waitFor("document.querySelectorAll('tbody tr').length === 50")
  await assert("window.matrixTest.requests.some(({ method, url }) => method === 'GET' && url.includes('/definitions/') && url.includes('/preview'))")
  await evaluate("document.querySelector('[aria-label=\"Close matrix viewer\"]').click()")
  await waitFor("!document.querySelector('dialog')")
  await assert("document.body.style.overflow !== 'hidden'")
  await evaluate("window.matrixTest.holdEstimates = true")
  await click('Estimate')
  await waitFor("window.matrixTest.pending.length === 1")
  await evaluate("const input = document.querySelector('.matrix-definition-build-row input'); Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, 'Updated scope'); input.dispatchEvent(new Event('input', { bubbles: true }))")
  await evaluate("window.matrixTest.release()")
  await assert("window.matrixTest.lastAborted && !document.querySelector('.matrix-estimate')")
  await evaluate("window.matrixTest.holdLists = true")
  await click('refresh')
  await waitFor("window.matrixTest.pending.length === 1")
  await click('delete definition')
  await waitFor("document.querySelectorAll('.matrix-artifact').length === 0")
  await evaluate("window.matrixTest.release()")
  await assert("window.matrixTest.lastAborted && document.querySelectorAll('.matrix-artifact').length === 0")
  await evaluate("window.matrixTest.holdLists = false")
  await click('Save definition')
  await waitFor("document.querySelectorAll('.matrix-artifact').length === 1")
  await evaluate("window.matrixTest.definitions.push({ ...window.matrixTest.definitions[0], id: 'second' })")
  await click('refresh')
  await waitFor("document.body.textContent.includes('load more definitions')")
  await evaluate("window.matrixTest.holdLists = true; const button = Array.from(document.querySelectorAll('button')).find(b => b.textContent === 'load more definitions'); button.click(); button.click()")
  await waitFor("window.matrixTest.pending.length === 1")
  await evaluate("window.matrixTest.release()")
  await waitFor("document.querySelectorAll('.matrix-artifact').length === 2")
  await assert("window.matrixTest.requests.filter(({ url }) => url.includes('offset=1')).length === 1")
  console.log('PASS: metadata-only save, live previews, copy, fullscreen, stale estimate/list cancellation and duplicate pagination protection')
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit')
    browser.kill('SIGTERM')
    await stopped
  }
  await rm(temporary, { recursive: true, force: true })
}
