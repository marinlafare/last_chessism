import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-algorithm-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9242'], { stdio: ['ignore', 'pipe', 'pipe'] })
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
let socket
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Firefox startup timed out')), 15000)
    const output = (chunk) => {
      const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/)
      if (match) { clearTimeout(timer); resolve(match[1]) }
    }
    browser.stdout.on('data', output); browser.stderr.on('data', output)
    browser.once('error', (error) => { clearTimeout(timer); reject(error) })
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
  const { context } = await send('browsingContext.create', { type: 'tab' })
  await send('browsingContext.setViewport', { context, viewport: { width: 1280, height: 900 } })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/algorithms.html', wait: 'complete' })
  const evaluate = async (expression) => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const waitFor = async (expression) => {
    for (let attempt = 0; attempt < 50; attempt++) { if (await evaluate(expression)) return; await pause(100) }
    throw new Error('Timeout: ' + expression + '\n' + await evaluate('document.body.innerText'))
  }
  const assert = async (expression) => { if (!await evaluate(expression)) throw new Error('Failed: ' + expression) }
  const click = (name) => evaluate(`Array.from(document.querySelectorAll('button')).find(b => b.textContent === ${JSON.stringify(name)}).click()`)
  await waitFor("Boolean(Array.from(document.querySelectorAll('button')).find(b => b.textContent === 'Save algorithm' && !b.disabled))")
  await assert("document.querySelectorAll('.algorithm-checks input').length === 2")
  await click('Preview live rows')
  await waitFor("document.querySelector('dialog')?.open")
  await assert("document.querySelector('dialog').textContent.includes('1500')")
  await evaluate("document.querySelector('[aria-label=\"Close matrix viewer\"]').click()")
  await click('Check scope')
  await waitFor("document.querySelector('.algorithm-estimate')?.textContent.includes('100 selected rows')")
  await click('Save algorithm')
  await waitFor("document.body.textContent.includes('Instructions saved.')")
  await assert("!window.algorithmTest.requests.some(r => r.method === 'POST' && r.url.endsWith('/runs'))")
  await click('Run')
  await waitFor("document.querySelector('.algorithm-status')?.textContent === 'queued'")
  await evaluate('window.algorithmTest.finish()')
  await click('Refresh')
  await waitFor("document.querySelector('.algorithm-status')?.textContent === 'complete'")
  await click('Show results')
  await waitFor("document.querySelectorAll('.algorithm-scatter circle').length === 2")
  await assert("document.querySelector('.algorithm-heatmap').textContent.includes('0.900') && document.querySelector('.algorithm-metrics').textContent.includes('90')")
  await click('Run')
  await waitFor("document.querySelector('.algorithm-status')?.textContent === 'queued'")
  await click('Cancel run')
  await waitFor("document.querySelector('.algorithm-status')?.textContent === 'cancelled'")
  await click('Delete instructions')
  await waitFor("document.body.textContent.includes('No saved algorithms yet.')")
  await assert("document.querySelectorAll('.algorithm-status').length === 2")
  console.log('PASS: numeric inputs, live preview, preflight, instructions-only save, explicit run, results, cancellation and preserved history')
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM'); await stopped
  }
  await rm(temporary, { recursive: true, force: true })
}
