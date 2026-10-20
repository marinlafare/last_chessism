// Real React progress panel, synthetic data, no live API requests or credentials.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-ingestion-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile,
  '--remote-debugging-port', '9274'], { stdio: ['ignore', 'pipe', 'pipe'] })
let socket
const pause = ms => new Promise(resolve => setTimeout(resolve, ms))
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Browser startup timeout')), 15000)
    const output = chunk => {
      const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/)
      if (match) { clearTimeout(timer); resolve(match[1]) }
    }
    browser.stdout.on('data', output); browser.stderr.on('data', output)
    browser.once('error', reject)
  })
  socket = new WebSocket(endpoint.replace(/\/$/, '') + '/session')
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject })
  const pending = new Map(); let nextId = 0
  socket.onmessage = ({ data }) => {
    const message = JSON.parse(data), entry = pending.get(message.id)
    if (!entry) return
    pending.delete(message.id)
    message.type === 'error' ? entry.reject(new Error(JSON.stringify(message))) : entry.resolve(message.result)
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId; pending.set(id, { resolve, reject })
    socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  const evaluate = async expression => {
    const result = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (result.type === 'exception') throw new Error(JSON.stringify(result))
    return result.result.value
  }
  const waitFor = async expression => {
    for (let n = 0; n < 60; n++) {
      if (await evaluate(expression)) return
      await pause(100)
    }
    throw new Error('Timed out: ' + expression)
  }
  await send('browsingContext.navigate', { context,
    url: 'http://127.0.0.1:6789/tests/fixtures/ingestion.html', wait: 'complete' })
  await waitFor("document.querySelectorAll('.fen-pipeline-stage').length === 3")
  await waitFor("document.querySelectorAll('.fen-pipeline-stage.is-complete').length === 1")
  await waitFor("document.querySelector('.fen-pipeline-stage.is-working').textContent.includes('1,000')")
  await waitFor("!document.querySelector('.fen-pipeline-stage.is-working [role=progressbar]')")
  await evaluate("window.ingestionTest.run.steps[1].heartbeat_at -= 60; window.ingestionTest.render()")
  await waitFor("document.body.textContent.includes('heartbeat delayed')")
  await evaluate("window.ingestionTest.run.steps[1].status='complete'; window.ingestionTest.run.steps[1].total=1000; window.ingestionTest.render()")
  await waitFor("document.querySelectorAll('.fen-pipeline-stage.is-complete').length === 2")
  await evaluate("window.ingestionTest.run.steps[2].status='failed'; window.ingestionTest.render()")
  await waitFor("document.querySelector('.fen-pipeline-stage.is-failed')")
  await waitFor("window.ingestionTest.errors.length === 0")
  await send('browsingContext.reload', { context, wait: 'complete' })
  await waitFor("document.querySelectorAll('.fen-pipeline-stage').length === 3")
  console.log('PASS: real React panel, unknown totals, completed steps, stale heartbeat, failure and reload')
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM'); await stopped
  }
  await rm(temporary, { recursive: true, force: true })
}
