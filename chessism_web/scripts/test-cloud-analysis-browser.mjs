// Isolated mocked page: never submits jobs or contacts the production API.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-cloud-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9252'], { stdio: ['ignore', 'pipe', 'pipe'] })
let socket
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Browser startup timeout')), 15000)
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
  await send('browsingContext.setViewport', { context, viewport: { width: 1280, height: 1000 } })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/cloudAnalysis.html', wait: 'complete' })
  const evaluate = async (expression) => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const waitFor = async (expression) => {
    for (let attempt=0; attempt<80; attempt++) {
      if (await evaluate(expression)) return
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    throw new Error('Timeout: ' + expression)
  }
  const assert = async (expression) => { if (!await evaluate(expression)) throw new Error('Failed: ' + expression) }
  await waitFor("document.querySelectorAll('fieldset:disabled').length === 4 && document.querySelector('progress')?.value === 500")
  await assert("document.querySelector('.cloud-selection progress').max === 2082 && document.querySelector('.cloud-fen-count strong').textContent === '24%'")
  await assert("document.querySelector('progress').getBoundingClientRect().height >= 16 && document.querySelector('progress').getBoundingClientRect().width > 200")
  await evaluate("window.cloudTest.job.imported = 2082; window.cloudTest.job.runs[0].status = 'cleaning'")
  await waitFor("document.querySelector('.cloud-fen-count strong').textContent === '100%'")
  await assert("document.querySelectorAll('fieldset:disabled').length === 4 && document.body.textContent.includes('cleanup may still be running')")
  await evaluate(`window.cloudTest.job.runs[0].status = 'log_cleaning'; window.cloudTest.job.runs[0].performance = {
    analyzed_this_attempt:2082,resumed:0,metrics:{worker_seconds:100,fen_per_second:20.82,vm_cpu_busy_percent:94},
    workers:[0,1,2,3].map(worker=>({worker,positions:worker===3?522:520,analysis_seconds:90,upload_wait_seconds:0.02}))
  }`)
  await waitFor("document.querySelector('.cloud-job details summary')?.textContent.includes('saved locally')")
  await evaluate("document.querySelector('.cloud-job details').open = true")
  await assert("document.querySelectorAll('.cloud-job details tbody tr').length === 4 && document.querySelectorAll('fieldset:disabled').length === 4")
  await evaluate("window.cloudTest.job.status = 'complete'; window.cloudTest.job.runs[0].status = 'complete'")
  await waitFor("document.querySelectorAll('fieldset:disabled').length === 0")
  await evaluate("document.querySelector('.cloud-selection button[type]')?.focus(); document.querySelector('.cloud-selection .btn').click()")
  await waitFor("window.cloudTest.requests.some(r => r.method === 'POST') && document.querySelectorAll('fieldset:disabled').length === 4")
  await evaluate("document.querySelectorAll('.cloud-selection')[1].dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }))")
  await assert("window.cloudTest.requests.filter(r => r.method === 'POST').length === 1")
  await evaluate("window.cloudTest.job.status = 'paused'")
  await waitFor("Array.from(document.querySelectorAll('button')).some(b => b.textContent === 'Resume saved job' && !b.disabled)")
  await assert("document.querySelectorAll('fieldset:disabled').length === 4")
  await evaluate("window.cloudTest.online = false")
  await waitFor("document.body.textContent.includes('offline — start cloud-controller')")
  await assert("Array.from(document.querySelectorAll('button')).find(b => b.textContent === 'Resume saved job').disabled")
  console.log('PASS: visible FEN progress, batch updates, all cloud controls locked through cleanup, recovery and offline states, duplicate-submit guard.')
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM'); await stopped
  }
  await rm(temporary, { recursive: true, force: true })
}
