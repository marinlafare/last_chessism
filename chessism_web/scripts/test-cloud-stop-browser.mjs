// All fetches are mocked by the fixture. This never contacts the live cloud API.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm } from 'node:fs/promises'
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
  const { context } = await send('browsingContext.create', { type: 'tab' })
  await send('browsingContext.setViewport', { context, viewport: { width: 1280, height: 1000 } })
  await send('browsingContext.navigate', { context, url: (process.env.CLOUD_UI_TEST_URL || 'http://localhost:6789') + '/tests/fixtures/cloudAnalysis.html', wait: 'complete' })
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
  await assert("!document.querySelector('.cloud-job-dismiss') && document.querySelector('.cloud-job').lastElementChild.classList.contains('cloud-job-stop')")
  await evaluate("window.confirm = () => false; document.querySelector('.cloud-job-stop').click()")
  await assert("!window.cloudTest.requests.some(r => r.path.endsWith('/stop-and-cleanup'))")
  await evaluate("window.confirm = () => true; document.querySelector('.cloud-job-stop').click()")
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
  await assert("!document.querySelector('.cloud-stop-indicator') && document.querySelector('.cloud-job').dataset.styleId === 'card'")
  // The old automatic dismissal happened after ten seconds.
  await new Promise(resolve => setTimeout(resolve, 11000))
  await assert("!!document.querySelector('.cloud-job-dismiss') && !document.querySelector('.cloud-job-fading')")
  await evaluate("document.querySelector('.cloud-job-dismiss').click()")
  await waitFor("!document.querySelector('.cloud-job')")
  await assert("JSON.parse(localStorage.getItem('chessism.cloud-progress.review.v1')).dismissed.includes('cloud-test')")
  await evaluate("Object.assign(window.cloudTest.job, {id:'finished-test', created_at:new Date().toISOString(), status:'complete', imported:2082, stop_cleanup:null, runs:[{id:'done-run',status:'complete',batch_jobs:[]}]})")
  await waitFor("!!document.querySelector('.cloud-job-dismiss')")
  await assert("!document.querySelector('.cloud-job-stop') && document.querySelector('.cloud-job').lastElementChild.classList.contains('cloud-job-dismiss')")
  console.log('PASS: confirmation, stop phases, retained completion, explicit dismissal, saved acknowledgement; no live API writes.')
} finally {
  socket?.close()
  const stopped = once(browser, 'exit')
  browser.kill('SIGTERM')
  await Promise.race([stopped, new Promise(resolve => setTimeout(resolve, 3000))])
  if (browser.exitCode === null) { browser.kill('SIGKILL'); await stopped }
  await rm(temporary, { recursive: true, force: true })
}
