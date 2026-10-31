// Real behavioral panel with mocked rating requests, no production data writes.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-rating-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9268'], { stdio: ['ignore', 'pipe', 'pipe'] })
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
  const pending = new Map(); let nextId = 0
  socket.onmessage = ({ data }) => {
    const response = JSON.parse(data), request = pending.get(response.id)
    if (!request) return
    pending.delete(response.id)
    response.type === 'error' ? request.reject(new Error(JSON.stringify(response))) : request.resolve(response.result)
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId; pending.set(id, { resolve, reject }); socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  const evaluate = async expression => {
    const result = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (result.type === 'exception') throw new Error(JSON.stringify(result))
    return result.result.value
  }
  const waitFor = async expression => {
    for (let i = 0; i < 80; i++) {
      if (await evaluate(expression)) return
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw new Error('Timeout: ' + expression)
  }
  const assert = async expression => {
    if (!await evaluate(expression)) throw new Error('Failed: ' + expression)
  }
  for (const [width, height] of [[1400, 1000], [390, 844]]) {
    await send('browsingContext.setViewport', { context, viewport: { width, height } })
    await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/player-rating-distribution.html', wait: 'complete' })
    await waitFor("document.querySelectorAll('.rating-distribution-bar').length === 4")
    await assert("document.querySelectorAll('.behavior-summary > div').length === 2 && document.querySelectorAll('.behavior-results-column').length === 3")
    await assert("document.querySelector('.rating-distribution-bar title').textContent === '2200–2209: 0.40%'")
    await assert("(() => {const r=document.querySelector('.rating-distribution-chart').getBoundingClientRect(); return r.left >= 0 && r.right <= innerWidth})()")
    await evaluate("document.querySelector('.rating-distribution-chart').focus()")
    await waitFor("document.querySelector('.rating-distribution-note').textContent === '2200–2209: 0.40%'")
    await evaluate("document.querySelector('.rating-distribution-chart').dispatchEvent(new KeyboardEvent('keydown', {key:'ArrowRight',bubbles:true}))")
    await waitFor("document.querySelector('.rating-distribution-note').textContent === '2210–2219: 50.20%'")
    await evaluate("window.changeMode = mode => {const select=document.querySelector('.rating-distribution-heading select'); select.value=mode; select.dispatchEvent(new Event('change', {bubbles:true}))}; window.changeMode('blitz')")
    await waitFor("document.querySelector('.player-rating-distribution [role=alert]') !== null")
    await evaluate("window.ratingTest.failBlitz=false; document.querySelector('.player-rating-distribution button').click()")
    await waitFor("document.querySelectorAll('.rating-distribution-bar').length === 1")
    await assert("document.querySelector('.rating-distribution-bar title').textContent === '2400–2409: 100.00%'")
    await evaluate("window.changeMode('bullet')")
    await waitFor("window.ratingTest.requests.some(r=>r.url.includes('mode=bullet'))")
    await evaluate("window.changeMode('rapid')")
    await waitFor("document.querySelector('.player-rating-distribution').textContent.includes('No rated games')")
    await new Promise(resolve => setTimeout(resolve, 650))
    await assert("document.querySelector('.player-rating-distribution').textContent.includes('No rated games') && !document.querySelector('.rating-distribution-bar')")
    await assert("window.ratingTest.unexpected.length === 0 && window.ratingTest.requests.every(r=>r.method==='GET')")
    console.log(`PASS: ${width}px chart, percentages, keyboard exploration, modes, retry, empty data and late-response protection; no writes.`)
  }
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM'); await stopped
  }
  await rm(temporary, { recursive: true, force: true })
}
