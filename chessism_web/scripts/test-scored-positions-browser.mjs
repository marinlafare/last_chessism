// Real page, synthetic large responses; never reaches the API or Google Cloud.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-scored-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile,
  '--remote-debugging-port', '9255'], { stdio: ['ignore', 'pipe', 'pipe'] })
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
    const timer = setTimeout(() => { requests.delete(id); reject(new Error(`Timeout: ${method}`)) }, 45000)
    requests.set(id, { resolve, reject, timer }); socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const assert = async expression => {
    if (!await evaluate(expression)) throw new Error('Failed: ' + expression)
  }
  for (const count of [0, 1, 200000]) {
    await send('browsingContext.navigate', { context, wait: 'complete',
      url: (process.env.SCORED_UI_TEST_URL || 'http://localhost:6789') + `/tests/fixtures/scoredPositions.html?count=${count}` })
    let ready = false
    for (let i = 0; i < 100; i++) {
      ready = await evaluate(`window.scoredTest?.requests.length === 5 && document.querySelectorAll('.rating-scatter-chart circle').length === ${count} && document.querySelector('.scored-coverage-grid strong')?.textContent === '4,000,000'`)
      if (ready) break
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    if (!ready) throw new Error(`Page did not render ${count} ratings`)
    await assert('window.scoredTest.errors.length === 0 && window.scoredTest.requests.every(r => r.method === "GET")')
    if (count) {
      await assert(`Array.from(document.querySelectorAll('.rating-scatter-chart circle')).every(p => Number.isFinite(p.cx.baseVal.value) && Number.isFinite(p.cy.baseVal.value))`)
      await assert(`document.querySelector('.rating-scatter-chart').textContent.includes(${JSON.stringify(count.toLocaleString('en-US'))})`)
      await evaluate(`Array.from(document.querySelectorAll('.rating-group-button')).find(b => b.textContent === 'great').click()`)
      await new Promise(resolve => setTimeout(resolve, 300))
      await assert(`document.querySelectorAll('.rating-scatter-chart circle').length === ${count} && document.querySelector('.rating-group-button.active').textContent === 'great' && window.scoredTest.errors.length === 0`)
    }
    console.log(`PASS: scored positions remains mounted with ${count.toLocaleString('en-US')} ratings, correct axes and no runtime errors.`)
  }
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM'); await stopped
  }
  await rm(temporary, { recursive: true, force: true })
}
