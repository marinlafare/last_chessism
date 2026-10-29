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
    const diagnostic = await evaluate(`JSON.stringify({
      text: document.body.innerText.slice(0, 2000),
      errors: window.ingestionPollingTest?.errors || window.ingestionTest?.errors,
      freshness: window.ingestionPollingTest?.freshness,
      requests: window.ingestionPollingTest?.requests,
      local: window.positionPollingTest && { errors: window.positionPollingTest.errors,
        requests: window.positionPollingTest.requests, jobs: window.positionPollingTest.page?.jobState },
    })`)
    throw new Error('Timed out: ' + expression + '\n' + diagnostic)
  }
  if (!process.argv.includes('--local-only')) {
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
  await evaluate("window.ingestionTest.run.status='failed'; window.ingestionTest.run.finished_at=new Date().toISOString(); window.ingestionTest.render()")
  await waitFor("document.querySelector('.fen-ingestion-dismiss')")
  await pause(11000)
  await waitFor("document.querySelector('.fen-ingestion-result') && !document.querySelector('.fen-pipeline-stage.is-working')")
  await waitFor("window.ingestionTest.errors.length === 0")
  await send('browsingContext.reload', { context, wait: 'complete' })
  await waitFor("document.querySelectorAll('.fen-pipeline-stage').length === 3")
  await waitFor("document.querySelector('.fen-ingestion-dismiss')")
  await evaluate("document.querySelector('.fen-ingestion-dismiss').click()")
  await waitFor("!document.querySelector('.fen-ingestion-result')")
  await send('browsingContext.reload', { context, wait: 'complete' })
  await waitFor("document.querySelector('.fen-ingestion-panel') && !document.querySelector('.fen-ingestion-result')")
  await evaluate("window.ingestionTest.run.status='complete'; window.ingestionTest.run.finished_at=new Date().toISOString(); window.ingestionTest.run.steps.forEach(s=>s.status='complete'); window.ingestionTest.render()")
  await waitFor("document.querySelector('.fen-ingestion-dismiss') && document.querySelectorAll('.fen-pipeline-stage.is-complete').length === 3")
  console.log('PASS: progress, unknown totals, stale heartbeat, failure, retained completion, dismissal/reload, retried run review; no live requests')

  await send('browsingContext.navigate', { context,
    url: 'http://127.0.0.1:6789/tests/fixtures/ingestionPolling.html', wait: 'complete' })
  const state = 'window.ingestionPollingTest'
  const progress = `document.querySelector('.fen-pipeline-stage [role="progressbar"]')?.getAttribute('aria-valuenow')`
  const completed = `Array.from(document.querySelectorAll('.fen-ingestion-result')).find(row => row.querySelector('.fen-ingestion-result-heading strong')?.textContent === 'offline-player')`
  await waitFor(`${progress} === '10' && ${state}.freshness.data === 10`)
  await evaluate(`${state}.before = { calls: ${state}.requests.length, stamp: ${state}.freshness.lastSuccessAt }; ${state}.visibility(true); ${state}.count = 40`)
  await pause(5200)
  await waitFor(`${state}.requests.length === ${state}.before.calls && ${state}.freshness.lastSuccessAt === ${state}.before.stamp && ${progress} === '10'`)
  await evaluate(`${state}.visibility(false)`)
  await waitFor(`${progress} === '40' && ${state}.freshness.data === 40 && ${state}.freshness.lastSuccessAt > ${state}.before.stamp`)

  // A hung status request is a transport problem, never a failed backend job.
  await evaluate(`${state}.visibility(true); ${state}.hold = true; ${state}.visibility(false)`)
  await waitFor(`${state}.pending.length >= 2`)
  await pause(10500)
  await waitFor(`document.querySelector('[role="alert"]')?.textContent.includes('timed out') && document.querySelector('.fen-pipeline-stage.is-working') && !document.querySelector('.fen-pipeline-stage.is-failed')`)
  await evaluate(`${state}.visibility(true); ${state}.hold = false; ${state}.count = 60; ${state}.visibility(false)`)
  await waitFor(`${progress} === '60' && !document.querySelector('[role="alert"]')`)
  await evaluate(`${state}.release()`)
  await pause(200)
  await waitFor(`${progress} === '60'`)

  // Leaving/remounting the page cancels reads only and reconnects to current work.
  await evaluate(`${state}.visibility(true); ${state}.hold = true; ${state}.visibility(false)`)
  await waitFor(`${state}.pending.length >= 2`)
  await evaluate(`${state}.render(false)`)
  await waitFor(`!document.querySelector('.fen-ingestion-panel')`)
  await evaluate(`${state}.before.calls = ${state}.requests.length; ${state}.release(); ${state}.visibility(true); ${state}.visibility(false)`)
  await pause(3200)
  await waitFor(`${state}.requests.length === ${state}.before.calls`)
  await evaluate(`${state}.hold = false; ${state}.count = 100; ${state}.status = 'complete'; ${state}.render()`)
  await waitFor(`${completed}?.querySelector('[role="progressbar"]')?.getAttribute('aria-valuenow') === '100'`)
  await evaluate(`${completed}.querySelector('.fen-ingestion-dismiss').click()`)
  await waitFor(`!${completed} && document.querySelector('.fen-ingestion-result')?.textContent.includes('sample')`)
  await evaluate(`${state}.render(false)`)
  await waitFor(`!document.querySelector('.fen-ingestion-panel')`)
  await evaluate(`${state}.render()`)
  await waitFor(`document.querySelector('.fen-ingestion-panel') && !${completed}`)
  await waitFor(`${state}.errors.length === 0 && ${state}.requests.every(request => request.method === 'GET')`)
  console.log('PASS: real ingestion polling; hidden-tab freshness, immediate return, timeout/recovery, late response suppression, navigation, retained completion/dismissal; mocked GETs only')
  }

  await send('browsingContext.navigate', { context,
    url: 'http://127.0.0.1:6789/tests/fixtures/positionPolling.html', wait: 'complete' })
  const local = 'window.positionPollingTest'
  const localProgress = `document.querySelector('.job-result [role="progressbar"]')?.getAttribute('aria-valuenow')`
  await waitFor(`${localProgress} === '10' && document.querySelector('[data-style-id="local.activity.track"]')`)
  await evaluate(`${local}.visibility(true); ${local}.count = 40; ${local}.before = ${local}.requests.length`)
  await pause(3300)
  await waitFor(`${local}.requests.length === ${local}.before && ${localProgress} === '10'`)
  await evaluate(`${local}.visibility(false)`)
  await waitFor(`${localProgress} === '40'`)
  // Discovery and individual status reads may arrive in either order.
  await evaluate(`${local}.discovery = [{job_id:'local-fixture',status:'in_progress',progress:{kind:'most_repeated',phase:'analyzing',processed:5,total:100,updated_at:5}}]; ${local}.visibility(true); ${local}.visibility(false)`)
  await pause(200)
  await waitFor(`${localProgress} === '40'`)
  // Unknown 404s remain retryable and keep the last known progress visible.
  await evaluate(`${local}.visibility(true); ${local}.failure = true; ${local}.visibility(false)`)
  await waitFor(`document.querySelector('.job-result [role="alert"]')?.textContent.includes('Proxy') && ${localProgress} === '40' && ${local}.page.jobState.global.jobId === 'local-fixture'`)
  await evaluate(`${local}.visibility(true); ${local}.failure = false; ${local}.hold = true; ${local}.visibility(false)`)
  await waitFor(`${local}.pending.length >= 3`)
  await evaluate(`${local}.beforeJobs = ${local}.requests.filter(request => request.path.startsWith('/jobs/')).length; ${local}.before = ${local}.requests.length`)
  await pause(3300)
  await waitFor(`${local}.requests.filter(request => request.path.startsWith('/jobs/')).length === ${local}.beforeJobs && ${local}.requests.length > ${local}.before`)
  await pause(7300)
  await waitFor(`document.querySelector('.job-result [role="alert"]')?.textContent.includes('timed out') && ${localProgress} === '40'`)
  await evaluate(`${local}.visibility(true); ${local}.hold = false; ${local}.count = 70; ${local}.visibility(false)`)
  await waitFor(`${localProgress} === '70' && !document.querySelector('.job-result [role="alert"]')`)
  await evaluate(`${local}.release()`)
  await pause(100)
  await waitFor(`${localProgress} === '70'`)
  await evaluate(`${local}.render(false)`)
  await waitFor(`!document.querySelector('.job-result')`)
  await evaluate(`${local}.before = ${local}.requests.length; ${local}.visibility(true); ${local}.visibility(false)`)
  await pause(3300)
  await waitFor(`${local}.requests.length === ${local}.before`)
  await evaluate(`${local}.render()`)
  await waitFor(`${localProgress} === '70'`)
  // A summary outage cannot turn a completed local analysis into a failed one.
  await evaluate(`${local}.visibility(true); ${local}.count = 100; ${local}.phase = 'complete'; ${local}.failTotals = true; ${local}.visibility(false)`)
  await waitFor(`${localProgress} === '100' && ${local}.page.jobState.global.status.status === 'complete'`)
  await evaluate(`${local}.visibility(true); ${local}.visibility(false)`)
  await pause(100)
  await waitFor(`${local}.page.jobState.global.status.status === 'complete' && !document.querySelector('.job-result [role="alert"]')`)
  await waitFor(`${local}.errors.length === 0 && ${local}.requests.every(request => request.method === 'GET')`)
  console.log('PASS: real local progress hooks; serial reads, hidden return, timeout recovery, unrelated 404, stale discovery, navigation and completion despite unavailable totals; mocked GETs only')
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM'); await stopped
  }
  await rm(temporary, { recursive: true, force: true })
}
