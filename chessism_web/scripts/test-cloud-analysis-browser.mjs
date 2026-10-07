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
  await send('browsingContext.navigate', { context, url: (process.env.CLOUD_UI_TEST_URL || 'http://localhost:6789') + '/tests/fixtures/cloudAnalysis.html', wait: 'complete' })
  const evaluate = async (expression) => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const waitFor = async (expression, timeout = 8000) => {
    for (let attempt=0; attempt<timeout / 100; attempt++) {
      if (await evaluate(expression)) return
      await new Promise((resolve) => setTimeout(resolve, 100))
    }
    throw new Error('Timeout: ' + expression)
  }
  const assert = async (expression) => { if (!await evaluate(expression)) throw new Error('Failed: ' + expression) }
  await waitFor("document.querySelectorAll('fieldset:disabled').length === 2 && document.querySelector('progress')?.value === 500")
  await assert("document.querySelectorAll('.cloud-job').length === 1 && !document.body.textContent.includes('old-complete') && !document.body.textContent.includes('old-cancelled')")
  await assert("document.querySelectorAll('.cloud-job-steps li').length === 10 && document.querySelector('.cloud-job .cloud-step[aria-current=step]').textContent.includes('Analyze FENs')")
  await assert("document.querySelectorAll('.cloud-analysis-grid > .cloud-selection').length === 2")
  await assert("document.querySelectorAll('.cloud-analysis-grid form').length === 2 && !document.querySelector('select[aria-label=\"Cloud backend\"]')")
  await assert("document.querySelector('input[aria-label=\"Spot VMs (n_vms)\"]').matches(':disabled') && !document.body.textContent.includes('Pending multi-VM support')")
  await assert("document.querySelector('select[aria-label=\"FEN selection\"]').matches(':disabled') && document.querySelector('input[max=\"64\"]').matches(':disabled')")
  await assert("document.querySelector('.cloud-selection progress').max === 2082 && document.querySelector('.cloud-fen-count strong').textContent === '24%'")
  await assert("document.querySelector('progress').getBoundingClientRect().height >= 16 && document.querySelector('progress').getBoundingClientRect().width > 200")
  await assert("document.querySelectorAll('.cloud-job-indicator--running').length === 2 && !document.querySelector('.cloud-job-indicator--success')")
  await assert("document.querySelector('.cloud-selection .cloud-job-indicator').getAttribute('aria-label').startsWith('In progress')")
  await assert("getComputedStyle(document.querySelector('.cloud-job-indicator')).backgroundColor === 'rgb(255, 146, 43)' && getComputedStyle(document.querySelector('.cloud-job-indicator')).borderRadius === '50%'")
  await assert("getComputedStyle(document.querySelector('.cloud-job-indicator')).animationName === (matchMedia('(prefers-reduced-motion: reduce)').matches ? 'none' : 'cloud-job-glow')")
  await evaluate("window.cloudTest.job.imported = 2082; window.cloudTest.job.runs[0].status = 'cleaning'")
  await waitFor("document.querySelector('.cloud-fen-count strong').textContent === '100%'")
  await assert("document.querySelectorAll('fieldset:disabled').length === 2 && document.body.textContent.includes('Job is not done:')")
  await assert("document.querySelector('.cloud-job .cloud-step[aria-current=step]').textContent.includes('Clean up') && document.querySelector('.cloud-job-steps li:last-child small').textContent === 'Pending'")
  await assert("document.querySelectorAll('.cloud-job-indicator--running').length === 2 && !document.querySelector('.cloud-job-indicator--success')")
  await evaluate(`window.cloudTest.job.runs[0].status = 'log_cleaning'; window.cloudTest.job.runs[0].performance = {
    analyzed_this_attempt:2082,resumed:0,metrics:{worker_seconds:100,fen_per_second:20.82,vm_cpu_busy_percent:94},
    workers:[0,1,2,3].map(worker=>({worker,positions:worker===3?522:520,analysis_seconds:90,upload_wait_seconds:0.02}))
  }`)
  await waitFor("document.querySelector('.cloud-job details summary')?.textContent.includes('saved locally')")
  await evaluate("document.querySelector('.cloud-job details').open = true")
  await assert("document.querySelectorAll('.cloud-job details tbody tr').length === 4 && document.querySelectorAll('fieldset:disabled').length === 2")
  await evaluate("window.cloudTest.job.runs[0].status = 'complete'")
  await waitFor("document.querySelector('.cloud-job .cloud-step[aria-current=step]')?.textContent.includes('Refresh database totals')")
  await assert("document.querySelectorAll('fieldset:disabled').length === 2 && document.querySelector('.cloud-job-steps li:last-child small').textContent === 'Pending'")
  await assert("document.querySelectorAll('.cloud-job-indicator--running').length === 2")
  await evaluate("window.cloudTest.job.status = 'complete'")
  await waitFor("document.querySelectorAll('fieldset:disabled').length === 0")
  await assert("document.querySelector('.cloud-job > strong').textContent.endsWith('— Done') && document.querySelectorAll('.cloud-step--complete').length === 10")
  await assert("document.querySelectorAll('.cloud-job-indicator--success').length === 2 && !document.querySelector('.cloud-job-indicator--running')")
  await assert("getComputedStyle(document.querySelector('.cloud-job-indicator')).backgroundColor === 'rgb(74, 222, 128)' && document.querySelector('.cloud-job-indicator').getAttribute('aria-label').startsWith('Succeeded')")
  await evaluate("window.cloudTest.doneSeenAt = Date.now()")
  await new Promise((resolve) => setTimeout(resolve, 8500))
  await assert("document.querySelector('.cloud-job') !== null && !document.querySelector('.cloud-job-fading')")
  await waitFor("document.querySelector('.cloud-job-fading') !== null", 3000)
  await waitFor("!document.querySelector('.cloud-job') && !document.querySelector('.cloud-selection progress')")
  await assert("Date.now() - window.cloudTest.doneSeenAt >= 9900 && document.body.textContent.includes('No active cloud jobs.')")
  await assert("!document.querySelector('.cloud-job-indicator')")
  await assert("document.querySelector('input[max=\"64\"]').value === '4' && document.querySelector('input[aria-label=\"Spot VMs (n_vms)\"]').value === '1'")
  await evaluate(`window.cloudUiSetValue = (selector, value) => {
    const element = document.querySelector(selector)
    const prototype = element.tagName === 'SELECT' ? HTMLSelectElement.prototype : HTMLInputElement.prototype
    Object.getOwnPropertyDescriptor(prototype, 'value').set.call(element, value)
    element.dispatchEvent(new Event(element.tagName === 'SELECT' ? 'change' : 'input', { bubbles: true }))
  }; window.cloudCreates = () => window.cloudTest.requests.filter(r => r.path.endsWith('/analysis/cloud') && r.method === 'POST')`)
  const chooseMode = async (mode) => {
    await evaluate(`window.cloudUiSetValue('select[aria-label="FEN selection"]', '${mode}')`)
    await waitFor(`document.querySelector('select[aria-label="FEN selection"]').value === '${mode}'`)
  }
  await assert("Array.from(document.querySelector('select[aria-label=\"FEN selection\"]').options, o => o.value).sort().join(',') === 'all,games,loop,player'")
  await chooseMode('player')
  await waitFor("document.querySelector('input[placeholder=\"chess.com nickname\"]') !== null")
  await evaluate("window.cloudUiSetValue('input[placeholder=\"chess.com nickname\"]', 'magnuscarlsen')")
  await evaluate("Array.from(document.querySelectorAll('button')).find(b => b.textContent === 'Inspect player').click()")
  await waitFor("document.querySelector('form.cloud-selection').textContent.includes('3000 positions; 1000 analyzed.')")
  await chooseMode('loop')
  await assert("document.querySelector('form.cloud-selection').textContent.includes('Positions per group') && !document.querySelector('input[placeholder=\"chess.com nickname\"]')")
  await chooseMode('all')
  await assert("!document.querySelector('input[placeholder=\"chess.com nickname\"]') && !document.querySelector('form.cloud-selection').textContent.includes('3000 positions')")
  await chooseMode('games')
  await assert("document.querySelector('form.cloud-selection button[type=submit]').disabled")
  await evaluate("window.cloudTest.previewCount = 0; Array.from(document.querySelectorAll('button')).find(b => b.textContent === 'Preview games').click()")
  await waitFor("document.querySelector('form.cloud-selection').textContent.includes('No missing FENs')")
  await assert("document.querySelector('form.cloud-selection button[type=submit]').disabled")
  await evaluate("window.cloudTest.previewCount = 2082; Array.from(document.querySelectorAll('button')).find(b => b.textContent === 'Preview games').click()")
  await waitFor("!document.querySelector('form.cloud-selection button[type=submit]').disabled")
  // Changing selection invalidates the preview, even when switching back to the same games.
  await chooseMode('all')
  await chooseMode('games')
  await assert("document.querySelector('form.cloud-selection button[type=submit]').disabled && !document.querySelector('form.cloud-selection').textContent.includes('Frozen preview:')")
  await evaluate("Array.from(document.querySelectorAll('button')).find(b => b.textContent === 'Preview games').click()")
  await waitFor("!document.querySelector('form.cloud-selection button[type=submit]').disabled")
  await evaluate("window.cloudUiSetValue('input[max=\"64\"]', '65')")
  await assert("!document.querySelector('form.cloud-selection').checkValidity()")
  await evaluate("document.querySelector('form.cloud-selection button[type=submit]').click()")
  await assert("window.cloudCreates().length === 0")
  await evaluate("window.cloudUiSetValue('input[max=\"64\"]', '8')")
  await assert("document.querySelector('form.cloud-selection').checkValidity() && !document.querySelector('form.cloud-selection button[type=submit]').disabled && document.querySelector('form.cloud-selection').textContent.includes('Frozen preview:')")
  await evaluate("document.querySelector('form.cloud-selection button[type=submit]').click()")
  await waitFor("window.cloudCreates().length === 1 && document.querySelectorAll('fieldset:disabled').length === 2")
  await assert("window.cloudCreates()[0].body.backend === 'cloud_run' && window.cloudCreates()[0].body.n_cpus === 8 && window.cloudCreates()[0].body.mode === 'games'")
  await assert("window.cloudCreates()[0].body.plan_id === 'a'.repeat(32) && !Object.hasOwn(window.cloudCreates()[0].body, 'total_fens')")
  await assert("document.querySelector('input[max=\"64\"]').matches(':disabled') && document.querySelector('select[aria-label=\"FEN selection\"]').matches(':disabled')")
  await evaluate("document.querySelector('form.cloud-selection').dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }))")
  await assert("window.cloudCreates().length === 1")
  await evaluate("window.cloudTest.job.status = 'paused'")
  await waitFor("Array.from(document.querySelectorAll('button')).some(b => b.textContent === 'Resume saved job' && !b.disabled)")
  await assert("document.querySelectorAll('fieldset:disabled').length === 2")
  await assert("document.querySelectorAll('.cloud-job-indicator--attention').length === 2 && !document.querySelector('.cloud-job-indicator--running')")
  await evaluate("window.cloudTest.online = false")
  await waitFor("document.body.textContent.includes('offline — start cloud-controller')")
  await assert("Array.from(document.querySelectorAll('button')).find(b => b.textContent === 'Resume saved job').disabled")
  // Finish the mocked Run job, then exercise the independent Batch controls.
  await evaluate("window.cloudTest.online = true; window.cloudTest.job.status = 'complete'")
  await waitFor("document.querySelectorAll('fieldset:disabled').length === 0")
  await evaluate(`window.cloudUiSetValue('form[aria-labelledby="cloud-batch-title"] select[aria-label="FEN selection"]', 'games')`)
  await evaluate(`window.cloudUiSetValue('form[aria-labelledby="cloud-batch-title"] input[placeholder="chess.com nickname"]', 'hikaru')`)
  await evaluate(`Array.from(document.querySelectorAll('form[aria-labelledby="cloud-batch-title"] button')).find(b => b.textContent === 'Preview games').click()`)
  await waitFor(`document.querySelector('form[aria-labelledby="cloud-batch-title"]').textContent.includes('Frozen preview:')`)
  await evaluate(`window.cloudUiSetValue('input[aria-label="Spot VMs (n_vms)"]', '11')`)
  await assert(`!document.querySelector('form[aria-labelledby="cloud-batch-title"]').checkValidity()`)
  await evaluate(`window.cloudUiSetValue('input[aria-label="Spot VMs (n_vms)"]', '2')`)
  await assert(`document.querySelector('form[aria-labelledby="cloud-batch-title"]').textContent.includes('Frozen preview:')`)
  await evaluate(`document.querySelector('form[aria-labelledby="cloud-batch-title"] button[type=submit]').click()`)
  await waitFor("window.cloudCreates().length === 2 && document.querySelectorAll('fieldset:disabled').length === 2")
  await assert("window.cloudCreates()[1].body.backend === 'batch_spot' && window.cloudCreates()[1].body.n_vms === 2 && !Object.hasOwn(window.cloudCreates()[1].body, 'n_cpus')")
  await assert("window.cloudCreates()[1].body.mode === 'games' && window.cloudCreates()[1].body.player_name === 'hikaru' && !Object.hasOwn(window.cloudCreates()[1].body, 'total_fens')")
  await evaluate(`window.cloudTest.job.status = 'running'; window.cloudTest.job.runs = [{id:'fleet',status:'running',batch_jobs:['vm0','vm1'],vm_statuses:[
    {index:0,positions:1041,state:'RECOVERING',preemptions:2,application_failures:0},
    {index:1,positions:1041,state:'RUNNING',preemptions:0,application_failures:0}]}]`)
  await waitFor(`document.querySelector('ul[aria-label="Batch VM progress"]')?.textContent.includes('VM 1: RECOVERING')`)
  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 844 } })
  await assert("getComputedStyle(document.querySelector('.cloud-analysis-grid')).gridTemplateColumns.split(' ').length === 1")
  console.log('PASS: workflow stages/glow/fade, both backend FEN selectors, CPU/VM bounds, frozen previews, Batch payloads/per-VM recovery, cross-backend locks and mobile layout.')
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM'); await stopped
  }
  await rm(temporary, { recursive: true, force: true })
}
