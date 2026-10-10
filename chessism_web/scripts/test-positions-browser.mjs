// Real page and hooks, isolated browser profile, mocked requests only.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-positions-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile,
  '--remote-debugging-port', '9257'], { stdio: ['ignore', 'pipe', 'pipe'] })
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
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) {
      if (await evaluate(expression)) return
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw new Error('Timeout: ' + expression + '\n' + await evaluate('JSON.stringify(window.positionsTest)'))
  }
  const assert = async expression => {
    if (!await evaluate(expression)) throw new Error('Failed: ' + expression)
  }
  const click = selector => evaluate(`document.querySelector(${JSON.stringify(selector)}).click()`)
  const edit = (selector, value) => evaluate(`(() => {
    const el=document.querySelector(${JSON.stringify(selector)});
    Object.getOwnPropertyDescriptor(el.tagName==='SELECT'?HTMLSelectElement.prototype:HTMLInputElement.prototype,'value').set.call(el,${JSON.stringify(value)});
    el.dispatchEvent(new Event(el.tagName==='SELECT'?'change':'input',{bubbles:true}));
  })()`)
  const mode = value => edit('select[aria-label="Local FEN selection"]', value)
  for (const [width, height] of [[1400,1000],[390,844]]) {
    await send('browsingContext.setViewport', { context, viewport: { width, height } })
    await send('browsingContext.navigate', { context,
      url: (process.env.POSITIONS_UI_TEST_URL || 'http://localhost:6789') + '/tests/fixtures/positions.html', wait: 'complete' })
    await waitFor("document.querySelector('#analysis-gcloud-toggle')?.textContent.includes('500 / 2,082')")
    await assert("[...document.querySelectorAll('.analysis-disclosure-toggle')].every(b=>b.getAttribute('aria-expanded')==='false')")
    await assert("[...document.querySelectorAll('.analysis-disclosure-panel')].every(p=>p.hidden && p.getBoundingClientRect().height===0)")
    await assert("[...document.querySelectorAll('.analysis-disclosure-toggle')].every(b=>Math.abs(b.getBoundingClientRect().width-document.querySelector('.coverage-bar-chart').getBoundingClientRect().width)<4)")
    await assert("document.documentElement.scrollWidth<=innerWidth")
    await click('#analysis-local-toggle')
    await edit('.local-position-fields input', '321')
    await mode('player')
    await edit('.local-selection-form input[type=text]', 'hikaru')
    await click('.local-selection-form .input-action-row button')
    await waitFor("document.querySelector('.local-selection-counts')?.textContent.includes('3,000')")
    await mode('games')
    await edit('.player-game-scope-form input[type=text]', 'hikaru')
    await click('.player-game-scope-form button[type=submit]')
    await waitFor("!!document.querySelector('.player-game-selection-controls')")
    await click('.player-game-selection-controls button')
    await waitFor("!!document.querySelector('.player-game-confirmation')")
    await click('#analysis-local-toggle')
    await click('#analysis-local-toggle')
    await assert("document.querySelector('.player-game-confirmation').textContent.includes('hikaru')")
    await assert("document.documentElement.scrollWidth<=innerWidth")
    await mode('loop')
    await edit('.analysis-loop-fields input', '2000')
    await assert("!!document.querySelector('.analysis-loop-card') && !document.querySelector('.local-game-selection')")
    await mode('all')
    await assert("document.querySelector('.local-position-fields input').value==='321'")
    // A mock submission verifies the same existing handler and fixed nodes.
    await click('.local-selection-form button[type=submit]')
    await waitFor("window.positionsTest.requests.some(r=>r.path.endsWith('/analysis/run_job'))")
    await assert("window.positionsTest.requests.some(r=>r.body?.total_fens_to_process===321 && r.body?.nodes_limit===100000 && r.body?.batch_size===500)")
    await mode('player')
    await assert("document.querySelector('.local-selection-form input[type=text]').value==='hikaru'")
    await mode('games')
    await assert("!!document.querySelector('.player-game-confirmation')")
    await mode('loop')
    await assert("document.querySelector('.analysis-loop-fields input').value==='2000'")
    await click('.local-activity summary')
    await assert("document.querySelector('.local-activity').open")
    await assert("document.documentElement.scrollWidth<=innerWidth")
    await click('#analysis-local-toggle')
    await click('#analysis-gcloud-toggle')
    await assert("[...document.querySelectorAll('.cloud-selection fieldset')].every(f=>f.disabled)")
    await evaluate("[...document.querySelectorAll('.cloud-selection button')].find(b=>b.textContent==='Build combined player-game job').click()")
    await waitFor("document.querySelector('[aria-label=\"Draft player\"]') && !document.querySelector('.cloud-draft-editor').disabled")
    await edit('[aria-label="Draft player"]', 'keep-my-draft')
    await edit('[aria-label="Draft games"]', '77')
    await click('#analysis-gcloud-toggle')
    await evaluate('window.positionsTest.job.imported=1000')
    // Polling must continue with the cloud panel closed.
    await waitFor("document.querySelector('#analysis-gcloud-toggle').textContent.includes('1,000 / 2,082')")
    await assert("document.querySelector('#analysis-gcloud-panel').hidden")
    await click('#analysis-gcloud-toggle')
    await assert("document.querySelector('[aria-label=\"Draft player\"]').value==='keep-my-draft' && document.querySelector('[aria-label=\"Draft games\"]').value==='77'")
    await assert("window.positionsTest.requests.filter(r=>r.path.endsWith('/drafts/current')).length===1")
    await assert("!window.positionsTest.requests.some(r=>r.method!=='GET' && r.path.includes('/analysis/cloud'))")
    await assert("window.positionsTest.errors.length===0 && window.positionsTest.unexpected.length===0")
    await assert("document.documentElement.scrollWidth<=innerWidth")
    await click('#analysis-gcloud-toggle')
    await evaluate('scrollTo(0,0)')
    if (process.env.POSITIONS_UI_SCREENSHOT && width===1400) {
      const screenshot = await send('browsingContext.captureScreenshot', { context })
      await writeFile(process.env.POSITIONS_UI_SCREENSHOT, Buffer.from(screenshot.data, 'base64'))
    }
    console.log(`PASS: ${width}px, full-width collapsible sections; local selection/preview/submit; background cloud polling; drafts retained; no cloud mutations.`)
  }
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM'); await stopped
  }
  await rm(temporary, { recursive: true, force: true })
}
