// Isolated page and mocked API only; never touches production data or cloud jobs.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-salience-ui-'))
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
    if (pending) {
      requests.delete(response.id)
      response.type === 'error' ? pending.reject(new Error(JSON.stringify(response))) : pending.resolve(response.result)
    }
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId; requests.set(id, { resolve, reject }); socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  await send('browsingContext.setViewport', { context, viewport: { width: 1280, height: 900 } })
  await send('browsingContext.navigate', { context, url: (process.env.SALIENCE_UI_TEST_URL || 'http://localhost:6789') + '/tests/fixtures/dailySalience.html', wait: 'complete' })
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const waitFor = async expression => {
    for (let i = 0; i < 80; i++) {
      if (await evaluate(expression)) return
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw new Error('Timeout: ' + expression)
  }
  const assert = async expression => { if (!await evaluate(expression)) throw new Error('Failed: ' + expression) }
  await waitFor("document.querySelectorAll('.measure-daily-efficiency-point').length === 5")
  // Year navigation is on the plot, not a row of year buttons.
  await assert("document.querySelectorAll('.measure-daily-years button').length === 1 && document.querySelector('.measure-daily-years button').textContent === 'All'")
  const yearGeometry = JSON.parse(await evaluate(`JSON.stringify((() => {
    const r = document.querySelector('.daily-year-band[data-year="2024"]').getBoundingClientRect();
    return {x:Math.round(r.x+r.width/2), y:Math.round(r.y+r.height/2)};
  })())`))
  await send('input.performActions', {context, actions:[{type:'pointer', id:'mouse', parameters:{pointerType:'mouse'}, actions:[
    {type:'pointerMove', x:yearGeometry.x, y:yearGeometry.y},
  ]}]})
  await waitFor(`document.querySelector('.daily-year-band.highlighted')?.dataset.year === '2024' && document.querySelector('.daily-year-label')?.textContent === '2024'`)
  if (process.env.YEAR_SCREENSHOT) {
    const result = await send('browsingContext.captureScreenshot', {context})
    await writeFile(process.env.YEAR_SCREENSHOT, Buffer.from(result.data, 'base64'))
  }
  await send('input.performActions', {context, actions:[{type:'pointer', id:'mouse', parameters:{pointerType:'mouse'}, actions:[
    {type:'pointerDown',button:0}, {type:'pointerUp',button:0},
  ]}]})
  await waitFor("document.querySelectorAll('.measure-daily-efficiency-point').length === 4 && document.querySelector('.measure-daily-axis-title').textContent === 'DATE · 2024'")
  await assert("document.querySelectorAll('.daily-year-band').length === 0 && document.querySelector('.measure-daily-years button').getAttribute('aria-pressed') === 'false'")
  await evaluate("document.querySelector('.measure-daily-years button').click()")
  await waitFor("document.querySelectorAll('.measure-daily-efficiency-point').length === 5")
  // Keyboard navigation also handles the final year with just one point.
  await evaluate(`document.querySelector('.daily-year-band[data-year="2025"]').focus()`)
  await waitFor(`document.querySelector('.daily-year-band.highlighted')?.dataset.year === '2025'`)
  await evaluate(`document.querySelector('.daily-year-band[data-year="2025"]').dispatchEvent(new KeyboardEvent('keydown', {key:'Enter',bubbles:true}))`)
  await waitFor("document.querySelectorAll('.measure-daily-efficiency-point').length === 1 && document.querySelector('.measure-daily-axis-title').textContent === 'DATE · 2025'")
  await assert("document.activeElement === document.querySelector('.measure-daily-years button')")
  await evaluate("document.querySelector('.measure-daily-years button').click()")
  await waitFor("document.querySelectorAll('.measure-daily-efficiency-point').length === 5")
  await evaluate(`window.salienceSnapshot = () => JSON.stringify({
    axes: Array.from(document.querySelectorAll('svg text'), n => [n.textContent, n.getAttribute('x'), n.getAttribute('y')]),
    middle: ['cx','cy'].map(k => document.querySelector('[data-date="2024-01-03"]').getAttribute(k))
  }); window.beforeSalience = window.salienceSnapshot();
  window.salienceKey = (index, key) => document.querySelector('[data-salience-handle="'+index+'"]').dispatchEvent(new KeyboardEvent('keydown', {key, bubbles:true}));
  window.salienceValues = () => Array.from(document.querySelectorAll('[role="slider"]'), n => Number(n.getAttribute('aria-valuenow')));
  window.beforeRequests = window.salienceTest.requests.length`)
  await assert("Array.from(document.querySelectorAll('.measure-daily-efficiency-point')).every(n => n.getAttribute('r') === '4' && n.getAttribute('opacity') === '1')")
  await assert("document.querySelector('.daily-salience-control').textContent.includes('0 = 0.2000; 100 = 0.7000')")
  await assert("document.querySelector('.daily-salience-control').textContent.includes('Average per game') && document.body.textContent.includes('whole days, not individual games')")
  await assert("document.querySelector('.daily-salience-track').getBoundingClientRect().height > document.querySelector('.daily-salience-track').getBoundingClientRect().width")
  for (let i=0; i<3; i++) {
    await evaluate("window.salienceKey(0, 'PageUp')")
    await waitFor(`window.salienceValues()[0] === ${(i + 1) * 10}`)
    await evaluate("window.salienceKey(1, 'PageDown')")
    await waitFor(`window.salienceValues()[1] === ${100 - (i + 1) * 10}`)
  }
  await assert("document.querySelector('.daily-salience-count').textContent === 'Showing 3 of 5 days'")
  await assert("window.beforeSalience === window.salienceSnapshot() && window.salienceTest.requests.length === window.beforeRequests")
  await assert(`document.querySelector('[data-date="2024-01-03"] title').textContent.includes('0.4500 average game salience') && document.querySelector('[data-date="2024-01-03"] title').textContent.includes('4.50 total effective games')`)
  await assert(`document.querySelector('[data-date="2024-01-03"]').getAttribute('aria-label').includes('0.4500 average game salience')`)
  await evaluate("document.querySelector('[data-date=" + JSON.stringify('2024-01-03') + "]').dispatchEvent(new MouseEvent('click', {bubbles:true}))")
  await assert("window.salienceTest.selections[0].date === '2024-01-03' && window.salienceTest.selections[0].minimum_game_moves_exclusive === 10")
  await assert("document.querySelector('.measure-daily-axis-title').textContent === 'DATE · ALL YEARS' && document.querySelectorAll('.daily-year-band').length === 2")
  // Empty range, collision, reset, and the original axes survive filtering.
  await evaluate("window.salienceKey(0, 'End')")
  await waitFor("window.salienceValues()[0] === 70")
  await evaluate("window.salienceKey(0, 'ArrowDown')")
  await waitFor("window.salienceValues()[0] === 69")
  await evaluate("window.salienceKey(1, 'ArrowDown')")
  await waitFor("document.body.textContent.includes('No days in this salience range.')")
  await assert("document.querySelectorAll('.measure-daily-x-tick').length === 7 && window.salienceValues().join(',') === '69,69'")
  await evaluate("document.querySelector('.daily-salience-reset').click()")
  await waitFor("document.querySelectorAll('.measure-daily-efficiency-point').length === 5")
  // Real pointer input: drag the lower handle to 50, without a server request.
  const geometry = JSON.parse(await evaluate(`JSON.stringify((() => {
    const t=document.querySelector('.daily-salience-track').getBoundingClientRect();
    const h=document.querySelector('[data-salience-handle="0"]').getBoundingClientRect();
    return {x:Math.round(h.x+h.width/2), y:Math.round(h.y+h.height/2), middle:Math.round(t.y+t.height/2)};
  })())`))
  await send('input.performActions', { context, actions: [{ type:'pointer', id:'mouse', parameters:{pointerType:'mouse'}, actions:[
    {type:'pointerMove',x:geometry.x,y:geometry.y}, {type:'pointerDown',button:0},
    {type:'pointerMove',x:geometry.x,y:geometry.middle,duration:250}, {type:'pointerUp',button:0},
  ] }] })
  await waitFor("window.salienceValues()[0] === 50 && document.querySelectorAll('.measure-daily-efficiency-point').length === 3")
  await assert("window.beforeSalience === window.salienceSnapshot()")
  // Changing year keeps the full-series normalization and selected range.
  await evaluate(`document.querySelector('.daily-year-band[data-year="2024"]').dispatchEvent(new MouseEvent('click', {bubbles:true}))`)
  await waitFor("document.querySelector('.daily-salience-count').textContent === 'Showing 2 of 4 days'")
  await assert("document.querySelector('.daily-salience-control').textContent.includes('100 = 0.7000') && window.salienceValues()[0] === 50")
  await evaluate("document.querySelector('.measure-daily-years button').click()")
  await waitFor("document.querySelector('.daily-salience-count').textContent === 'Showing 3 of 5 days'")
  await assert("window.salienceValues()[0] === 50 && window.salienceTest.requests.length === window.beforeRequests")
  await evaluate(`document.querySelector('.daily-year-band[data-year="2024"]').dispatchEvent(new KeyboardEvent('keydown', {key:' ',bubbles:true}))`)
  await waitFor("document.querySelector('.daily-salience-count').textContent === 'Showing 2 of 4 days'")
  // Changing game types resets the range; it is a new salience series.
  await evaluate("Array.from(document.querySelectorAll('.behavior-mode-controls button')).find(b=>b.textContent.trim()==='bullet').click()")
  await waitFor("window.salienceValues()[0] === 0 && document.querySelector('.daily-salience-count').textContent === 'Showing 4 of 4 days'")
  if (process.env.SALIENCE_SCREENSHOT) {
    const result = await send('browsingContext.captureScreenshot', {context})
    await writeFile(process.env.SALIENCE_SCREENSHOT, Buffer.from(result.data, 'base64'))
  }
  for (const [player, count] of [['equal',5], ['single',1], ['empty',0], ['missing',5]]) {
    await evaluate(`window.salienceTest.show('${player}')`)
    await waitFor(`document.querySelectorAll('.measure-daily-efficiency-point').length === ${count} && document.querySelectorAll('[role="slider"]:disabled').length === 2`)
    await assert("window.salienceValues().join(',') === '0,100'")
  }
  await assert("document.body.textContent.includes('Salience unavailable.') && document.body.textContent.includes('unweighted Accuracy')")
  await evaluate("window.salienceTest.show('dense')")
  await waitFor("document.querySelectorAll('.measure-daily-efficiency-point').length === 3000")
  for (let i=0; i<5; i++) {
    await evaluate("window.salienceKey(0, 'PageUp')")
    await waitFor(`window.salienceValues()[0] === ${(i + 1) * 10}`)
  }
  await assert("document.querySelector('.daily-salience-count').textContent === 'Showing 1,500 of 3,000 days'")
  await evaluate("window.salienceTest.show('example')")
  await waitFor(`document.querySelectorAll('[role="slider"]:enabled').length === 2`)
  await send('browsingContext.setViewport', {context, viewport:{width:390,height:844}})
  await assert("document.documentElement.scrollWidth <= 390 && getComputedStyle(document.querySelector('.measure-chart-section')).overflowX === 'auto'")
  await assert("window.salienceTest.requests.every(r => r.method === 'GET' && r.path.includes('/analysis/measures/daily-'))")
  console.log('PASS: calendar year hover/click/keyboard zoom, All reset, day drilldown without zoom, single-day year, retained salience filter, average-per-game salience, fixed-size dots, stable axes/year scale, keyboard/pointer controls, missing/equal/empty states and mobile overflow.')
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM'); await stopped
  }
  await rm(temporary, {recursive:true, force:true})
}
