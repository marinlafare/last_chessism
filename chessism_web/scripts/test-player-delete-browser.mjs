// Real home/inline-player UI, mock GETs only. Never confirms a deletion.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-delete-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile,
  '--remote-debugging-port', '9256'], { stdio: ['ignore', 'pipe', 'pipe'] })
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
    throw new Error('Timeout: ' + expression)
  }
  const assert = async expression => {
    if (!await evaluate(expression)) throw new Error('Failed: ' + expression)
  }
  for (const [width, height] of [[1400,1000],[390,844]]) {
    await send('browsingContext.setViewport', { context, viewport: { width, height } })
    await send('browsingContext.navigate', { context,
      url: (process.env.HOME_UI_TEST_URL || 'http://localhost:6789') + '/tests/fixtures/home.html?initial=' + encodeURIComponent('/?player=arjunerigaisi'),
      wait: 'complete' })
    await waitFor("document.querySelector('.players-delete-button') && !document.querySelector('.players-delete-button').disabled")
    await evaluate("document.querySelector('.players-delete-button').scrollIntoView({block:'center'}); document.querySelector('.players-delete-button').click()")
    await waitFor("document.querySelector('[role=alertdialog]') !== null")
    const geometry = await evaluate(`JSON.stringify((() => {
      const backdrop=document.querySelector('.player-delete-backdrop').getBoundingClientRect();
      const dialog=document.querySelector('[role=alertdialog]').getBoundingClientRect();
      return {width:innerWidth,height:innerHeight,backdrop:backdrop.toJSON(),dialog:dialog.toJSON()};
    })())`)
    console.log(geometry)
    await assert(`(() => {const r=document.querySelector('[role=alertdialog]').getBoundingClientRect();
      return r.top>=0 && r.bottom<=innerHeight && r.left>=0 && r.right<=innerWidth;})()`)
    await assert("document.querySelector('.player-delete-backdrop').parentElement===document.body")
    await assert(`(() => {const r=document.querySelector('[role=alertdialog]').getBoundingClientRect();
      return document.elementFromPoint(r.x+r.width/2,r.y+r.height/2)?.closest('[role=alertdialog]')!==null;})()`)
    await assert("document.querySelector('.player-delete-actions .btn-danger').disabled && document.querySelector('#player-delete-title').textContent==='Delete arjunerigaisi?'")
    await evaluate(`window.setConfirmation = value => {
      const input=document.querySelector('.player-delete-confirm-field input');
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,value);
      input.dispatchEvent(new Event('input',{bubbles:true}));
    }; window.setConfirmation('wrong-player')`)
    await assert("document.querySelector('.player-delete-actions .btn-danger').disabled")
    await evaluate("window.setConfirmation('arjunerigaisi')")
    await waitFor("!document.querySelector('.player-delete-actions .btn-danger').disabled")
    // Cancel, never click the enabled destructive action.
    await evaluate("document.querySelector('.player-delete-actions .btn-secondary').click()")
    await waitFor("!document.querySelector('[role=alertdialog]')")
    await evaluate("document.querySelector('.players-delete-button').click()")
    await waitFor("document.querySelector('[role=alertdialog]') !== null")
    await assert("document.querySelector('.player-delete-confirm-field input').value==='' && document.querySelector('.player-delete-actions .btn-danger').disabled")
    await evaluate("document.querySelector('.player-delete-backdrop').click()")
    await waitFor("!document.querySelector('[role=alertdialog]')")
    await assert("window.homeTest.unexpected.length===0 && window.homeTest.requests.every(r=>r.method==='GET')")
    console.log(`PASS: ${width}px deletion preview visible above the scrolled page; confirmation and cancellation intact; zero writes.`)
  }
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM'); await stopped
  }
  await rm(temporary, { recursive: true, force: true })
}
