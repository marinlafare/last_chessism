// Mounted React lifecycle checks. All application requests are synthetic.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { once } from 'node:events'

const web = fileURLToPath(new URL('../', import.meta.url))
const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-stage-c-browser-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
await writeFile(path.join(profile, 'user.js'), 'user_pref("app.update.auto", false);\nuser_pref("browser.shell.checkDefaultBrowser", false);\n')
const children = [], pending = new Map()
let socket, nextId = 0
const pause = ms => new Promise(resolve => setTimeout(resolve, ms))
const launch = (command, args, options = {}) => {
  const child = spawn(command, args, { stdio: ['ignore', 'pipe', 'pipe'], ...options })
  children.push(child)
  return child
}
try {
  const vite = launch(process.execPath, [path.join(web, 'node_modules/vite/bin/vite.js'), '--host', '127.0.0.1', '--port', '16789', '--strictPort'], { cwd: web })
  let serverLog = ''
  vite.stdout.on('data', chunk => { serverLog += chunk }); vite.stderr.on('data', chunk => { serverLog += chunk })
  for (let n = 0; n < 100 && !serverLog.includes('16789'); n++) {
    if (vite.exitCode !== null) throw new Error(serverLog)
    await pause(50)
  }
  if (!serverLog.includes('16789')) throw new Error('Isolated Vite startup timed out: ' + serverLog)
  const browser = launch('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9376'])
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Browser startup timeout')), 15000)
    const read = chunk => {
      const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/)
      if (match) { clearTimeout(timer); resolve(match[1]) }
    }
    browser.stdout.on('data', read); browser.stderr.on('data', read); browser.once('error', reject)
  })
  socket = new WebSocket(endpoint.replace(/\/$/, '') + '/session')
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject })
  socket.onmessage = ({ data }) => {
    const message = JSON.parse(data), entry = pending.get(message.id)
    if (!entry) return
    pending.delete(message.id)
    message.type === 'error' ? entry.reject(new Error(JSON.stringify(message))) : entry.resolve(message.result)
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId; pending.set(id, { resolve, reject }); socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  const evaluate = async expression => {
    const value = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (value.type === 'exception') throw new Error(JSON.stringify(value))
    return value.result.value
  }
  await send('browsingContext.navigate', { context, url: 'http://127.0.0.1:16789/tests/fixtures/stageC.html', wait: 'complete' })
  for (let n = 0; n < 150; n++) {
    if (await evaluate('window.stageC?.finished')) break
    await pause(100)
  }
  const results = JSON.parse(await evaluate('JSON.stringify(window.stageC || { error: document.body.innerText })'))
  console.log(JSON.stringify(results, null, 2))
  if (!results.finished || results.errors.length || results.checks.some(check => !check.passed)) process.exitCode = 1
  await send('session.end', {})
} finally {
  socket?.close()
  for (const child of children.reverse()) {
    if (child.exitCode !== null || child.signalCode) continue
    const ended = once(child, 'exit')
    child.kill('SIGTERM')
    await ended
  }
  await rm(temporary, { recursive: true, force: true })
}
