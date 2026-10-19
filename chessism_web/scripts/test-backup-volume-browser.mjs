// Actual backup page, synthetic read-only responses: never starts backups/jobs.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-backup-volume-ui-'))
const profile = path.join(temporary, 'profile'); await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile,
  '--remote-debugging-port', '9261'], { stdio: ['ignore', 'pipe', 'pipe'] })
let socket
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('Browser startup timeout')), 15000)
    const output = chunk => { const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/); if (match) { clearTimeout(timer); resolve(match[1]) } }
    browser.stdout.on('data', output); browser.stderr.on('data', output)
    browser.once('error', error => { clearTimeout(timer); reject(error) })
  })
  socket = new WebSocket(endpoint.replace(/\/$/, '') + '/session')
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject })
  const pending = new Map(); let nextId = 0
  socket.onmessage = ({ data }) => {
    const response = JSON.parse(data), call = pending.get(response.id)
    if (!call) return
    pending.delete(response.id); clearTimeout(call.timer)
    response.type === 'error' ? call.reject(Error(JSON.stringify(response))) : call.resolve(response.result)
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId, timer = setTimeout(() => { pending.delete(id); reject(Error(`Timeout: ${method}`)) }, 30000)
    pending.set(id, { resolve, reject, timer }); socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw Error(JSON.stringify(response))
    return response.result.value
  }
  const assert = async expression => { if (!await evaluate(expression)) throw Error(`Failed: ${expression}`) }
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) { if (await evaluate(`Boolean(${expression})`)) return; await new Promise(resolve => setTimeout(resolve, 100)) }
    throw Error(`Timeout: ${expression}`)
  }
  await send('browsingContext.setViewport', { context, viewport: { width: 1400, height: 1100 } })
  await send('browsingContext.navigate', { context, wait: 'complete',
    url: (process.env.BACKUP_UI_TEST_URL || 'http://localhost:6789') + '/tests/fixtures/backupVolume.html' })
  await waitFor('document.querySelectorAll(".backup-volume-region").length === 7')
  await assert(`document.querySelector('.backup-volume-region').dataset.allocation==='chessism' && document.querySelectorAll('.backup-volume-region')[1].dataset.allocation==='__free' && Array.from(document.querySelectorAll('.backup-volume-region')).at(-1).dataset.allocation==='__reserve'`)
  await assert(`!document.querySelector('.database-backups-stats, .database-backups-meter')`)
  await assert(`document.querySelector('.backup-volume-disk').getBoundingClientRect().height >= 5 * parseFloat(getComputedStyle(document.documentElement).fontSize)`)
  await assert(`Math.abs(parseFloat(document.querySelector('[data-allocation="chessism"]').style.width) - 200 / 847 * 100) < 0.001`)
  await assert(`document.querySelector('[data-allocation="chessism"] .backup-volume-fill').style.width === '62.74999999999999%' || Math.abs(parseFloat(document.querySelector('[data-allocation="chessism"] .backup-volume-fill').style.width) - 62.75) < 0.001`)
  await assert(`getComputedStyle(document.querySelector('.backup-volume-fill')).backgroundColor === 'rgb(35, 122, 195)'`)
  await assert(`document.querySelector('[data-allocation="timeshift"]').title.includes('no published reservation') && document.querySelector('[data-allocation="real-butler"]').title.includes('50.00 GB allocated')`)
  await assert(`document.querySelector('[data-allocation="tiny"]').getBoundingClientRect().width < 1`)
  await assert(`!document.querySelector('.backup-volume-details').open`)
  await evaluate(`document.querySelector('[data-allocation="chessism"]').focus()`)
  await assert(`document.querySelector('[data-allocation="chessism"]').dataset.highlighted === 'true'`)
  const screenshot = await send('browsingContext.captureScreenshot', { context, origin: 'viewport' })
  await writeFile('/tmp/chessism-backup-volume-desktop.png', Buffer.from(screenshot.data, 'base64'))
  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 844 } })
  await assert(`document.documentElement.scrollWidth <= innerWidth + 1`)
  await assert(`document.querySelector('.backup-volume-disk').getBoundingClientRect().height >= 5 * parseFloat(getComputedStyle(document.documentElement).fontSize)`)
  await evaluate('window.backupVolumeTest.fail = true; window.backupVolumeTest.refreshVolume()')
  await waitFor(`document.querySelector('.backup-volume .status-banner')?.textContent.includes('Synthetic disk read error')`)
  await assert(`document.querySelectorAll('.backup-volume-region').length === 7`)
  await evaluate('window.backupVolumeTest.fail = false; window.backupVolumeTest.pending = true; window.backupVolumeTest.refreshVolume()')
  await waitFor(`document.querySelector('.backup-volume-disk').getAttribute('aria-busy') === 'true'`)
  await assert(`!document.querySelector('.backup-volume-region')`)
  await evaluate('window.backupVolumeTest.pending = false; window.backupVolumeTest.volume.allocations[0].reserved_bytes = 2e12; window.backupVolumeTest.refreshVolume()')
  await waitFor(`document.querySelector('.backup-volume .status-banner')?.textContent.includes('exceed disk capacity')`)
  await assert(`!document.querySelector('.backup-volume-region') && document.querySelector('.backup-volume-key').textContent.includes('2.00 TB allocated')`)
  await assert(`window.backupVolumeTest.errors.length === 0 && window.backupVolumeTest.requests.every(r=>r.method === 'GET')`)
  console.log('PASS: disk/quota/used proportions, physical sizes, tiny/empty folders, desktop/mobile, keyboard/hover, pending/errors/over-allocation, no writes.')
} finally {
  socket?.close(); browser.kill('SIGTERM')
  await Promise.race([once(browser, 'exit'), new Promise(resolve => setTimeout(resolve, 3000))])
  await rm(temporary, { recursive: true, force: true })
}
