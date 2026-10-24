// Actual backup page, synthetic read-only responses: never starts backups/jobs.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile, readFile } from 'node:fs/promises'
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
  const read = name => readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url), 'utf8').then(JSON.parse)
  const [base, scopes, charts] = await Promise.all(['appearance_registry', 'appearance_scopes', 'appearance_charts'].map(read))
  const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: Object.fromEntries(
    Object.entries(scopes.scopes).map(([scope, spec]) => [scope, { ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets } }])) }
  await send('script.addPreloadScript', { functionDeclaration: `() => { window.backupVolumeRegistry=${JSON.stringify(registry)} }` })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw Error(JSON.stringify(response))
    return response.result.value
  }
  const assert = async expression => {
    if (!await evaluate(expression)) throw Error(`Failed: ${expression}\n` + await evaluate(`JSON.stringify({header:[...document.querySelectorAll('.backup-volume-section-head,.backup-volume-measured')].map(n=>({rect:n.getBoundingClientRect().toJSON(),display:getComputedStyle(n).display,margin:getComputedStyle(n).margin,padding:getComputedStyle(n).padding,text:n.textContent})),errors:backupVolumeTest.errors})`))
  }
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) { if (await evaluate(`Boolean(${expression})`)) return; await new Promise(resolve => setTimeout(resolve, 100)) }
    throw Error(`Timeout: ${expression}`)
  }
  const move = async (allocation = null) => {
    const point = allocation ? JSON.parse(await evaluate(`JSON.stringify((() => {
      const n=document.querySelector('[data-allocation="${allocation}"]');n.scrollIntoView({block:'center'});
      const r=n.getBoundingClientRect();return {x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)};
    })())`)) : { x: 5, y: 5 }
    await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [{ type: 'pointerMove', origin: 'viewport', ...point }] }] })
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
  await assert(`document.querySelector('[data-allocation="timeshift"]').getAttribute('aria-label').includes('no published reservation') && document.querySelector('[data-allocation="real-butler"]').getAttribute('aria-label').includes('50.00 GB allocated')`)
  await assert(`document.querySelector('[data-allocation="tiny"]').getBoundingClientRect().width < 1`)
  await assert(`!document.querySelector('.backup-volume-details, .backup-volume-key, .database-backups-location, .database-backups-storage-note') && !document.querySelector('[data-style-id="storage.panel"]').textContent.includes('Blue =')`)
  await assert(`document.querySelector('.backup-volume-tooltip').hidden && document.querySelector('.backup-volume-measured time').dateTime === new Date(backupVolumeTest.volume.measured_at).toISOString()`)
  await assert(`(() => {const stamp=document.querySelector('.backup-volume-measured').getBoundingClientRect(),head=document.querySelector('.backup-volume-section-head').getBoundingClientRect();return Math.abs(stamp.right-head.right)<1 && Math.abs(stamp.top-head.top)<1})()`)
  await waitFor(`document.querySelector('[data-style-id="backup.last"] time')`)
  await assert(`!document.querySelector('.database-backups-table, .database-backups-progress') && document.querySelectorAll('.database-backups-timestamp').length===2 && document.querySelectorAll('.database-backup-action').length===2`)
  await assert(`document.querySelector('[data-style-id="backup.last"] time').dateTime==='2026-10-13T03:00:00.000Z' && document.querySelector('[data-style-id="restore.last"] time').dateTime==='2026-10-13T04:00:00.000Z'`)
  await assert(`document.querySelector('.database-backups-main').lastElementChild.matches('[data-style-id="summary.panel"]') && document.querySelector('.database-backups-summary > [aria-label="Fen Backup"]')`)
  await assert(`(() => {const a=document.querySelector('.database-backups-results').getBoundingClientRect(),b=document.querySelector('[aria-label="Fen Backup"]').getBoundingClientRect();return a.right<b.left && Math.abs(a.top-b.top)<1})()`)
  await assert(`!document.querySelector('[data-style-id="backup.run"]').disabled && document.querySelector('[data-style-id="backup.test"]').disabled`)
  const storageHeight = await evaluate(`document.querySelector('[data-style-id="storage.panel"]').getBoundingClientRect().height`)
  for (const [id, expected] of [['chessism', '125.50 GB used of 200.00 GB allocated'], ['real-butler', '12.00 GB used of 50.00 GB allocated'], ['timeshift', 'no published reservation'], ['__reserve', 'Protected free reserve'], ['__free', 'Unallocated']]) {
    await move(id)
    await waitFor(`!document.querySelector('.backup-volume-tooltip').hidden && document.querySelector('.backup-volume-tooltip').textContent.includes(${JSON.stringify(expected)})`)
    await assert(`document.querySelectorAll('.backup-volume-region[data-highlighted="true"]').length===1 && Math.abs(document.querySelector('[data-style-id="storage.panel"]').getBoundingClientRect().height-${storageHeight})<1`)
    await move(); await waitFor(`document.querySelector('.backup-volume-tooltip').hidden`)
  }
  await evaluate(`document.querySelector('[data-allocation="chessism"]').focus()`)
  await waitFor(`document.querySelector('[data-allocation="chessism"]').dataset.highlighted === 'true' && !document.querySelector('.backup-volume-tooltip').hidden`)
  await evaluate(`document.querySelector('[data-allocation="chessism"]').dispatchEvent(new KeyboardEvent('keydown',{key:'Escape',bubbles:true}))`)
  await waitFor(`document.querySelector('.backup-volume-tooltip').hidden`)
  await evaluate(`document.querySelector('[data-allocation="tiny"]').focus()`)
  await waitFor(`document.querySelector('.backup-volume-tooltip').textContent.includes('tiny app: 4 KB')`)
  await evaluate('document.activeElement.blur()')
  await waitFor(`document.querySelector('.backup-volume-tooltip').hidden`)
  const screenshot = await send('browsingContext.captureScreenshot', { context, origin: 'viewport' })
  await writeFile('/tmp/chessism-backup-volume-desktop.png', Buffer.from(screenshot.data, 'base64'))
  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 844 } })
  await assert(`document.documentElement.scrollWidth <= innerWidth + 1`)
  await assert(`(() => {const a=document.querySelector('.database-backups-results').getBoundingClientRect(),b=document.querySelector('[aria-label="Fen Backup"]').getBoundingClientRect();return a.bottom<b.top && Math.abs(a.left-b.left)<1})()`)
  await assert(`document.querySelector('.backup-volume-disk').getBoundingClientRect().height >= 5 * parseFloat(getComputedStyle(document.documentElement).fontSize)`)
  await move('chessism')
  await waitFor(`!document.querySelector('.backup-volume-tooltip').hidden`)
  await assert(`(() => {const r=document.querySelector('.backup-volume-tooltip').getBoundingClientRect();return r.left>=0 && r.right<=innerWidth+1})()`)
  await move()
  await evaluate('window.backupVolumeTest.fail = true; window.backupVolumeTest.refreshVolume()')
  await waitFor(`document.querySelector('.backup-volume .status-banner')?.textContent.includes('Synthetic disk read error')`)
  await assert(`document.querySelectorAll('.backup-volume-region').length === 7`)
  await evaluate('window.backupVolumeTest.fail = false; window.backupVolumeTest.pending = true; window.backupVolumeTest.refreshVolume()')
  await waitFor(`document.querySelector('.backup-volume-disk').getAttribute('aria-busy') === 'true'`)
  await assert(`!document.querySelector('.backup-volume-region')`)
  await evaluate('window.backupVolumeTest.pending = false; window.backupVolumeTest.volume.allocations[0].reserved_bytes = 2e12; window.backupVolumeTest.refreshVolume()')
  await waitFor(`document.querySelector('.backup-volume .status-banner')?.textContent.includes('exceed disk capacity')`)
  await assert(`!document.querySelector('.backup-volume-region, .backup-volume-key') && document.querySelector('.backup-volume-tooltip').hidden && !document.querySelector('.backup-volume .status-banner').textContent.includes('listed below')`)
  // The hidden hover legend is editable from the surrounding storage panel.
  await evaluate(`[...document.querySelectorAll('button')].find(n=>n.textContent==='Edit appearance').click()`)
  await evaluate(`document.querySelector('[data-style-id="storage.panel"]').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:300,clientY:100}))`)
  await waitFor(`document.querySelector('[data-appearance-target="storage.tooltip"]') && document.querySelector('#appearance-related-background')`)
  await evaluate(`document.querySelector('[data-style-id="storage.measured"]').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:300,clientY:100}))`)
  await waitFor(`document.querySelector('.appearance-editor header strong')?.textContent==='Backup storage · last measured' && document.querySelector('#appearance-color')`)
  for (const [target, label] of [['summary.panel', 'Backup summary and FEN backup container'], ['backup.last', 'Last backup badge'], ['restore.last', 'Last test badge']]) {
    await evaluate(`document.querySelector('[data-style-id="${target}"]').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:300,clientY:100}))`)
    await waitFor(`document.querySelector('.appearance-editor header strong')?.textContent===${JSON.stringify(label)} && document.querySelector('#appearance-hoverBackground') && document.querySelector('#appearance-radius')`)
  }
  await evaluate(`[...document.querySelectorAll('button')].find(n=>n.textContent==='Exit appearance editing').click()`)
  // Remount with synthetic active/failed/empty overviews. No action buttons are clicked.
  await evaluate(`backupVolumeTest.overview.status={status:'running',phase:'copying',detail:'Synthetic backup in progress',started_at:new Date().toISOString(),progress_total_bytes:1000,progress_completed_bytes:250};backupVolumeTest.renderPage()`)
  await waitFor(`document.querySelector('[aria-label="Database backup byte progress"]')?.getAttribute('aria-valuenow')==='250'`)
  await assert(`document.querySelector('[data-style-id="backup.last"] time').dateTime==='2026-10-13T03:00:00.000Z' && [...document.querySelectorAll('.database-backup-action')].every(n=>n.disabled)`)
  await evaluate(`backupVolumeTest.overview.status={status:'success'};backupVolumeTest.overview.restore_test={status:'running',detail:'Synthetic restore in progress'};backupVolumeTest.renderPage()`)
  await waitFor(`document.querySelector('[aria-label="Database restore test status"]')?.textContent.includes('Synthetic restore in progress')`)
  await assert(`!document.querySelector('[aria-label="Database backup status"]') && [...document.querySelectorAll('.database-backup-action')].every(n=>n.disabled)`)
  await evaluate(`backupVolumeTest.overview.status={status:'failed',completed_at:'2026-10-21T03:00:00Z',detail:'Synthetic backup failed'};backupVolumeTest.overview.restore_test={status:'failed',completed_at:'2026-10-21T04:00:00Z',detail:'Synthetic restore failed'};backupVolumeTest.renderPage()`)
  await waitFor(`document.querySelectorAll('.database-backups-progress.failed').length===2`)
  await assert(`document.querySelector('[data-style-id="backup.last"] time').dateTime==='2026-10-13T03:00:00.000Z' && document.querySelector('[data-style-id="restore.last"] time').dateTime==='2026-10-21T04:00:00.000Z' && document.querySelector('[aria-label="Database restore test status"]').textContent.includes('Synthetic restore failed')`)
  await evaluate(`backupVolumeTest.overview.status={status:'stale'};backupVolumeTest.overview.restore_test={status:'not_tested'};backupVolumeTest.overview.backups=[];backupVolumeTest.renderPage()`)
  await waitFor(`[...document.querySelectorAll('.database-backups-timestamp')].filter(n=>n.textContent.includes('Not yet')).length===2`)
  await assert(`!document.querySelector('.database-backups-progress') && document.querySelector('[data-style-id="backup.test"]').disabled && document.querySelector('[aria-label="Fen Backup"]')`)
  await assert(`window.backupVolumeTest.errors.length === 0 && window.backupVolumeTest.requests.every(r=>r.method === 'GET')`)
  console.log('PASS: two-column summary, completion badges, active/error/empty states, FEN backup, hover-only legends, mobile, recovery controls and independent Edit styles; no writes.')
} finally {
  socket?.close(); browser.kill('SIGTERM')
  await Promise.race([once(browser, 'exit'), new Promise(resolve => setTimeout(resolve, 3000))])
  await rm(temporary, { recursive: true, force: true })
}
