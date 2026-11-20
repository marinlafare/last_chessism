import React, { act, useState, useLayoutEffect } from 'react'
import { createRoot } from 'react-dom/client'
import { usePositionsPage } from '/src/pages/positions/usePositionsPage.js'
import { useJobCompletionPresentation } from '/src/pages/positions/useJobCompletionPresentation.js'
import { COMPLETED_JOB_VISIBLE_MS, COMPLETED_JOB_FADE_MS } from '/src/pages/positions/positionPageSupport.js'
import LocalGameSelection from '/src/pages/positions/LocalGameSelection.jsx'
import MainCharacters from '/src/pages/MainCharacters.jsx'
import PlayerWorkspace from '/src/pages/players/PlayerWorkspace.jsx'
import DownloadNewGames from '/src/pages/DownloadNewGames.jsx'
import { useTrackedGameJob } from '/src/services/useTrackedGameJob.js'
import { fetchGameJobStatus } from '/src/pages/download-new-games/downloadNewGamesApi.js'
import { UPDATE_JOB_STORAGE_KEY, PLAYER_DELETE_JOB_STORAGE_KEY, storeJob } from '/src/services/gameJobService.js'

window.IS_REACT_ACT_ENVIRONMENT = true
const results = window.stageC = { checks: [], errors: [], finished: false }
window.addEventListener('error', event => results.errors.push(event.message))
window.addEventListener('unhandledrejection', event => results.errors.push(String(event.reason)))
const assert = (value, message) => { if (!value) throw new Error(message) }
const equal = (actual, expected, message) => assert(JSON.stringify(actual) === JSON.stringify(expected), `${message}: ${JSON.stringify(actual)} != ${JSON.stringify(expected)}`)
let root, page, tracker, requests, route, timers, timerId = 0
const realTimeout = window.setTimeout, realClear = window.clearTimeout
window.setTimeout = (fn, ms) => { timers.set(++timerId, { fn, ms }); return timerId }
window.clearTimeout = id => timers.delete(id)
Object.defineProperty(document, 'visibilityState', { configurable: true, get: () => 'visible' })
const response = (value, status = 200) => ({ ok: status >= 200 && status < 300, status, json: async () => value })
const deferred = () => {
  let resolve, reject
  const promise = new Promise((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve: value => resolve(response(value)), reject }
}
const defaultRoute = ({ path }) => {
  if (path === '/players/main_characters/time_controls') return response({ bullet: 1, blitz: 1, rapid: 1 })
  if (path === '/players/main_characters/top') return response({ players: [{ player_name: 'alice', n_games: 5, rating: 2000, score_rate: 0.5 }] })
  if (/\/players\/\w+\/hero/.test(path)) return response({ player_name: path.split('/')[2], joined: 1600000000 })
  if (path === '/jobs/active' || path === '/jobs/analysis') return response({ jobs: [] })
  if (path === '/fens/pipeline/timings/latest') return response({ run: null })
  if (path.startsWith('/jobs/')) return response({ job_id: path.split('/').at(-1), status: 'in_progress' })
  if (path.startsWith('/players/') || path.startsWith('/games/') || path.startsWith('/fens/')) return response({})
  throw new Error(`Unmocked route ${path}`)
}
window.fetch = async (input, options = {}) => {
  const url = new URL(input, location.href)
  const request = { path: url.pathname.replace(/^\/api/, ''), query: url.search, method: options.method || 'GET',
    body: options.body ? JSON.parse(options.body) : null, signal: options.signal }
  requests.push(request)
  return route(request) ?? defaultRoute(request)
}
const flush = () => act(async () => {})
const mount = async component => { root = createRoot(document.getElementById('root')); await act(async () => root.render(component)) }
const unmount = async () => { if (root) { await act(async () => root.unmount()); root = null } }
const runDelay = async ms => {
  const entries = [...timers].filter(([, timer]) => timer.ms === ms)
  assert(entries.length, `Expected timer ${ms}ms`)
  await act(async () => { for (const [id, timer] of entries) { timers.delete(id); timer.fn() } })
}
const click = async selector => {
  const node = document.querySelector(selector)
  assert(node, `Missing ${selector}`)
  await act(async () => node.click())
}
const complete = (success = true, value = {}) => ({ status: 'complete', result: { success, result: value } })
function Positions() { page = usePositionsPage(); return <LocalGameSelection page={page} /> }
function Tracked() { tracker = useTrackedGameJob(UPDATE_JOB_STORAGE_KEY, fetchGameJobStatus); return <p>{tracker.message}</p> }
const change = values => act(async () => page.setPlayerGameAnalysis(current => ({ ...current, ...values })))
const inspect = () => act(async () => { void page.handleInspectPlayerGameScope({ preventDefault() {} }) })
const preview = () => act(async () => { void page.handlePreviewPlayerGameAnalysis() })
const test = async (name, run) => {
  await unmount()
  localStorage.clear(); page = null; requests = []; route = () => null; timers = new Map()
  try { await run(); results.checks.push({ name, passed: true }) }
  catch (error) { results.checks.push({ name, passed: false, error: error.message + '\n' + error.stack, requests: requests.map(({ path, method, body }) => ({ path, method, body })), timers: [...timers.values()].map(t => t.ms) }) }
  await unmount()
}
const plan = (id, player = 'alice') => ({ plan_id: id, player_name: player, selected_games: 1, fens_to_analyze: 20 })

await test('Positions ignores Alice scope after switching to Bob', async () => {
  const old = deferred(); route = req => req.path.endsWith('/scope') ? old.promise : null
  await mount(<Positions />); await change({ playerName: 'alice' }); await inspect()
  const request = requests.find(req => req.path.endsWith('/scope'))
  await change({ playerName: 'bob' }); old.resolve({ player_name: 'alice' }); await flush()
  equal(page.playerGameAnalysis.scope, null, 'stale scope rejected')
  assert(request.signal?.aborted, 'obsolete scope request aborted')
})
await test('Positions rejects old Alice preview after Alice/Bob/Alice and a newer preview', async () => {
  const old = deferred(); let calls = 0
  route = req => req.path.endsWith('/scope') ? response({ player_name: req.body.player_name })
    : req.path.endsWith('/preview') ? (++calls === 1 ? old.promise : response(plan('new'))) : null
  await mount(<Positions />); await change({ playerName: 'alice' }); await inspect(); await preview()
  await change({ playerName: 'bob' }); await change({ playerName: 'alice' }); await inspect(); await preview()
  old.resolve(plan('old')); await flush()
  equal(page.playerGamePreview?.plan_id, 'new', 'latest revision wins')
})
await test('changing preview parameters invalidates the in-flight request and confirmation', async () => {
  const old = deferred()
  route = req => req.path.endsWith('/scope') ? response({ player_name: 'alice' }) : req.path.endsWith('/preview') ? old.promise : null
  await mount(<Positions />); await change({ playerName: 'alice' }); await inspect(); await preview()
  await change({ gameLimit: 7 }); old.resolve(plan('old')); await flush()
  await act(async () => { await page.handleConfirmPlayerGameAnalysis() })
  equal(page.playerGamePreview, null, 'preview remains invalid')
  assert(!requests.some(req => req.path.endsWith('/run_job')), 'stale plan not submitted')
})
await test('confirmation consumes the current frozen plan once and preserves a newer selection', async () => {
  const queued = deferred()
  route = req => req.path.endsWith('/scope') ? response({ player_name: req.body.player_name })
    : req.path.endsWith('/preview') ? response(plan(req.body.player_name, req.body.player_name))
    : req.path.endsWith('/player_games/run_job') ? queued.promise : null
  await mount(<Positions />); await change({ playerName: 'alice' }); await inspect(); await preview()
  await act(async () => { void page.handleConfirmPlayerGameAnalysis(); void page.handleConfirmPlayerGameAnalysis() })
  const writes = requests.filter(req => req.path.endsWith('/run_job'))
  equal(writes.length, 1, 'one submission'); equal(writes[0].body.plan_id, 'alice', 'frozen ID')
  await change({ playerName: 'bob' }); await inspect(); await preview()
  queued.resolve({ job_id: 'alice-job' }); await flush()
  equal(page.playerGamePreview?.plan_id, 'bob', 'new preview survived old enqueue')
})
await test('Positions aborts outstanding preview on unmount', async () => {
  const pending = deferred()
  route = req => req.path.endsWith('/scope') ? response({ player_name: 'alice' }) : req.path.endsWith('/preview') ? pending.promise : null
  await mount(<Positions />); await change({ playerName: 'alice' }); await inspect(); await preview()
  const request = requests.find(req => req.path.endsWith('/preview')); await unmount()
  assert(request.signal?.aborted, 'unmount aborts preview'); pending.resolve(plan('late')); await flush()
})
await test('Positions player-count inspection rejects Alice/Bob/Alice stale data', async () => {
  const pending = deferred(); route = req => req.path.endsWith('/alice/analysis_counts') ? pending.promise : null
  await mount(<Positions />)
  await act(async () => { page.setPlayerJob(value => ({ ...value, playerName: 'alice' })) })
  await act(async () => { void page.handleInspectPlayer() })
  await act(async () => { page.setPlayerJob(value => ({ ...value, playerName: 'bob' })); page.setPlayerJob(value => ({ ...value, playerName: 'alice' })) })
  pending.resolve({ player_name: 'alice', total_fens: 999 }); await flush()
  equal(page.playerInspection.data, null, 'old counts rejected')
})
await test('Positions restored job survives a proxy error and waits for authoritative failure', async () => {
  let calls = 0
  localStorage.setItem('chessism:positions:jobs', JSON.stringify({ global: { jobId: 'analysis-one' } }))
  route = req => req.path === '/jobs/analysis-one' ? (++calls === 1 ? Promise.reject(new Error('offline'))
    : calls === 2 ? response({ status: 'complete', progress: { phase: 'complete' } })
    : response(complete(false, { message: 'database failed' }))) : null
  await mount(<Positions />)
  equal(page.jobState.global.jobId, 'analysis-one', 'job retained')
  await runDelay(6000); assert(!page.jobState.global.completedAt, 'telemetry cannot announce completion')
  await runDelay(3000); equal(page.jobState.global.status.result.success, false, 'real failure visible')
})
await test('a pre-failure queued discovery cannot revive failure; a subsequent requeue can', async () => {
  const old = deferred(); let reads = 0
  localStorage.setItem('chessism:positions:jobs', JSON.stringify({ global: { jobId: 'analysis-one' } }))
  route = req => req.path === '/jobs/active' ? old.promise
    : req.path === '/jobs/analysis-one' ? response(++reads === 1 ? complete(false, { message: 'failed' }) : { status: 'in_progress' }) : null
  await mount(<Positions />)
  equal(page.jobState.global.status.result.success, false, 'confirmed failure')
  old.resolve({ jobs: [{ job_id: 'analysis-one', status: 'deferred', info: { function: 'run_analysis_job' }, progress: { kind: 'most_repeated' } }] })
  await flush()
  equal(page.jobState.global.status.result.success, false, 'old queue read ignored')
  await runDelay(3000)
  equal(page.jobState.global.status.status, 'in_progress', 'fresh recovery read resumes tracking')
})
await test('late Positions discovery cannot overwrite a newer completed job', async () => {
  const old = deferred()
  route = req => req.path === '/jobs/active' ? old.promise
    : req.path === '/analysis/run_job' ? response({ job_id: 'new-analysis' }, 202)
    : req.path === '/jobs/new-analysis' ? response(complete()) : null
  await mount(<Positions />)
  await act(async () => { void page.handleGlobalAnalysis({ preventDefault() {} }) })
  equal(page.jobState.global.jobId, 'new-analysis', 'new job tracked')
  equal(page.jobState.global.status.result.success, true, 'new job complete')
  old.resolve({ jobs: [{ job_id: 'old-analysis', status: 'in_progress', info: { function: 'run_analysis_job' }, progress: { kind: 'most_repeated' } }] })
  await flush()
  equal(page.jobState.global.jobId, 'new-analysis', 'old discovery rejected')
})
await test('completion presentation cleans timers and cannot dismiss a replacement job', async () => {
  const previousObserver = window.IntersectionObserver, previousFocus = document.hasFocus
  window.IntersectionObserver = class {
    constructor(callback) { this.callback = callback }
    observe(target) { this.callback([{ target, isIntersecting: true, intersectionRatio: 1 }]) }
    unobserve() {} disconnect() {}
  }
  document.hasFocus = () => true
  let replace, state
  function Presentation() {
    const [jobs, setJobs] = useState({ global: { jobId: 'old', status: complete() } })
    state = jobs; replace = setJobs
    const patch = (key, value, expected) => setJobs(previous => previous[key]?.jobId === expected
      ? { ...previous, [key]: { ...previous[key], ...value } } : previous)
    const clear = (key, expected) => setJobs(previous => previous[key]?.jobId === expected ? {} : previous)
    const display = useJobCompletionPresentation(jobs, null, patch, clear)
    return <div ref={display.setJobCardRef('global')}>job</div>
  }
  try {
    await mount(<Presentation />); await runDelay(COMPLETED_JOB_VISIBLE_MS)
    assert(state.global.fading, 'confirmed completion fades')
    await act(async () => replace({ global: { jobId: 'replacement', status: { status: 'in_progress' } } }))
    await runDelay(COMPLETED_JOB_FADE_MS)
    equal(state.global.jobId, 'replacement', 'old fade cannot dismiss new job')
    await act(async () => replace({ global: { jobId: 'replacement', status: complete() } }))
    assert(timers.size > 0, 'new completed job has its own timer')
    await unmount(); equal(timers.size, 0, 'unmount clears presentation timers')
  } finally { window.IntersectionObserver = previousObserver; document.hasFocus = previousFocus }
})
await test('tracked jobs ignore an old response after replacement and restore on remount', async () => {
  const old = deferred(); route = req => req.path === '/jobs/old' ? old.promise : null
  await mount(<Tracked />)
  await act(async () => tracker.track({ jobId: 'old', playerName: 'alice' }))
  await act(async () => tracker.track({ jobId: 'new', playerName: 'bob' }))
  old.resolve(complete()); await flush()
  equal(tracker.job.jobId, 'new', 'replacement retained')
  await unmount(); await mount(<Tracked />)
  equal(tracker.job.jobId, 'new', 'restored persistent ID')
})
await test('Player Workspace retains update after repeated outages and refreshes only on success', async () => {
  localStorage.setItem(UPDATE_JOB_STORAGE_KEY, JSON.stringify({ jobId: 'update', playerName: 'alice' }))
  let calls = 0
  route = req => req.path === '/jobs/update' ? (++calls < 3 ? Promise.reject(new Error('offline')) : response(complete(true, { message: 'Ingestion complete; FEN extraction pending.' }))) : null
  await mount(<PlayerWorkspace playerName="alice" />)
  const before = requests.filter(req => req.path.endsWith('/hero')).length
  assert(localStorage.getItem(UPDATE_JOB_STORAGE_KEY), 'tracking retained after first failure')
  await runDelay(3000); assert(localStorage.getItem(UPDATE_JOB_STORAGE_KEY), 'tracking retained after repeated failure')
  await runDelay(6000)
  equal(localStorage.getItem(UPDATE_JOB_STORAGE_KEY), null, 'confirmed completion clears tracking')
  assert(requests.filter(req => req.path.endsWith('/hero')).length > before, 'successful update refreshes profile')
})
await test('Player deletion missing response never navigates and transient 404 retains tracking', async () => {
  localStorage.setItem(PLAYER_DELETE_JOB_STORAGE_KEY, JSON.stringify({ jobId: 'delete', playerName: 'alice', redirectPlayer: 'bob' }))
  let calls = 0, navigations = []
  route = req => req.path === '/jobs/delete' ? (++calls === 1 ? response({ detail: 'proxy missing' }, 404)
    : response({ job_id: 'delete', status: 'not_found' }, 404)) : null
  await mount(<PlayerWorkspace playerName="alice" onSelectPlayer={name => navigations.push(name)} />)
  assert(localStorage.getItem(PLAYER_DELETE_JOB_STORAGE_KEY), 'proxy error retains deletion')
  await runDelay(3000)
  equal(navigations, [], 'unknown outcome cannot navigate as deleted')
  equal(localStorage.getItem(PLAYER_DELETE_JOB_STORAGE_KEY), null, 'explicit missing job ends tracking')
})
await test('finishing a deletion after changing player does not redirect the new selection', async () => {
  const pending = deferred(), navigations = []
  localStorage.setItem(PLAYER_DELETE_JOB_STORAGE_KEY, JSON.stringify({ jobId: 'delete', playerName: 'alice', redirectPlayer: 'carol' }))
  route = req => req.path === '/jobs/delete' ? pending.promise : null
  const select = name => navigations.push(name)
  await mount(<PlayerWorkspace playerName="alice" onSelectPlayer={select} />)
  await act(async () => root.render(<PlayerWorkspace playerName="bob" onSelectPlayer={select} />))
  pending.resolve(complete()); await flush()
  equal(navigations, [], 'old deletion cannot redirect a replacement selection')
  equal(localStorage.getItem(PLAYER_DELETE_JOB_STORAGE_KEY), null, 'confirmed deleted job released')
})
await test('Player Workspace aborts status request on unmount and late success cannot refresh', async () => {
  const pending = deferred()
  localStorage.setItem(UPDATE_JOB_STORAGE_KEY, JSON.stringify({ jobId: 'update', playerName: 'alice' }))
  route = req => req.path === '/jobs/update' ? pending.promise : null
  await mount(<PlayerWorkspace playerName="alice" />)
  const request = requests.find(req => req.path === '/jobs/update'), before = requests.length
  await unmount(); assert(request.signal?.aborted, 'status request aborted')
  pending.resolve(complete()); await flush()
  equal(requests.length, before, 'late response has no side effects')
  assert(localStorage.getItem(UPDATE_JOB_STORAGE_KEY), 'unmount preserves tracking')
})
for (const outcome of ['success', 'failure', 'cancelled']) await test(`Main Characters acknowledges queued work then handles ${outcome}`, async () => {
  let phase = 'queued'
  route = req => req.path === '/games/update' ? response({ job_id: 'main-update', message: 'Enqueued', player_name: 'alice' }, 202)
    : req.path === '/jobs/main-update' ? response(phase === 'queued' ? { status: 'queued' }
      : outcome === 'success' ? complete(true, { committed_games: 2, ingestion_run_id: 'run', fen_extraction: { status: 'queued', job_id: 'fen' }, message: 'Saved 2 games. Automatic FEN extraction queued.' })
      : complete(false, { type: outcome === 'cancelled' ? 'CancelledError' : 'RuntimeError', message: 'import failed' })) : null
  await mount(<MainCharacters />); await click('.games-mode-button'); const before = requests.filter(req => req.path.endsWith('/top')).length
  await click('.main-character-update-btn')
  equal(requests.filter(req => req.path.endsWith('/top')).length, before, '202 does not refresh data')
  assert(document.querySelector('.main-character-update-btn').disabled, 'queued work stays busy')
  await unmount(); await mount(<MainCharacters />); await click('.games-mode-button')
  assert(document.querySelector('.main-character-update-btn').disabled, 'restored job stays busy')
  const restored = requests.filter(req => req.path.endsWith('/top')).length
  phase = 'done'; await runDelay(1500)
  equal(requests.filter(req => req.path.endsWith('/top')).length, restored + (outcome === 'success' ? 1 : 0), 'only success refreshes games')
  const text = document.body.textContent
  if (outcome === 'success') assert(text.includes('FEN extraction queued'), 'FEN follow-up remains distinct')
  else assert(text.includes(outcome === 'cancelled' ? 'cancelled' : 'import failed'), 'failure shown')
})
await test('Main Characters preserves a late enqueue acknowledgment across navigation', async () => {
  const pending = deferred()
  route = req => req.path === '/games/update' ? pending.promise : null
  await mount(<MainCharacters />); await click('.games-mode-button'); await click('.main-character-update-btn')
  await unmount(); pending.resolve({ job_id: 'late-main', player_name: 'alice' }); await flush()
  equal(JSON.parse(localStorage.getItem(UPDATE_JOB_STORAGE_KEY)).jobId, 'late-main', 'acknowledged job preserved')
  await mount(<MainCharacters />); await click('.games-mode-button')
  assert(document.querySelector('.main-character-update-btn').disabled, 'restored acknowledged job busy')
})
await test('Main Characters addition waits for ingestion before refreshing counts', async () => {
  let phase = 'queued'
  route = req => req.path === '/games' ? response({ job_id: 'main-add', player_name: 'bob' }, 202)
    : req.path === '/jobs/main-add' ? response(phase === 'queued' ? { status: 'queued' }
      : complete(true, { committed_games: 5, message: 'Saved games. FEN extraction is already running.', fen_extraction: { status: 'already_active' } })) : null
  await mount(<MainCharacters />); await click('.games-mode-button')
  const button = [...document.querySelectorAll('button')].find(node => node.textContent.trim() === 'add player')
  await act(async () => button.click())
  const input = document.querySelector('.main-character-add-player-input')
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, 'bob')
    input.dispatchEvent(new Event('input', { bubbles: true }))
  })
  const before = requests.filter(req => req.path.endsWith('/time_controls')).length
  const go = [...document.querySelectorAll('button')].find(node => node.textContent.trim() === 'go')
  await act(async () => go.click())
  equal(requests.filter(req => req.path.endsWith('/time_controls')).length, before, 'enqueue cannot refresh counts')
  phase = 'done'; await runDelay(1500)
  equal(requests.filter(req => req.path.endsWith('/time_controls')).length, before + 1, 'completion refreshes counts')
  assert(document.body.textContent.includes('FEN extraction is already running'), 'separate follow-up shown')
})
await test('Main Characters retains tracking after errors and ignores outdated mode responses', async () => {
  const old = deferred(); let reads = 0
  localStorage.setItem(UPDATE_JOB_STORAGE_KEY, JSON.stringify({ jobId: 'main-update', playerName: 'alice' }))
  route = req => req.path === '/jobs/main-update' ? (++reads < 3 ? Promise.reject(new Error('offline')) : response(complete(true, { committed_games: 0, message: 'No new games.' })))
    : req.path.endsWith('/top') && req.query.includes('bullet') ? old.promise : null
  await mount(<MainCharacters />)
  await click('.games-mode-button'); await click('.games-mode-button:nth-child(2)')
  old.resolve({ players: [{ player_name: 'obsolete', n_games: 1 }] }); await flush()
  assert(!document.body.textContent.includes('obsolete'), 'old mode response rejected')
  await runDelay(3000); assert(localStorage.getItem(UPDATE_JOB_STORAGE_KEY), 'outage preserves main job')
  await runDelay(6000); assert(document.body.textContent.includes('all games ready'), 'zero count decides up-to-date state')
})
await test('Download recovery retries through the shared transport and does not replace a restored job', async () => {
  localStorage.setItem(UPDATE_JOB_STORAGE_KEY, JSON.stringify({ jobId: 'saved', playerName: 'alice' }))
  let calls = 0
  route = req => req.path === '/jobs/active' ? (++calls === 1 ? Promise.reject(new Error('offline'))
    : response({ jobs: [{ job_id: 'other', status: 'queued', info: { function: 'run_update_player_games_job' } }] })) : null
  await mount(<DownloadNewGames />); await runDelay(5000)
  equal(JSON.parse(localStorage.getItem(UPDATE_JOB_STORAGE_KEY)).jobId, 'saved', 'restored identity preserved')
})

for (const destination of ['main', 'player', 'download']) await test(`late acknowledgment reaches an already-mounted ${destination} page`, async () => {
  const pending = deferred(); let statusReads = 0
  route = req => req.path === '/games/update' ? pending.promise
    : req.path === '/jobs/late-observed' ? (++statusReads === 1 ? Promise.reject(new Error('offline')) : response(complete(true, { committed_games: 1, fen_extraction: { status: 'queued' } }))) : null
  await mount(<MainCharacters />); await click('.games-mode-button'); await click('.main-character-update-btn')
  await unmount()
  await mount(destination === 'main' ? <MainCharacters /> : destination === 'player' ? <PlayerWorkspace playerName="alice" /> : <DownloadNewGames />)
  if (destination === 'main') await click('.games-mode-button')
  const profileReads = requests.filter(req => req.path.endsWith('/hero')).length
  pending.resolve({ job_id: 'late-observed', player_name: 'alice' }); await flush()
  equal(statusReads, 1, 'already-mounted consumer polls the late acknowledgment')
  equal(JSON.parse(localStorage.getItem(UPDATE_JOB_STORAGE_KEY)).jobId, 'late-observed', 'outage retains persistent identity')
  if (destination === 'main') assert(document.querySelector('.main-character-update-btn').disabled, 'replacement busy after late acknowledgment')
  if (destination === 'download') assert(document.querySelector('.btn-primary').disabled, 'late update blocks overlapping download')
  await runDelay(3000)
  equal(statusReads, 2, 'polling retries normally')
  equal(localStorage.getItem(UPDATE_JOB_STORAGE_KEY), null, 'confirmed success clears shared identity')
  if (destination === 'player') equal(requests.filter(req => req.path.endsWith('/hero')).length, profileReads + 1, 'workspace refreshes matching player')
  await unmount()
  equal(timers.size, 0, 'unmounted consumers release polling timers')
})
for (const newerCompleted of [false, true]) await test(`old acknowledgment cannot replace a newer ${newerCompleted ? 'completed' : 'active'} request`, async () => {
  const older = deferred(); let enqueues = 0
  route = req => req.path === '/games/update' ? (++enqueues === 1 ? older.promise : response({ job_id: 'newer-request', player_name: 'alice' }, 202))
    : req.path === '/jobs/newer-request' && newerCompleted ? response(complete(true, { committed_games: 1 })) : null
  await mount(<MainCharacters />); await click('.games-mode-button'); await click('.main-character-update-btn')
  await unmount(); await mount(<MainCharacters />); await click('.games-mode-button'); await click('.main-character-update-btn')
  const before = localStorage.getItem(UPDATE_JOB_STORAGE_KEY)
  older.resolve({ job_id: 'older-request', player_name: 'alice' }); await flush()
  equal(localStorage.getItem(UPDATE_JOB_STORAGE_KEY), before, 'late old response cannot replace or revive newer tracking')
  assert(!requests.some(req => req.path === '/jobs/older-request'), 'obsolete acknowledgment cannot start polling')
})
await test('a storage write during mounting is observed without a lost subscription notification', async () => {
  function WriteDuringCommit() {
    useLayoutEffect(() => { storeJob(UPDATE_JOB_STORAGE_KEY, { jobId: 'during-commit', playerName: 'alice' }) }, [])
    return null
  }
  await mount(<><Tracked /><WriteDuringCommit /></>)
  equal(tracker.job?.jobId, 'during-commit', 'snapshot changes between render and subscription are adopted')
  assert(requests.some(req => req.path === '/jobs/during-commit'), 'adopted identity starts polling')
})
await test('unmounted storage consumers do not restart requests after shared writes', async () => {
  await mount(<Tracked />); await act(async () => tracker.track({ jobId: 'first', playerName: 'alice' }))
  await unmount(); const before = requests.length
  await act(async () => storeJob(UPDATE_JOB_STORAGE_KEY, { jobId: 'after-unmount', playerName: 'bob' }))
  equal(requests.length, before, 'unsubscribed component cannot restart polling')
  equal(timers.size, 0, 'unmounted component has no timers')
  await mount(<Tracked />)
  equal(tracker.job.jobId, 'after-unmount', 'new subscriber reads latest storage')
  assert(requests.some(req => req.path === '/jobs/after-unmount'), 'restored identity is polled')
})
await test('Main Characters shows incomplete archive failure without claiming games ready', async () => {
  route = req => req.path === '/games/update' ? response({ job_id: 'incomplete', player_name: 'alice' }, 202)
    : req.path === '/jobs/incomplete' ? response(complete(false, { type: 'RuntimeError', message: 'Incomplete archive download for alice: 1/2 archives downloaded; failed 2026-02: HTTP 503' })) : null
  await mount(<MainCharacters />); await click('.games-mode-button')
  const before = requests.filter(req => req.path.endsWith('/top')).length
  await click('.main-character-update-btn')
  equal(requests.filter(req => req.path.endsWith('/top')).length, before, 'incomplete ingestion cannot trigger successful refresh')
  assert(document.body.textContent.includes('Incomplete archive download'), 'failed coverage is visible')
  assert(!document.body.textContent.includes('all games ready'), 'failure cannot claim complete coverage')
  equal(document.querySelector('.main-character-update-btn').textContent.trim(), 'retry', 'failed attempt remains retryable')
  equal(localStorage.getItem(UPDATE_JOB_STORAGE_KEY), null, 'confirmed failure releases job tracking')
})

for (const source of ['workspace', 'download']) await test(`late ${source} enqueue acknowledgment is adopted by Main Characters`, async () => {
  const pending = deferred()
  route = req => req.path === (source === 'workspace' ? '/games/update' : '/games') ? pending.promise : null
  await mount(source === 'workspace' ? <PlayerWorkspace playerName="alice" /> : <DownloadNewGames />)
  if (source === 'workspace') await click('.players-update-button')
  else {
    const input = document.querySelector('.text-input')
    await act(async () => {
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set.call(input, 'alice')
      input.dispatchEvent(new Event('input', { bubbles: true }))
    })
    await click('.btn-primary')
  }
  await unmount(); await mount(<MainCharacters />); await click('.games-mode-button')
  pending.resolve({ job_id: `late-${source}`, player_name: 'alice' }); await flush()
  assert(requests.some(req => req.path === `/jobs/late-${source}`), 'replacement polls another producer acknowledgment')
  assert(document.querySelector('.main-character-update-btn').disabled, 'acknowledgment disables overlapping ingestion')
})
await test('newly adopted work stays busy after the preceding job completed', async () => {
  route = req => req.path === '/jobs/preceding' ? response(complete()) : null
  localStorage.setItem(UPDATE_JOB_STORAGE_KEY, JSON.stringify({ jobId: 'preceding', playerName: 'alice' }))
  await mount(<DownloadNewGames />)
  equal(localStorage.getItem(UPDATE_JOB_STORAGE_KEY), null, 'preceding work completed')
  await act(async () => storeJob(UPDATE_JOB_STORAGE_KEY, { jobId: 'replacement', playerName: 'bob' }))
  assert(document.querySelector('.btn-primary').disabled, 'previous terminal presentation cannot unblock a new job')
})

window.setTimeout = realTimeout; window.clearTimeout = realClear
results.finished = true
console.log(JSON.stringify(results))
