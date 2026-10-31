import test from 'node:test'
import assert from 'node:assert/strict'
import {
  cloudJobDone, cloudJobProgress, cloudJobIndicator,
  cloudActiveRun, cloudTaskLabel, cloudStepProgress, CLOUD_JOB_STEP_IDS,
} from '../src/pages/positions/cloudJobProgress.js'
import { trackProgress, showProgress, dismissProgress } from '../src/pages/positions/cloudProgressReview.js'

const jobAt = (phase, status = 'running') => ({
  id: 'job', status, imported: 1000, target: 1000,
  runs: [{ status: phase, cloud_state: 'SUCCEEDED' }],
})

const sequenceAtSix = () => ({
  id: 'sequence', status: 'running', target: 4000000, imported: 2760000,
  sequence: { automatic: true, phase: 'running', times: 8, cloud_completed: 5, cloud_status: 'RUNNING' },
  runs: Array.from({ length: 8 }, (_, index) => ({
    id: `cycle-${index}`, positions: 500000,
    status: index < 5 ? 'finalizing' : index === 5 ? 'running' : 'cloud_queued',
    cloud_state: index < 5 ? 'SUCCEEDED' : index === 5 ? 'RUNNING' : 'PENDING',
    imported: index < 5 ? 500000 : index === 5 ? 260000 : 0,
  })),
})

test('every workflow step has a stable independently editable progress identity', () => {
  const steps = cloudJobProgress(jobAt('preparing')).steps
  assert.deepEqual(steps.map(step => step.id), CLOUD_JOB_STEP_IDS)
  assert.equal(new Set(CLOUD_JOB_STEP_IDS).size, steps.length)
  const stop = cloudJobProgress({ ...jobAt('running'), stop_cleanup: { phase: 'cleaning' } }).steps
  assert.ok(stop.every(step => step.id.startsWith('stop-') && !CLOUD_JOB_STEP_IDS.includes(step.id)))
  assert.equal(new Set(stop.map(step => step.id)).size, stop.length)
})

test('cleanup shows acknowledged files and keeps image/log completion separate', () => {
  const job = { ...jobAt('cleaning'), cleanup_progress: { phase: 'objects', total: 10533, removed: 8400 } }
  const step = cloudJobProgress(job).steps[7]
  assert.equal(cloudStepProgress(job, step).value, 8400 / 10533 * 100)
  assert.match(cloudStepProgress(job, step).detail, /8,400 \/ 10,533 files removed · 79%/)
  job.cleanup_progress.removed = 10533
  assert.equal(cloudStepProgress(job, step).value, 100)
  assert.equal(cloudJobDone(job), false)
  job.cleanup_progress.phase = 'image'
  assert.deepEqual(cloudStepProgress(job, step), { value: null, detail: 'Removing worker image', showDetail: true })
  job.cleanup_progress.phase = 'logs'
  assert.equal(cloudStepProgress(job, step).detail, 'Cleaning up eligible logs')
  job.stop_cleanup = { phase: 'cleaning' }
  job.cleanup_progress.phase = 'objects'
  assert.equal(cloudStepProgress(job, cloudJobProgress(job).steps[4]).value, 100)
  job.cleanup_progress.removed = 10534
  assert.equal(cloudStepProgress(job, step).value, null)
})

test('reservation progress uses only durable counters, including zero and a partial final selection', () => {
  const job = { status: 'running', runs: [], sequence: { automatic: true, phase: 'selecting', requested: 6000000, reserved: 0 } }
  const step = cloudJobProgress(job).steps[0]
  assert.equal(cloudStepProgress(job, step).value, 0)
  job.sequence.reserved = 50000
  assert.equal(cloudStepProgress(job, step).value, 50000 / 6000000 * 100)
  assert.match(cloudStepProgress(job, step).detail, /FENs reserved/)
  job.sequence.reserved = 5999999
  assert.match(cloudStepProgress(job, step).detail, /99%$/)
  job.sequence.phase = 'preparing'
  job.sequence.reserved = 3000000
  assert.equal(cloudStepProgress(job, cloudJobProgress(job).steps[0]).value, 100)
})

test('unknown preparation progress is indeterminate, never a fabricated percentage', () => {
  for (const phase of ['preparing', 'publishing', 'uploading', 'submitting', 'finalizing', 'cleaning', 'refreshing']) {
    const job = jobAt(phase)
    const step = cloudJobProgress(job).steps.find(step => step.state === 'current')
    assert.equal(cloudStepProgress(job, step).value, null, phase)
    assert.match(cloudStepProgress(job, step).detail, /percentage unavailable/)
  }
  const job = { status: 'queued', runs: [] }
  assert.equal(cloudStepProgress(job, cloudJobProgress(job).steps[0]).value, null)
})

test('Batch log phases override the last image progress label', () => {
  for (const phase of ['log_planning', 'log_cleaning']) {
    const job = { ...jobAt(phase), cleanup_progress: { phase: 'image', total: 100, removed: 100 } }
    const result = cloudStepProgress(job, cloudJobProgress(job).steps[7])
    assert.equal(result.value, null)
    assert.equal(result.showDetail, true)
    assert.match(result.detail, /logs/)
    assert.doesNotMatch(result.detail, /image/)
  }
})

test('analysis progress distinguishes concurrent sequence totals from legacy cycle counts', () => {
  const job = sequenceAtSix()
  let step = cloudJobProgress(job).steps[5]
  assert.equal(cloudStepProgress(job, step).value, 69)
  job.sequence.automatic = false
  job.runs[0].status = 'complete'
  job.runs[1] = { status: 'running', positions: 500000, imported: 100000 }
  step = cloudJobProgress(job).steps[5]
  assert.equal(cloudStepProgress(job, step).value, 20)
  const single = { ...jobAt('running'), runs: [{ status: 'running', positions: 2000, imported: 500 }] }
  assert.equal(cloudStepProgress(single, step).value, 25)
})

test('completed and pending meters are fixed; paused and malformed counters cannot fake activity', () => {
  const job = jobAt('publishing', 'paused')
  const steps = cloudJobProgress(job).steps
  assert.equal(cloudStepProgress(job, steps[0]).value, 100)
  assert.equal(cloudStepProgress(job, steps[2]).value, null)
  assert.equal(cloudStepProgress(job, steps[2]).detail, 'Needs attention')
  assert.equal(cloudStepProgress(job, steps[3]).value, 0)
  const step = { id: 'analyze', state: 'current' }
  for (const imported of [null, undefined, NaN, Infinity, -1]) {
    assert.equal(cloudStepProgress({ target: 100, imported }, step).value, null)
  }
  assert.equal(cloudStepProgress({ target: 100, imported: 101 }, step).value, 100)
  assert.equal(cloudStepProgress({ target: 0, imported: 0 }, step).value, null)
  const done = cloudJobProgress(jobAt('complete', 'complete')).steps
  assert.ok(done.every(step => cloudStepProgress(job, step).value === 100))
})

test('task six is selected ahead of five tasks awaiting shared cleanup', () => {
  const job = sequenceAtSix()
  assert.equal(cloudActiveRun(job), job.runs[5])
  assert.equal(cloudTaskLabel(job, cloudActiveRun(job)), 'Cloud processing: task 6 of 8 — RUNNING')
  assert.equal(cloudActiveRun(job).imported, 260000)
  assert.equal(cloudJobDone(job), false)
})

test('cloud compute takes precedence over earlier local import catch-up', () => {
  const job = sequenceAtSix()
  Object.assign(job.runs[0], { status: 'running', imported: 100000 })
  assert.equal(cloudActiveRun(job), job.runs[5])
  for (const state of ['QUEUED', 'SCHEDULED', 'RETRYING', 'RECOVERING']) {
    job.runs[5].cloud_state = state
    assert.equal(cloudActiveRun(job), job.runs[5])
    assert.equal(cloudTaskLabel(job, cloudActiveRun(job)), `Cloud processing: task 6 of 8 — ${state}`)
  }
})

test('the next unstarted task is explicitly waiting, not running or the previous finished task', () => {
  const job = sequenceAtSix()
  Object.assign(job.runs[5], { status: 'finalizing', cloud_state: 'SUCCEEDED', imported: 500000 })
  job.sequence.cloud_completed = 6
  assert.equal(cloudActiveRun(job), job.runs[6])
  assert.equal(cloudTaskLabel(job, cloudActiveRun(job)), 'Next cloud task: task 7 of 8 — waiting to start')
})

test('cloud success shows local catch-up and verification without calling them cloud execution', () => {
  const job = sequenceAtSix()
  job.sequence.cloud_status = 'SUCCEEDED'
  job.sequence.cloud_completed = 8
  job.runs.forEach(run => Object.assign(run, { status: 'finalizing', cloud_state: 'SUCCEEDED', imported: 500000 }))
  Object.assign(job.runs[2], { status: 'running', imported: 100000 })
  assert.equal(cloudActiveRun(job), job.runs[2])
  assert.equal(cloudTaskLabel(job, cloudActiveRun(job)), 'Local import: task 3 of 8 — cloud analysis finished')
  job.runs[2].imported = 500000
  assert.equal(cloudTaskLabel(job, cloudActiveRun(job)), 'Local verification: task 3 of 8 — cloud analysis finished')
  job.runs[2].status = 'finalizing'
  assert.equal(cloudActiveRun(job), undefined)
  assert.equal(cloudJobDone(job), false)
})

test('failed or stopped cloud sequences do not suggest future pending tasks will start', () => {
  const job = sequenceAtSix()
  job.status = 'failed'
  job.sequence.cloud_status = 'FAILED'
  job.runs[5].cloud_state = 'FAILED'
  assert.equal(cloudActiveRun(job), job.runs[5])
  assert.equal(cloudTaskLabel(job, cloudActiveRun(job)), 'Cloud task 6 of 8 — FAILED')
  Object.assign(job.runs[5], { cloud_state: 'SUCCEEDED', status: 'finalizing' })
  job.sequence.cloud_status = 'STOPPED'
  assert.equal(cloudActiveRun(job), undefined)
})

test('shared cleanup and final refresh never show an old task as the active cloud task', () => {
  const job = sequenceAtSix()
  for (const phase of ['cleanup_planning', 'cleaning', 'log_planning', 'log_cleaning', 'refreshing', 'complete']) {
    job.sequence.phase = phase
    assert.equal(cloudActiveRun(job), undefined, phase)
    assert.equal(cloudJobDone(job), false, phase)
  }
})

test('single jobs and legacy sequences keep their existing active-run behavior', () => {
  const job = jobAt('finalizing')
  job.runs.push({ status: 'running' })
  assert.equal(cloudActiveRun(job), job.runs[0])
  job.sequence = { automatic: false }
  assert.equal(cloudActiveRun(job), job.runs[0])
  job.runs[0].status = 'complete'
  assert.equal(cloudActiveRun(job), job.runs[1])
  assert.equal(cloudActiveRun({}), undefined)
})

test('automatic sequence progress does not stop at the first imported cycle', () => {
  const job = {...jobAt('finalizing'), sequence:{automatic:true,phase:'running'}}
  assert.equal(cloudJobProgress(job).steps[5].state,'current')
  job.sequence.phase='cleaning'
  assert.equal(cloudJobProgress(job).steps[7].state,'current')
  assert.equal(cloudJobDone(job),false)
})

test('indicator stays orange through startup, analysis, import, cleanup and final DB refresh', () => {
  assert.equal(cloudJobIndicator({ status: 'queued', runs: [] }).state, 'running')
  for (const phase of ['preparing', 'publishing', 'uploading', 'run_creating', 'run_start',
    'run_discover', 'running', 'finalizing', 'cleanup_planning', 'cleaning', 'log_cleaning', 'refreshing', 'complete']) {
    const indicator = cloudJobIndicator(jobAt(phase))
    assert.equal(indicator.state, 'running', phase)
    assert.match(indicator.label, /^In progress/)
  }
})

test('indicator is green only after successful cleanup; interrupted work never glows as running', () => {
  assert.deepEqual(cloudJobIndicator(jobAt('complete', 'complete')), {
    state: 'success', label: 'Succeeded — analysis, import and cleanup complete',
  })
  assert.match(cloudJobIndicator(jobAt('complete', 'limit_reached')).label, /available FENs/)
  for (const status of ['paused', 'waiting']) {
    assert.equal(cloudJobIndicator(jobAt('cleaning', status)).state, 'attention')
  }
  assert.equal(cloudJobIndicator(jobAt('failed', 'failed')).state, 'error')
  assert.equal(cloudJobIndicator(jobAt('failed')).state, 'error')
  assert.equal(cloudJobIndicator(jobAt('running', 'cancelled')).state, 'idle')
  assert.equal(cloudJobIndicator(jobAt('cleaning', 'complete')).state, 'idle')
})

test('100% imported and cloud success never bypass finalization or cleanup', () => {
  for (const phase of ['running', 'finalizing', 'cleanup_planning', 'cleaning', 'log_planning', 'log_cleaning', 'refreshing', 'complete']) {
    const job = jobAt(phase)
    assert.equal(cloudJobDone(job), false)
    assert.equal(cloudJobProgress(job).steps.at(-1).state, 'pending')
    assert.notEqual(cloudJobProgress(job).label, 'Done')
  }
  assert.equal(cloudJobProgress(jobAt('complete')).label, 'Refresh database totals')
  assert.equal(cloudJobDone(jobAt('cleaning', 'complete')), false)
  assert.equal(cloudJobDone({ status: 'complete' }), false)
  const done = cloudJobProgress(jobAt('complete', 'complete'))
  assert.equal(done.label, 'Done')
  assert.ok(done.steps.every((step) => step.state === 'complete'))
})

test('cloud success and fully imported results show verification while its proof is being saved', () => {
  const job = { ...jobAt('running'), target: 3760444, imported: 3760444,
    runs: [{ status: 'running', cloud_state: 'SUCCEEDED', positions: 3760444, imported: 3760444 }] }
  const progress = cloudJobProgress(job)
  assert.equal(progress.label, 'Verify imported results')
  assert.equal(progress.steps[5].state, 'complete')
  assert.equal(progress.steps[6].state, 'current')
  assert.equal(cloudStepProgress(job, progress.steps[6]).value, null)
  assert.equal(progress.steps[7].state, 'pending')
  assert.equal(progress.steps[9].state, 'pending')
  assert.equal(progress.done, false)
  assert.match(cloudJobIndicator(job).label, /Verify imported results/)
  job.status = 'paused'
  assert.equal(cloudJobProgress(job).steps[6].state, 'attention')
})

test('import count alone and cloud success alone do not advance to verification', () => {
  const job = { ...jobAt('running'), runs: [{ status: 'running', cloud_state: 'RUNNING', positions: 1000, imported: 1000 }] }
  for (const state of ['RUNNING', 'FAILED', 'CANCELLED', undefined]) {
    job.runs[0].cloud_state = state
    assert.equal(cloudJobProgress(job).steps[5].state, 'current')
  }
  job.runs[0].cloud_state = 'SUCCEEDED'
  for (const imported of [999, 1001, null, undefined, NaN]) {
    job.runs[0].imported = imported
    assert.equal(cloudJobProgress(job).steps[5].state, 'current')
  }
})

test('automatic sequence moves to verification only when every selected cycle is ready', () => {
  const job = sequenceAtSix()
  job.sequence.cloud_status = 'SUCCEEDED'
  job.runs.forEach(run => Object.assign(run, { status: 'running', cloud_state: 'SUCCEEDED', imported: run.positions }))
  assert.equal(cloudJobProgress(job).steps[6].state, 'current')
  assert.equal(cloudJobProgress(job).steps[7].state, 'pending')
  job.runs.at(-1).imported -= 1
  assert.equal(cloudJobProgress(job).steps[5].state, 'current')
})

test('all durable phases map to a visible workflow step', () => {
  const phases = {
    preparing: 1, publishing: 2, uploading: 3,
    submitting: 4, run_creating: 4, run_start: 4, run_discover: 4,
    running: 5, finalizing: 6, cleanup_planning: 7, cleaning: 7, log_planning: 7, log_cleaning: 7,
    refreshing: 8, complete: 8,
  }
  for (const [phase, expected] of Object.entries(phases)) {
    const progress = cloudJobProgress(jobAt(phase))
    assert.equal(progress.steps.findIndex((step) => step.state === 'current'), expected, phase)
  }
  assert.equal(cloudJobProgress({ status: 'queued', runs: [] }).steps[0].state, 'current')
})

test('partial completion is explicit and failed or paused work is not marked done', () => {
  assert.equal(cloudJobProgress(jobAt('complete', 'limit_reached')).label, 'Done — available FENs only')
  for (const status of ['failed', 'paused', 'waiting', 'cancelled']) {
    const progress = cloudJobProgress(jobAt('cleaning', status))
    assert.equal(progress.done, false)
    assert.equal(progress.steps[7].state, 'attention')
    assert.equal(progress.steps.at(-1).state, 'pending')
  }
})

test('finished progress stays visible until explicitly reviewed, including after reload', () => {
  let job = jobAt('cleaning')
  let review = trackProgress({ since: 1000, initialized: false, tracked: [], dismissed: [] }, [job])
  assert.equal(dismissProgress(review, [job], job.id), review)
  job = jobAt('complete', 'complete')
  review = trackProgress(review, [job])
  assert.equal(showProgress(job, review), true)
  assert.equal(showProgress(job, JSON.parse(JSON.stringify(review))), true)
  review = dismissProgress(review, [job], job.id)
  assert.equal(showProgress(job, trackProgress(review, [job])), false)
  assert.equal(showProgress(jobAt('cleaning', 'paused'), review), true)
})

test('cancellation has its own stages and is never analysis success', () => {
  let job = { ...jobAt('running'), stop_cleanup: { phase: 'importing' } }
  assert.equal(cloudJobProgress(job).label, 'Save completed results')
  assert.equal(cloudJobProgress(job).steps[2].state, 'current')
  job = { ...job, stop_cleanup: { phase: 'settling' } }
  assert.equal(cloudJobProgress(job).label, 'Wait for late uploads')
  assert.equal(cloudJobProgress(job).steps[1].state, 'current')
  assert.equal(cloudJobProgress(job).steps[3].state, 'pending')
  job = { ...job, status: 'cancelled', stop_cleanup: { phase: 'complete' } }
  assert.equal(cloudJobDone(job), false)
  assert.equal(cloudJobProgress(job).label, 'Cancelled — cleaned')
  assert.equal(cloudJobIndicator(job).state, 'idle')
})
