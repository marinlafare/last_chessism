import test from 'node:test'
import assert from 'node:assert/strict'
import {
  cloudJobDone, cloudJobProgress, cloudJobIndicator, cloudJobDisplayState, reconcileCloudJobDisplay,
  CLOUD_DONE_VISIBLE_MS, CLOUD_DONE_FADE_MS,
} from '../src/pages/positions/cloudJobProgress.js'

const jobAt = (phase, status = 'running') => ({
  id: 'job', status, imported: 1000, target: 1000,
  runs: [{ status: phase, cloud_state: 'SUCCEEDED' }],
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

test('old completed and cancelled displays are hidden, without hiding failed jobs', () => {
  const now = 100000
  for (const status of ['complete', 'limit_reached', 'cancelled', 'failed', 'paused', 'waiting', 'running']) {
    const job = jobAt('complete', status)
    const entries = reconcileCloudJobDisplay({}, [job], now)
    assert.equal(cloudJobDisplayState(job, entries.job, now),
      ['complete', 'limit_reached', 'cancelled'].includes(status) ? 'hidden' : 'visible', status)
  }
  assert.equal(cloudJobDisplayState(jobAt('cleaning', 'complete'), undefined, now), 'visible')
})

test('completion stays visible for ten seconds, fades, and cannot be revived by polls', () => {
  let job = jobAt('cleaning')
  let entries = reconcileCloudJobDisplay({}, [job], 1000)
  job = jobAt('complete', 'complete')
  assert.equal(cloudJobDisplayState(job, entries.job, 2000), 'visible')
  entries = reconcileCloudJobDisplay(entries, [job], 2000)
  entries = reconcileCloudJobDisplay(entries, [job], 5000)
  assert.equal(entries.job.endedAt, 2000)
  assert.equal(cloudJobDisplayState(job, entries.job, 2000 + CLOUD_DONE_VISIBLE_MS - 1), 'visible')
  assert.equal(cloudJobDisplayState(job, entries.job, 2000 + CLOUD_DONE_VISIBLE_MS), 'fading')
  assert.equal(cloudJobDisplayState(job, entries.job, 2000 + CLOUD_DONE_VISIBLE_MS + CLOUD_DONE_FADE_MS), 'hidden')
  entries = reconcileCloudJobDisplay(entries, [job], 20000)
  assert.equal(cloudJobDisplayState(job, entries.job, 20000), 'hidden')
  assert.equal(cloudJobDisplayState(job, reconcileCloudJobDisplay({}, [job], 20000).job, 20000), 'hidden')
})

test('a reopened job becomes visible and a later completion receives a new countdown', () => {
  let job = jobAt('complete', 'complete')
  let entries = reconcileCloudJobDisplay({}, [job], 1000)
  job = jobAt('cleaning', 'paused')
  assert.equal(cloudJobDisplayState(job, entries.job, 2000), 'visible')
  entries = reconcileCloudJobDisplay(entries, [job], 2000)
  assert.equal(entries.job.endedAt, null)
  job = jobAt('complete', 'complete')
  entries = reconcileCloudJobDisplay(entries, [job], 3000)
  assert.equal(entries.job.endedAt, 3000)
})
