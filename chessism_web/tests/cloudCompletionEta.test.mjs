import test from 'node:test'
import assert from 'node:assert/strict'
import { comparableCloudJobs, completionProfile, estimateCloudCompletion } from '../src/pages/system-status/cloudCompletionEta.js'
import { recordCloudEta } from '../src/pages/system-status/cloudEta.js'

const epoch = Date.parse('2026-10-10T12:00:00Z')
const at = seconds => new Date(epoch + seconds * 1000).toISOString()
const job = { id: 'current', status: 'running', target: 2000, imported: 1000,
  selection: { backend: 'batch_spot', n_vms: 2 }, sequence: { automatic: true, phase: 'running' },
  runs: [{ id: 'root', status: 'running' }] }
const prior = { ...job, id: 'prior', status: 'complete', target: 2000, created_at: at(-10000),
  runs: [{ id: 'prior-root', status: 'complete' }] }
const phase = (name, seconds, run = 'prior-root') => ({ phase: name, run_id: run, started_at: at(seconds), outcome: 'complete' })
const usage = { phases: [phase('finalizing', 0), phase('cleaning', 30), phase('log_planning', 330),
  phase('log_cleaning', 350), phase('refreshing', 360),
  { ...phase('global_summaries', 500, null), finished_at: at(600) }] }
const context = { jobId: job.id, observedAt: epoch + 1000 * 1000, current: { phases: [phase('running', 0, 'root')] },
  completed: [{ job: prior, usage }] }
const estimate = (current = job, ctx = context, history = null, options = {}) => estimateCloudCompletion(history, current,
  { now: epoch + 1000 * 1000, available: true, lastSuccessAt: epoch + 1000 * 1000, ...options }, ctx)

test('selects bounded, completed, similar-size history from the same backend and execution mode', () => {
  const candidates = [prior, { ...prior, id: 'small', target: 10 }, { ...prior, id: 'failed', status: 'failed' },
    { ...prior, id: 'run', selection: { backend: 'cloud_run' } }, { ...prior, id: 'legacy', sequence: null },
    ...[1000, 2500, 7000].map(target => ({ ...prior, id: `size-${target}`, target }))]
  assert.deepEqual(comparableCloudJobs(job, candidates).map(row => row.id), ['prior', 'size-2500', 'size-1000'])
})
test('profile measures wall time without summing parallel phases or earlier tasks waiting for later ones', () => {
  const multi = { ...prior, runs: [...prior.runs, { id: 'other-root' }] }
  const overlapping = { phases: [...usage.phases, phase('finalizing', -1000, 'other-root'),
    phase('cleaning', 32, 'other-root'), phase('finalizing', 29, 'child-vm')] }
  assert.deepEqual(completionProfile(multi, overlapping), {
    finalizing: 30, cleaning: 300, log_planning: 20, log_cleaning: 10, refreshing: 240,
  })
  assert.equal(completionProfile(prior, { phases: usage.phases.slice(1) }), null)
  assert.equal(completionProfile(prior, { phases: [...usage.phases, { ...phase('cleaning', 20), outcome: 'failed' }] }), null)
})
test('saved start time provides a whole-process ETA immediately after opening the page', () => {
  assert.equal(estimate().seconds, 1600) // 1,000 seconds analysis/import + 600 seconds finalization.
  assert.equal(estimate().referenceJob, 'prior')
})
test('fresh measured import rate replaces the saved baseline and still includes all finalization', () => {
  let history = null
  for (const [seconds, imported] of [[940, 400], [970, 700], [1000, 1000]]) {
    history = recordCloudEta(history, { ...job, imported }, epoch + seconds * 1000)
  }
  assert.equal(estimate(job, context, history).seconds, 700)
})
test('scales volume-dependent tail work, not fixed log-deletion overhead', () => {
  assert.equal(estimate({ ...job, target: 4000, imported: 4000 }).seconds, 1170)
})
test('100% imported does not mean done; cleaning counts only its remaining time plus later phases', () => {
  assert.equal(estimate({ ...job, imported: 2000 }).seconds, 600)
  const cleaning = { ...job, imported: 2000, sequence: { automatic: true, phase: 'cleaning' } }
  const ctx = { ...context, current: { phases: [phase('cleaning', 900, 'root')] } }
  assert.equal(estimate(cleaning, ctx).seconds, 470)
  const overdue = { ...ctx, current: { phases: [phase('cleaning', 100, 'root')] } }
  assert.equal(estimate(cleaning, overdue).label, 'Recalculating…')
  assert.equal(estimate(cleaning).label, 'Updating estimate…')
  assert.equal(estimate({ ...job, status: 'complete', runs: [{ status: 'complete' }] }).label, 'Done')
})
test('unknown or stale history never silently substitutes an analysis-only ETA', () => {
  assert.equal(estimate(job, null).label, 'Loading history…')
  assert.equal(estimate(job, { ...context, completed: [] }).label, 'Not enough history')
  assert.equal(estimate(job, { jobId: job.id, error: new Error('Failed') }).label, 'Unavailable')
  assert.equal(estimate(job, { ...context, observedAt: epoch }).label, 'Unavailable')
  assert.equal(estimate(job, context, null, { available: false }).label, 'Unavailable')
  assert.equal(estimate(job, context, null, { lastSuccessAt: epoch }).label, 'Unavailable')
  assert.equal(estimate({ ...job, status: 'paused' }).label, 'Paused')
})
test('missing current start or no completed FENs cannot give a reliable analysis rate', () => {
  assert.equal(estimate(job, { ...context, current: { phases: [] } }).label, 'Calculating…')
  assert.equal(estimate({ ...job, imported: 0 }).label, 'Calculating…')
})
test('Cloud Run has its own completion history and no countdown reaches zero prematurely', () => {
  const run = { ...job, imported: 2000, selection: { backend: 'cloud_run', n_cpus: 16 }, sequence: null,
    runs: [{ id: 'root', status: 'refreshing' }] }
  const ctx = { ...context, current: { phases: [phase('refreshing', 760, 'root')] } }
  const eta = estimate(run, ctx)
  assert.equal(eta.seconds, 1)
  assert.equal(eta.label, '~<1 min')
  assert.deepEqual(comparableCloudJobs(run, [prior]), [])
})
