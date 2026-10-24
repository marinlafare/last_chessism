import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  getAnalysisButtonLabel, getAnalysisJobProgress, getAnalysisProcessView,
  getTrackedJobPhase, isTrackedJobActive,
} from '../src/pages/positions/positionPageSupport.js'

const queued = () => ({
  jobId: 'all-job', targetFens: 24000, startAnalyzed: 1_000_000,
  // Reproduce the misleading cached progress from the old database-wide fallback.
  progress: { analyzed: 5000, target: 24000, percent: 21, phase: 'analyzing' },
  status: {
    job_id: 'all-job', status: 'queued', progress: null,
    info: { function: 'run_analysis_job', kwargs: { total_fens_to_process: 24000 } },
  },
})

test('queued job ignores previously cached and database-wide progress', () => {
  const state = queued()
  assert.equal(getTrackedJobPhase(state), 'queued')
  assert.equal(getAnalysisButtonLabel(state), 'Queued')
  assert.equal(isTrackedJobActive(state), true) // Still disable duplicate submission.
  assert.deepEqual(getAnalysisJobProgress(state), {
    analyzed: 0, failed: 0, target: 24000, percent: 0,
    phase: 'queued', detail: 'Waiting to start.',
  })
  assert.equal(state.progress.analyzed, 5000) // Render normalization is non-mutating.
})

test('deferred and retried queue states override stale progress phases and counters', () => {
  const state = queued()
  state.status.status = 'deferred'
  state.status.progress = { phase: 'analyzing', processed: 5000, total: 24000 }
  assert.equal(getAnalysisButtonLabel(state), 'Queued')
  assert.equal(getAnalysisJobProgress(state).analyzed, 0)
  const view = getAnalysisProcessView(state.status)
  assert.equal(view.isQueued, true)
  assert.equal(view.processed, 0)
  assert.equal(view.percent, 0)
  assert.equal(view.progress.phase, 'queued')
})

test('running player and queued global jobs have independent progress', () => {
  const state = queued()
  const player = {
    jobId: 'player-job',
    status: { status: 'in_progress', progress: {
      total: 46321, processed: 5000, phase: 'cooling', detail: '120 seconds', failed: 0,
    } },
  }
  assert.equal(getAnalysisJobProgress(player).analyzed, 5000)
  assert.equal(getAnalysisButtonLabel(player), 'Cooling off')
  assert.equal(getAnalysisJobProgress(state).analyzed, 0)
  state.status.status = 'in_progress'
  state.status.progress = { total: 24000, processed: 123, phase: 'analyzing' }
  assert.equal(getAnalysisJobProgress(state).analyzed, 123)
  assert.equal(getAnalysisButtonLabel(state), 'Analyzing')
  assert.equal(getAnalysisProcessView(state.status).processed, 123)
})

test('missing server progress never falls back to the old cached count', () => {
  const state = queued()
  state.status.status = 'in_progress'
  assert.equal(getAnalysisJobProgress(state).analyzed, 0)
  assert.equal(getAnalysisJobProgress(state).target, 24000)
  state.progress.phase = 'queued'
  assert.equal(getAnalysisButtonLabel(state), 'Analyzing')
})

test('targets are recovered for player, loop, and complete-game requests', () => {
  for (const [kwargs, target] of [
    [{ total_fens_to_process: 2000 }, 2000],
    [{ positions_per_run: 5000, runs: 4 }, 20000],
    [{ planned_fens: 46321 }, 46321],
  ]) {
    assert.equal(getAnalysisJobProgress({ status: { status: 'queued', info: { kwargs } } }).target, target)
  }
  assert.equal(getAnalysisJobProgress({ targetPlayerFens: 3000 }).target, 3000)
  assert.equal(getAnalysisJobProgress({}).percent, 0)
})

test('idle, submitting, and completed controls retain their labels', () => {
  assert.equal(getAnalysisButtonLabel({}), 'Analyze')
  assert.equal(getAnalysisButtonLabel({ loading: true }), 'Queueing')
  assert.equal(getAnalysisButtonLabel({ jobId: 'finished', status: { status: 'complete' } }), 'Analyze')
})
