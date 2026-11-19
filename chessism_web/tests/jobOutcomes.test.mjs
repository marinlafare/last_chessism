import assert from 'node:assert/strict'
import { test } from 'node:test'
import { isTerminalJobStatus } from '../src/services/gameJobService.js'
import { watchGameJob } from '../src/services/gameJobPolling.js'
import { obsoletePositionStatus } from '../src/pages/positions/positionStatusFreshness.js'
const settle = () => new Promise(resolve => setImmediate(resolve))

test('neither complete ARQ status nor terminal telemetry proves a worker outcome', () => {
  for (const phase of ['complete', 'failed', 'unavailable', 'cancelled']) {
    for (const status of ['in_progress', 'complete']) {
      assert.equal(isTerminalJobStatus({ status, progress: { phase } }), false)
    }
  }
  for (const success of [true, false]) assert.equal(isTerminalJobStatus({ status: 'complete', result: { success } }), true)
})

test('confirmed failure cannot be revived by a late active response or contradictory telemetry', () => {
  const state = { jobId: 'one', status: { job_id: 'one', status: 'complete', result: { success: false } } }
  assert.equal(obsoletePositionStatus(state, { job_id: 'one', status: 'in_progress', progress: { phase: 'complete' } }), true)
  assert.equal(obsoletePositionStatus(state, { job_id: 'one', status: 'complete', result: { success: true } }), true)
})

test('repeated outages cap backoff and preserve the job until an authoritative result', async () => {
  const pending = new Map(), statuses = [], errors = []
  let id = 0, calls = 0
  const stop = watchGameJob({
    jobId: 'one', setTimer: (fn, ms) => { pending.set(++id, { fn, ms }); return id }, clearTimer: id => pending.delete(id),
    fetchStatus: async () => {
      calls++
      if (calls <= 7) throw new Error('offline')
      if (calls === 8) return { status: 'complete', progress: { phase: 'complete' } }
      return { status: 'complete', result: { success: false, result: { type: 'CancelledError' } } }
    },
    onStatus: value => statuses.push(value), onUnavailable: error => errors.push(error),
  })
  await settle()
  for (const delay of [3000, 6000, 12000, 15000, 15000, 15000, 15000, 1500]) {
    assert.equal(pending.size, 1)
    const [key, timer] = [...pending][0]
    assert.equal(timer.ms, delay)
    pending.delete(key)
    await timer.fn()
  }
  assert.equal(calls, 9)
  assert.equal(errors.length, 7)
  assert.equal(statuses.length, 2)
  assert.equal(pending.size, 0)
  stop()
})

test('structured ingestion counts and FEN follow-up determine copy without classifying prose', async () => {
  const { jobStatusMessage } = await import('../src/services/jobOutcome.js')
  assert.equal(jobStatusMessage({ ...{ status: 'complete' }, result: { success: true, result: {
    committed_games: 4, message: 'error failed no new games', fen_extraction: { status: 'queued' },
  } } }), '4 new games saved. FEN extraction queued.')
  assert.equal(jobStatusMessage({ status: 'complete', result: { success: false }, progress: { phase: 'complete', detail: 'Successfully saved!' } }), 'Job failed.')
})
