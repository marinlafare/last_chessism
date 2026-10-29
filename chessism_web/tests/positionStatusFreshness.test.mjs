import assert from 'node:assert/strict'
import { test } from 'node:test'
import { obsoletePositionStatus } from '../src/pages/positions/positionStatusFreshness.js'

const snapshot = (phase, updated_at, job_id = 'local-job') => ({
  job_id, status: phase === 'complete' ? 'complete' : 'in_progress',
  progress: { phase, updated_at },
})
test('discovery cannot revive a terminal job or replace a newer local checkpoint', () => {
  const state = { jobId: 'local-job', status: snapshot('analyzing', 200) }
  assert.equal(obsoletePositionStatus(state, snapshot('analyzing', 100)), true)
  assert.equal(obsoletePositionStatus(state, snapshot('analyzing', 300)), false)
  assert.equal(obsoletePositionStatus({ ...state, status: snapshot('complete', 200) }, snapshot('analyzing', 300)), true)
  assert.equal(obsoletePositionStatus(state, snapshot('complete', 300)), false)
  assert.equal(obsoletePositionStatus({ ...state, status: snapshot('failed', 200) }, {
    ...snapshot('analyzing', 300), status: 'deferred',
  }), false) // A genuine recovery may reuse a failed job ID.
})
test('freshness guards respect job identity, missing timestamps and actual completion', () => {
  const state = { jobId: 'local-job', status: snapshot('complete', 200) }
  assert.equal(obsoletePositionStatus(state, snapshot('analyzing', 100, 'another-job')), false)
  assert.equal(obsoletePositionStatus({}, snapshot('analyzing', 100)), false)
  assert.equal(obsoletePositionStatus(state, snapshot('complete', 300)), false)
  assert.equal(obsoletePositionStatus({ ...state, status: snapshot('analyzing', null) }, snapshot('analyzing', null)), false)
})
