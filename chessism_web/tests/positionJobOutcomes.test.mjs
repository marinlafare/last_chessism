import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  getAnalysisJobProgress, getTrackedJobPhase, isTrackedJobActive, isTrackedJobComplete, loadStoredJobState,
} from '../src/pages/positions/positionPageSupport.js'
import { obsoletePositionStatus } from '../src/pages/positions/positionStatusFreshness.js'

const oldComplete = {
  phase: 'complete', processed: 10, total: 10, detail: 'Previous attempt completed.',
  updated_at: 200, attempt_id: 'previous-attempt',
}
const job = (status, progress = oldComplete, result = null) => ({
  jobId: 'analysis-job',
  status: { job_id: 'analysis-job', status, progress, result },
})
const failed = { success: false, result: { type: 'RuntimeError', message: 'Database unavailable' } }

for (const phase of ['queued', 'deferred', 'in_progress']) {
  test(`ARQ ${phase} remains active despite completed progress from a failed reset`, () => {
    const state = job(phase)
    const before = structuredClone(state)
    assert.equal(getTrackedJobPhase(state), phase)
    assert.equal(isTrackedJobActive(state), true)
    assert.equal(isTrackedJobComplete(state), false)
    assert.deepEqual(state, before)
  })
}

for (const progress of [oldComplete, null]) {
  test(`unsuccessful ARQ result overrides ${progress ? 'completed' : 'missing'} progress`, () => {
    const state = job('complete', progress, failed)
    assert.equal(getTrackedJobPhase(state), 'failed')
    assert.equal(isTrackedJobActive(state), false)
    assert.equal(isTrackedJobComplete(state), false)
    assert.equal(getAnalysisJobProgress(state).detail, 'Database unavailable')
  })
}

test('ARQ result availability alone never proves success or ends polling', () => {
  for (const result of [null, {}, { success: 'true' }, { success: 'false' }, { success: 1 }]) {
    const state = job('complete', oldComplete, result)
    assert.equal(getTrackedJobPhase(state), 'in_progress')
    assert.equal(isTrackedJobActive(state), true)
    assert.equal(isTrackedJobComplete(state), false)
    assert.equal(getAnalysisJobProgress(state).detail, 'Waiting for job outcome.')
  }
})

test('confirmed success completes with valid, missing or stale failed progress', () => {
  for (const progress of [oldComplete, null, { phase: 'failed', detail: 'Old failure' }]) {
    const state = job('complete', progress, { success: true, result: { processed: 10 } })
    assert.equal(getTrackedJobPhase(state), 'complete')
    assert.equal(isTrackedJobActive(state), false)
    assert.equal(isTrackedJobComplete(state), true)
    assert.notEqual(getAnalysisJobProgress(state).detail, 'Old failure')
  }
})

test('confirmed result wins if the separate queue-status read still says running', () => {
  for (const success of [true, false]) {
    const state = job('in_progress', null, { success })
    assert.equal(getTrackedJobPhase(state), success ? 'complete' : 'failed')
    assert.equal(isTrackedJobActive(state), false)
    assert.equal(isTrackedJobComplete(state), success)
  }
})

test('cached UI completion without an authoritative response stays unresolved', () => {
  const state = { jobId: 'analysis-job', progress: { phase: 'complete' }, completedAt: 123 }
  assert.equal(isTrackedJobActive(state), true)
  assert.equal(isTrackedJobComplete(state), false)
})

test('restored unresolved or failed jobs discard stale completion and fading flags', () => {
  const savedWindow = globalThis.window
  try {
    for (const status of [job('in_progress').status, job('complete', null, failed).status]) {
      const stored = { global: { jobId: 'analysis-job', status, completedAt: 123, fading: true } }
      const bytes = JSON.stringify(stored)
      globalThis.window = { localStorage: { getItem: () => bytes } }
      const restored = loadStoredJobState()
      assert.equal(restored.global.completedAt, null)
      assert.equal(restored.global.fading, false)
      assert.deepEqual(restored.global.status, status)
      assert.equal(JSON.stringify(stored), bytes)
    }
    const complete = { global: { ...job('complete', null, { success: true }), completedAt: 123, fading: false } }
    globalThis.window = { localStorage: { getItem: () => JSON.stringify(complete) } }
    assert.deepEqual(loadStoredJobState(), complete)
  } finally {
    if (savedWindow === undefined) delete globalThis.window
    else globalThis.window = savedWindow
  }
})

test('terminal telemetry cannot end a running job before its result is known', () => {
  for (const phase of ['complete', 'failed', 'unavailable', 'cancelled', 'canceled']) {
    const state = job('in_progress', { phase, detail: 'Old attempt' })
    assert.equal(getTrackedJobPhase(state), 'in_progress')
    assert.equal(isTrackedJobActive(state), true)
    assert.equal(isTrackedJobComplete(state), false)
  }
})

test('missing progress and ordinary intermediate phases preserve active tracking', () => {
  for (const phase of [null, 'analyzing', 'committed', 'cooling', 'loop_start']) {
    const state = job('in_progress', phase ? { phase, total: 10, processed: 3 } : null)
    assert.equal(getTrackedJobPhase(state), phase || 'in_progress')
    assert.equal(isTrackedJobActive(state), true)
    assert.equal(getAnalysisJobProgress(state).analyzed, phase ? 3 : 0)
  }
})

test('serialized cancellation remains an unsuccessful outcome with a clear explanation', () => {
  const state = job('complete', oldComplete, {
    success: false, result: { type: 'CancelledError', message: '' },
  })
  assert.equal(getTrackedJobPhase(state), 'failed')
  assert.equal(isTrackedJobComplete(state), false)
  assert.equal(isTrackedJobActive(state), false)
  assert.equal(getAnalysisJobProgress(state).detail, 'Job was cancelled.')
})

test('a failed result without a message cannot reuse an old successful detail', () => {
  const state = job('complete', oldComplete, { success: false, result: null })
  assert.equal(getAnalysisJobProgress(state).detail, 'Job failed.')
})

test('an explicit missing-job response cannot inherit completed progress', () => {
  const state = job('not_found')
  assert.equal(getTrackedJobPhase(state), 'not_found')
  assert.equal(isTrackedJobActive(state), false)
  assert.equal(isTrackedJobComplete(state), false)
})

for (const success of [true, false]) {
  test(`confirmed ${success ? 'success' : 'failure'} bypasses stale progress timestamps`, () => {
    const state = job('in_progress', { phase: 'analyzing', updated_at: 300 })
    const incoming = job('complete', { ...oldComplete, updated_at: 100 }, { success }).status
    assert.equal(obsoletePositionStatus(state, incoming), false)
  })
}

test('cached telemetry completion cannot block a current running response', () => {
  const state = job('in_progress')
  assert.equal(obsoletePositionStatus(state, job('in_progress', null).status), false)
  assert.equal(obsoletePositionStatus(state, job('in_progress', {
    phase: 'analyzing', attempt_id: 'new-attempt', updated_at: 1,
  }).status), false)
})

test('recovery to a queue is allowed after confirmed failure despite older progress', () => {
  const state = job('complete', oldComplete, failed)
  const incoming = job('deferred', { ...oldComplete, updated_at: 100 }).status
  assert.equal(obsoletePositionStatus(state, incoming), false)
})

test('confirmed completion still rejects a delayed active discovery response', () => {
  const state = job('complete', oldComplete, { success: true })
  assert.equal(obsoletePositionStatus(state, job('in_progress', null).status), true)
})
