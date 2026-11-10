import assert from 'node:assert/strict'
import { test } from 'node:test'
import { watchStatus } from '../src/services/statusPolling.js'
import { isMissingJobResponse } from '../src/services/gameJobPolling.js'
import { getTrackedJobPhase, isTrackedJobActive } from '../src/pages/positions/positionPageSupport.js'
import { obsoletePositionStatus } from '../src/pages/positions/positionStatusFreshness.js'

const settle = () => new Promise(resolve => setImmediate(resolve))
const oldProgress = { phase: 'complete', detail: 'Old completion', updated_at: 200, attempt_id: 'old' }
const response = (status, progress = oldProgress, result = null) => ({
  job_id: 'analysis-job', status, progress, result,
})

function track(responses, initial = response('queued', null)) {
  const pending = new Map(), phases = [], errors = []
  let id = 0, requests = 0, state = { jobId: 'analysis-job', status: initial }
  // Use the shared transport with the same Positions status/freshness decisions.
  const stop = isTrackedJobActive(state) ? watchStatus({
    intervalMs: 3000, requestTimeoutMs: 10000,
    setTimer(callback, delay) { pending.set(++id, { callback, delay }); return id },
    clearTimer(key) { pending.delete(key) },
    fetcher: async () => {
      assert.ok(requests < responses.length, 'Unexpected extra status request')
      const next = responses[requests++]
      if (next instanceof Error) throw next
      return next
    },
    onData(status) {
      if (obsoletePositionStatus(state, status)) return
      state = { ...state, status }
      phases.push(getTrackedJobPhase(state))
      return isTrackedJobActive(state)
    },
    onError(error) {
      if (isMissingJobResponse(error, state.jobId)) {
        state = { ...state, status: error.payload }
        phases.push(getTrackedJobPhase(state))
        return false
      }
      errors.push(error)
    },
  }) : () => {}
  return {
    pending, phases, errors, stop,
    get requests() { return requests },
    async next(delay = 3000) {
      const entry = [...pending].find(([, timer]) => timer.delay === delay)
      assert.ok(entry, `Expected another Positions poll after ${delay}ms`)
      pending.delete(entry[0])
      await entry[1].callback()
    },
  }
}

test('failed-reset progress cannot stop polling before the actual unsuccessful result', async t => {
  const h = track([
    response('queued'), response('in_progress'),
    response('complete', null, { success: false, result: { type: 'RuntimeError', message: 'DB failed' } }),
  ])
  t.after(h.stop)
  await settle()
  assert.deepEqual(h.phases, ['queued'])
  await h.next()
  assert.deepEqual(h.phases, ['queued', 'in_progress'])
  await h.next()
  assert.deepEqual(h.phases, ['queued', 'in_progress', 'failed'])
  assert.equal(h.requests, 3)
  assert.equal(h.pending.size, 0)
})

test('restoring a running job with cached completion starts polling', async t => {
  const h = track([response('in_progress', null)], response('in_progress'))
  t.after(h.stop)
  await settle()
  assert.equal(h.requests, 1)
  assert.deepEqual(h.phases, ['in_progress'])
  assert.equal(h.pending.size, 1)
})

test('result availability without an outcome keeps polling until confirmed success', async t => {
  const h = track([response('complete'), response('complete', null, { success: true })])
  t.after(h.stop)
  await settle()
  assert.deepEqual(h.phases, ['in_progress'])
  await h.next()
  assert.deepEqual(h.phases, ['in_progress', 'complete'])
  assert.equal(h.pending.size, 0)
})

test('an older progress timestamp cannot prevent terminal failure from stopping polling', async t => {
  const h = track([response('complete', { ...oldProgress, updated_at: 100 }, { success: false })],
    response('in_progress', { phase: 'analyzing', updated_at: 300 }))
  t.after(h.stop)
  await settle()
  assert.deepEqual(h.phases, ['failed'])
  assert.equal(h.pending.size, 0)
})

test('network failure and missing progress preserve polling until ARQ confirms cancellation', async t => {
  const outage = new TypeError('Failed to fetch')
  const h = track([response('in_progress', null), outage,
    response('complete', null, { success: false, result: { type: 'CancelledError', message: '' } })])
  t.after(h.stop)
  await settle()
  await h.next()
  assert.deepEqual(h.errors, [outage])
  assert.deepEqual(h.phases, ['in_progress'])
  await h.next(6000)
  assert.deepEqual(h.phases, ['in_progress', 'failed'])
  assert.equal(h.pending.size, 0)
})

test('the explicit API missing-job response stops tracking without success', async t => {
  const missing = Object.assign(new Error('HTTP 404'), {
    status: 404, payload: response('not_found', null),
  })
  const h = track([missing])
  t.after(h.stop)
  await settle()
  assert.deepEqual(h.phases, ['not_found'])
  assert.equal(h.pending.size, 0)
  assert.deepEqual(h.errors, [])
})
