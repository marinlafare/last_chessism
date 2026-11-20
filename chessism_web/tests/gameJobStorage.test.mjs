import test from 'node:test'
import assert from 'node:assert/strict'
import { beginJobRequest, loadStoredJob, storeJob, subscribeStoredJob } from '../src/services/gameJobService.js'

const key = 'synthetic-job'
function browser(t) {
  const previous = globalThis.window, values = new Map(), events = new EventTarget()
  globalThis.window = Object.assign(events, { localStorage: {
    getItem: name => values.get(name) ?? null,
    setItem: (name, value) => values.set(name, value),
    removeItem: name => values.delete(name),
  } })
  t.after(() => { globalThis.window = previous })
  return window
}

test('same-document notifications observe the already-persisted identity and deletion', t => {
  browser(t)
  const observed = []
  const stop = subscribeStoredJob(key, () => observed.push(loadStoredJob(key)))
  t.after(stop)
  storeJob(key, { jobId: 'queued' })
  storeJob(key, null, { expectedJobId: 'queued' })
  assert.deepEqual(observed, [{ jobId: 'queued' }, null])
})

test('older acknowledgments cannot overwrite a newer identity or revive completed work', t => {
  browser(t)
  const older = beginJobRequest(key), newer = beginJobRequest(key)
  assert.equal(storeJob(key, { jobId: 'new' }, { request: newer }), true)
  assert.equal(storeJob(key, { jobId: 'old' }, { request: older }), false)
  assert.deepEqual(loadStoredJob(key), { jobId: 'new' })
  assert.equal(storeJob(key, null, { expectedJobId: 'new' }), true)
  assert.equal(storeJob(key, { jobId: 'old' }, { request: older }), false)
  assert.equal(loadStoredJob(key), null)
})

test('newer enqueue ownership is established before its response arrives', t => {
  browser(t)
  const older = beginJobRequest(key), newer = beginJobRequest(key)
  assert.equal(storeJob(key, { jobId: 'old' }, { request: older }), false)
  assert.equal(loadStoredJob(key), null)
  assert.equal(storeJob(key, { jobId: 'new' }, { request: newer }), true)
})

test('an old terminal callback cannot clear replacement work or notify subscribers', t => {
  browser(t)
  storeJob(key, { jobId: 'replacement' })
  let changes = 0
  const stop = subscribeStoredJob(key, () => changes++)
  t.after(stop)
  assert.equal(storeJob(key, null, { expectedJobId: 'old' }), false)
  assert.deepEqual(loadStoredJob(key), { jobId: 'replacement' })
  assert.equal(changes, 0)
})

test('unsubscribing releases same-document and cross-document listeners', t => {
  const window = browser(t)
  let changes = 0
  const stop = subscribeStoredJob(key, () => changes++)
  stop()
  storeJob(key, { jobId: 'next' })
  const event = Object.assign(new Event('storage'), { key, storageArea: window.localStorage })
  window.dispatchEvent(event)
  assert.equal(changes, 0)
})

test('an observed cross-document replacement invalidates an outstanding local request', t => {
  const window = browser(t)
  let observed
  const stop = subscribeStoredJob(key, () => { observed = loadStoredJob(key) })
  t.after(stop)
  const older = beginJobRequest(key)
  window.localStorage.setItem(key, JSON.stringify({ jobId: 'other-document' }))
  window.dispatchEvent(Object.assign(new Event('storage'), { key, storageArea: window.localStorage }))
  assert.deepEqual(observed, { jobId: 'other-document' })
  assert.equal(storeJob(key, { jobId: 'old' }, { request: older }), false)
})
