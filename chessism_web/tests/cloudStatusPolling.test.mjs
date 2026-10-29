import assert from 'node:assert/strict'
import { test } from 'node:test'
import { cloudStatusPolling } from '../src/pages/positions/cloudStatusPolling.js'

function fixture() {
  const requests = [], data = [], errors = [], timers = new Map()
  let next = 0, hidden = false
  const polling = cloudStatusPolling({
    fetchStatus: ({ signal }) => new Promise((resolve, reject) => requests.push({ signal, resolve, reject })),
    onData: value => data.push(value), onError: error => errors.push(error), isHidden: () => hidden,
    schedule: (fn, delay) => { timers.set(++next, { fn, delay }); return next },
    unschedule: id => timers.delete(id),
  })
  return { polling, requests, data, errors, timers, hide: () => { hidden = true },
    tick: async () => { const [id, timer] = timers.entries().next().value; timers.delete(id); await timer.fn() } }
}

test('refresh aborts and supersedes an older poll, even if transport ignores cancellation', async () => {
  const f = fixture()
  const first = f.polling.refresh(), second = f.polling.refresh()
  assert.equal(f.requests[0].signal.aborted, true)
  f.requests[1].resolve('stopping'); await second
  f.requests[0].resolve('running'); await first
  assert.deepEqual(f.data, ['stopping'])
  assert.equal(f.timers.size, 1)
  f.polling.stop()
  assert.equal(f.timers.size, 0)
})

test('stop acknowledgement invalidates pending status and unmount prevents late updates', async () => {
  const f = fixture()
  const first = f.polling.refresh()
  f.polling.invalidate()
  f.requests[0].resolve('old'); await first
  assert.deepEqual(f.data, [])
  const second = f.polling.refresh()
  f.polling.stop()
  f.requests[1].reject(new Error('late network failure')); await second
  assert.deepEqual(f.errors, [])
  assert.equal(f.timers.size, 0)
})

test('hidden tabs pause automatic reads but explicit actions still refresh', async () => {
  const f = fixture()
  const first = f.polling.refresh()
  f.requests[0].resolve('initial'); await first
  f.hide(); await f.tick()
  assert.equal(f.requests.length, 1)
  assert.equal(f.timers.size, 0) // No idle polling while hidden; visibility resumes it.
  const explicit = f.polling.refresh()
  f.requests[1].resolve('action result'); await explicit
  assert.deepEqual(f.data, ['initial', 'action result'])
  f.polling.stop()
})

test('cloud timeouts retry even when transport ignores abort, and late completion is ignored', async () => {
  const f = fixture()
  const pending = f.polling.refresh()
  const timeout = [...f.timers.values()][0]
  assert.equal(timeout.delay, 10000)
  await f.tick(); await pending
  assert.equal(f.requests[0].signal.aborted, true)
  assert.equal(f.errors[0].name, 'TimeoutError')
  const retry = f.tick()
  f.requests[1].resolve('still cleaning'); await retry
  f.requests[0].resolve('obsolete running snapshot')
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(f.data, ['still cleaning'])
  assert.equal([...f.timers.values()][0].delay, 5000)
  f.polling.stop()
})

test('100% saved is not terminal; cloud status continues through verification and cleanup', async () => {
  const f = fixture()
  for (const phase of ['verifying', 'cleaning', 'complete']) {
    const read = phase === 'verifying' ? f.polling.refresh() : f.tick()
    f.requests.at(-1).resolve({ imported: 100, target: 100, phase })
    await read
    assert.equal(f.timers.size, 1)
  }
  assert.deepEqual(f.data.map(row => row.phase), ['verifying', 'cleaning', 'complete'])
  f.polling.stop()
})

test('transient failures retain data, back off, and successful reads reset the interval', async () => {
  const f = fixture()
  const first = f.polling.refresh()
  f.requests[0].resolve('last snapshot'); await first
  const second = f.tick()
  f.requests[1].reject(new Error('offline')); await second
  assert.deepEqual(f.data, ['last snapshot'])
  assert.equal(f.errors.length, 1)
  assert.equal([...f.timers.values()][0].delay, 10000)
  const third = f.tick()
  f.requests[2].resolve('online'); await third
  assert.equal([...f.timers.values()][0].delay, 5000)
  f.polling.stop()
})
