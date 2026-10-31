import assert from 'node:assert/strict'
import { test } from 'node:test'
import { watchStatus } from '../src/services/statusPolling.js'

const settle = () => new Promise(resolve => setImmediate(resolve))
function clock() {
  const pending = new Map()
  let next = 0
  return {
    pending,
    setTimer(callback, delay) { pending.set(++next, { callback, delay }); return next },
    clearTimer(id) { pending.delete(id) },
    run(delay) {
      const entry = [...pending].find(([, task]) => task.delay === delay)
      assert.ok(entry, `Expected a ${delay}ms timer`)
      pending.delete(entry[0])
      return entry[1].callback()
    },
  }
}
function visibility(initial = 'visible') {
  const listeners = new Set()
  return {
    visibilityState: initial, listeners,
    addEventListener(name, fn) { assert.equal(name, 'visibilitychange'); listeners.add(fn) },
    removeEventListener(name, fn) { assert.equal(name, 'visibilitychange'); listeners.delete(fn) },
    change(value) { this.visibilityState = value; listeners.forEach(fn => fn()) },
  }
}
function harness(options = {}) {
  const time = clock(), document = visibility(), data = [], errors = []
  let reads = 0
  const stop = watchStatus({
    ...time, visibilityDocument: document, intervalMs: 100, maxBackoffMs: 400,
    fetcher: async () => ++reads, onData: value => data.push(value),
    onError: error => errors.push(error), ...options,
  })
  return { time, document, data, errors, stop }
}

test('default polling preserves serial reads, interval and bounded error backoff', async () => {
  let calls = 0
  const h = harness({ fetcher: async () => {
    if (++calls <= 3) throw new Error('Offline')
    return calls
  } })
  await settle()
  assert.equal(h.errors.length, 1)
  await h.time.run(200)
  await h.time.run(400)
  await h.time.run(400)
  assert.deepEqual(h.data, [4])
  await h.time.run(100)
  assert.deepEqual(h.data, [4, 5])
  assert.equal(h.document.listeners.size, 0)
  h.stop()
  assert.equal(h.time.pending.size, 0)
})

test('hidden tabs make no reads or fake successes; returning refreshes immediately', async () => {
  const h = harness({ pauseWhenHidden: true })
  await settle()
  h.document.change('hidden')
  assert.equal(h.time.pending.size, 0)
  assert.deepEqual(h.data, [1])
  h.document.change('visible')
  h.document.change('visible') // Duplicate browser event must not start a second read.
  await settle()
  assert.deepEqual(h.data, [1, 2])
  assert.equal(h.time.pending.size, 1)
  h.stop()
  assert.equal(h.document.listeners.size, 0)
  h.document.change('visible')
  await settle()
  assert.deepEqual(h.data, [1, 2])
})

test('initially hidden tabs wait until visible, unless pausing was not requested', async () => {
  const document = visibility('hidden')
  const h = harness({ visibilityDocument: document, pauseWhenHidden: true })
  await settle()
  assert.deepEqual(h.data, [])
  assert.equal(h.time.pending.size, 0)
  document.change('visible')
  await settle()
  assert.deepEqual(h.data, [1])
  h.stop()
  const defaults = harness({ visibilityDocument: visibility('hidden') })
  await settle()
  assert.deepEqual(defaults.data, [1])
  defaults.stop()
})

test('a hung request times out, retries, and cannot publish a late response', async () => {
  let resolveOld, signal, calls = 0
  const h = harness({ requestTimeoutMs: 20, fetcher: options => {
    if (++calls === 1) {
      signal = options.signal
      return new Promise(resolve => { resolveOld = resolve })
    }
    return Promise.resolve('current')
  } })
  await settle()
  h.time.run(20)
  await settle()
  assert.equal(signal.aborted, true)
  assert.equal(h.errors[0].name, 'TimeoutError')
  await h.time.run(200)
  assert.deepEqual(h.data, ['current'])
  resolveOld('obsolete')
  await settle()
  assert.deepEqual(h.data, ['current'])
  assert.equal(h.time.pending.size, 1)
  h.stop()
})

test('hiding aborts only the status read and suppresses its late result/error', async () => {
  for (const rejectOld of [false, true]) {
    let finish, signal, calls = 0
    const h = harness({ pauseWhenHidden: true, requestTimeoutMs: 20, fetcher: options => {
      if (++calls > 1) return Promise.resolve('fresh')
      signal = options.signal
      return new Promise((resolve, reject) => { finish = rejectOld ? reject : resolve })
    } })
    await settle()
    h.document.change('hidden')
    assert.equal(signal.aborted, true)
    assert.equal(h.time.pending.size, 0)
    h.document.change('visible')
    await settle()
    finish(rejectOld ? new Error('Old read failed') : 'old')
    await settle()
    assert.deepEqual(h.data, ['fresh'])
    assert.deepEqual(h.errors, [])
    h.stop()
  }
})

test('unmount aborts an in-flight read and removes every timer and listener', async () => {
  let finish, signal
  const h = harness({ pauseWhenHidden: true, requestTimeoutMs: 20, fetcher: options => {
    signal = options.signal
    return new Promise(resolve => { finish = resolve })
  } })
  await settle()
  h.stop()
  assert.equal(signal.aborted, true)
  assert.equal(h.time.pending.size, 0)
  assert.equal(h.document.listeners.size, 0)
  finish('late')
  await settle()
  assert.deepEqual(h.data, [])
  assert.deepEqual(h.errors, [])
})

test('synchronous failures retry and cooperative aborts stay silent', async () => {
  let calls = 0
  const h = harness({ fetcher: () => {
    if (++calls === 1) throw new Error('Synchronous read failure')
    if (calls === 2) throw Object.assign(new Error('Aborted'), { name: 'AbortError' })
    return 'ok'
  } })
  await settle()
  await h.time.run(200)
  await h.time.run(200)
  assert.deepEqual(h.data, ['ok'])
  assert.equal(h.errors.length, 1)
  h.stop()
})

test('workflow terminal callbacks stop reads, timers and visibility listeners', async () => {
  for (const terminalError of [false, true]) {
    let calls = 0, delivered = 0
    const h = harness({ pauseWhenHidden: true, requestTimeoutMs: 20,
      fetcher: () => { calls++; if (terminalError) throw new Error('Confirmed missing job'); return 'complete' },
      onData: () => { delivered++; return false },
      onError: () => { delivered++; return false },
    })
    await settle()
    assert.equal(delivered, 1)
    assert.equal(h.time.pending.size, 0)
    assert.equal(h.document.listeners.size, 0)
    h.document.change('hidden'); h.document.change('visible')
    assert.equal(calls, 1)
    h.stop()
  }
})
