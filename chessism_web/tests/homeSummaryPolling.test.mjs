import assert from 'node:assert/strict'
import { test } from 'node:test'
import { summaryPolling } from '../src/pages/home/summaryPolling.js'

function fixture() {
  const requests = [], data = [], errors = [], timers = new Map()
  let next = 0, hidden = false
  const polling = summaryPolling({
    fetchSummary: ({ signal }) => new Promise((resolve, reject) => requests.push({ signal, resolve, reject })),
    onData: value => data.push(value), onError: error => errors.push(error), isHidden: () => hidden,
    schedule: (fn, delay) => { timers.set(++next, { fn, delay }); return next },
    unschedule: id => timers.delete(id),
  })
  return { polling, requests, data, errors, timers, visibility: value => { hidden = value },
    tick: () => { const [id, timer] = timers.entries().next().value; timers.delete(id); return timer.fn() } }
}

test('refreshes a newly ingested roster after 15 seconds without clearing the chart', async () => {
  const f = fixture()
  const first = f.polling.refresh()
  f.requests[0].resolve(['existing']); await first
  assert.equal([...f.timers.values()][0].delay,15000)
  const second = f.tick()
  assert.deepEqual(f.data,[['existing']])
  f.requests[1].resolve(['existing','new-player']); await second
  assert.deepEqual(f.data,[['existing'],['existing','new-player']])
  f.polling.stop()
})

test('visibility/focus refreshes do not overlap, and returning to a hidden tab reloads', async () => {
  const f = fixture()
  const first = f.polling.refresh()
  await f.polling.refresh()
  assert.equal(f.requests.length,1)
  f.requests[0].resolve('first'); await first
  f.visibility(true); await f.tick()
  assert.equal(f.requests.length,1)
  assert.equal(f.timers.size,0)
  f.visibility(false)
  const resumed = f.polling.refresh()
  f.requests[1].resolve('new import'); await resumed
  assert.deepEqual(f.data,['first','new import'])
  assert.equal(f.timers.size,1)
  f.polling.stop()
})

test('errors retain the previous chart, back off and recover automatically', async () => {
  const f = fixture()
  const first = f.polling.refresh()
  f.requests[0].resolve('last good roster'); await first
  const failed = f.tick()
  f.requests[1].reject(new Error('offline')); await failed
  assert.deepEqual(f.data,['last good roster'])
  assert.equal(f.errors.length,1)
  assert.equal([...f.timers.values()][0].delay,30000)
  const retry = f.tick()
  f.requests[2].resolve('fresh roster'); await retry
  assert.equal([...f.timers.values()][0].delay,15000)
  f.polling.stop()
})

test('timeouts abort requests and unmount ignores late responses and clears all timers', async () => {
  const f = fixture()
  const request = f.polling.refresh()
  assert.equal([...f.timers.values()][0].delay,20000)
  f.tick()
  assert.equal(f.requests[0].signal.aborted,true)
  f.requests[0].reject(new DOMException('Timeout','AbortError')); await request
  assert.equal(f.errors.length,1)
  const next = f.polling.refresh()
  f.polling.stop()
  assert.equal(f.requests[1].signal.aborted,true)
  f.requests[1].resolve('too late'); await next
  assert.deepEqual(f.data,[])
  assert.equal(f.timers.size,0)
})
