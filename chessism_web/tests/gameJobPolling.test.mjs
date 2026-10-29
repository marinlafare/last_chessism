import assert from 'node:assert/strict'
import { test } from 'node:test'
import { recoverableGameJobs, watchGameJob } from '../src/services/gameJobPolling.js'

const settle = () => new Promise(resolve => setImmediate(resolve))
function timers() {
  const pending = new Map()
  let id = 0
  return {
    pending,
    setTimer(callback, delay) { pending.set(++id, { callback, delay }); return id },
    clearTimer(key) { pending.delete(key) },
    run(delay) {
      const entry = [...pending].find(([, task]) => task.delay === delay)
      assert.ok(entry, `Expected timer after ${delay}ms`)
      pending.delete(entry[0])
      return entry[1].callback()
    },
  }
}

test('a proxy 500 retains tracking and the next poll reports real worker progress', async () => {
  const clock = timers(), statuses = [], errors = []
  const progress = { status: 'in_progress', progress: { phase: 'downloading', processed: 28 } }
  let calls = 0
  const stop = watchGameJob({
    ...clock, jobId: 'existing',
    fetchStatus: async () => {
      if (++calls === 1) throw Object.assign(new Error('HTTP 500'), { status: 500 })
      return progress
    },
    onStatus: value => statuses.push(value), onUnavailable: error => errors.push(error),
  })
  await settle()
  assert.equal(errors.length, 1)
  assert.deepEqual(statuses, [])
  await clock.run(3000)
  assert.deepEqual(statuses, [progress])
  assert.equal(calls, 2)
  assert.equal([...clock.pending.values()][0].delay, 1500)
  stop()
  assert.equal(clock.pending.size, 0)
})

test('network loss and an unrelated 404 do not invent a terminal job state', async () => {
  const clock = timers(), statuses = []
  let calls = 0
  const stop = watchGameJob({
    ...clock, jobId: 'existing',
    fetchStatus: async () => {
      if (++calls === 1) throw new TypeError('Failed to fetch')
      throw Object.assign(new Error('Not found'), { status: 404, payload: {} })
    },
    onStatus: value => statuses.push(value), onUnavailable: () => {},
  })
  await settle()
  await clock.run(3000)
  assert.deepEqual(statuses, [])
  assert.equal([...clock.pending.values()][0].delay, 6000)
  stop()
})

test('the API explicit missing-job response ends tracking without claiming success', async () => {
  const clock = timers(), statuses = []
  const payload = { job_id: 'existing', status: 'not_found' }
  watchGameJob({
    ...clock, jobId: 'existing',
    fetchStatus: async () => { throw Object.assign(new Error('HTTP 404'), { status: 404, payload }) },
    onStatus: value => statuses.push(value), onUnavailable: () => assert.fail('Not a transport failure'),
  })
  await settle()
  assert.deepEqual(statuses, [payload])
  assert.equal(clock.pending.size, 0)
})

test('actual completion stops polling, including a failed worker result', async () => {
  for (const success of [true, false]) {
    const clock = timers(), statuses = []
    const payload = { status: 'complete', result: { success } }
    watchGameJob({
      ...clock, jobId: 'existing', fetchStatus: async () => payload,
      onStatus: value => statuses.push(value), onUnavailable: () => assert.fail(),
    })
    await settle()
    assert.deepEqual(statuses, [payload])
    assert.equal(clock.pending.size, 0)
  }
})

test('closing the page aborts an in-flight poll and ignores its late response', async () => {
  const clock = timers(), statuses = []
  let finish, signal
  const stop = watchGameJob({
    ...clock, jobId: 'existing',
    fetchStatus: (_, options) => { signal = options.signal; return new Promise(resolve => { finish = resolve }) },
    onStatus: value => statuses.push(value), onUnavailable: () => assert.fail(),
  })
  stop()
  assert.equal(signal.aborted, true)
  finish({ status: 'in_progress' })
  await settle()
  assert.deepEqual(statuses, [])
  assert.equal(clock.pending.size, 0)
})

test('a stuck status read times out and retries instead of hanging forever', async () => {
  const clock = timers(), errors = []
  const stop = watchGameJob({
    ...clock, jobId: 'existing',
    fetchStatus: (_, { signal }) => new Promise((_, reject) => {
      signal.addEventListener('abort', () => reject(new Error('Status read timed out')), { once: true })
    }),
    onStatus: () => assert.fail(), onUnavailable: error => errors.push(error),
  })
  clock.run(10000)
  await settle()
  assert.equal(errors.length, 1)
  assert.equal([...clock.pending.values()][0].delay, 3000)
  stop()
})

test('recovery discovers only live download/update jobs without resubmitting work', () => {
  const job = (id, functionName, phase = 'downloading') => ({
    job_id: id, status: 'in_progress', info: { function: functionName, args: [{ player_name: 'chessgarida' }] },
    progress: { phase },
  })
  const recovered = recoverableGameJobs([
    job('cloud', 'run_analysis_job'),
    job('old', 'run_create_player_games_job', 'complete'),
    job('download', 'run_create_player_games_job'),
    job('update', 'run_update_player_games_job'),
    job('delete', 'run_delete_player_job'),
  ])
  assert.deepEqual(Object.keys(recovered), ['download', 'update'])
  assert.deepEqual(recovered.download.job, { jobId: 'download', playerName: 'chessgarida' })
  assert.equal(recovered.update.job.jobId, 'update')
})

test('an ignored timeout cannot hang tracking or publish a late terminal response', async () => {
  const clock = timers(), statuses = [], errors = []
  let calls = 0, finishOld
  const stop = watchGameJob({
    ...clock, jobId: 'existing',
    fetchStatus: () => ++calls === 1 ? new Promise(resolve => { finishOld = resolve })
      : Promise.resolve({ status: 'in_progress', progress: { processed: 42 } }),
    onStatus: value => statuses.push(value), onUnavailable: error => errors.push(error),
  })
  clock.run(10000); await settle()
  assert.equal(errors[0].name, 'TimeoutError')
  await clock.run(3000)
  finishOld({ status: 'complete' }); await settle()
  assert.deepEqual(statuses, [{ status: 'in_progress', progress: { processed: 42 } }])
  assert.equal(clock.pending.size, 1)
  stop()
})
