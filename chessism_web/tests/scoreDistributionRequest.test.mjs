import assert from 'node:assert/strict'
import { test } from 'node:test'
import { pollScoreDistribution } from '../src/pages/players/scoreDistributionRequest.js'

test('ready charts are returned without polling', async () => {
  const data = { bins: [], by_mode: {}, stale: true }
  let calls = 0
  const result = await pollScoreDistribution(async () => { calls++; return data })
  assert.equal(result, data)
  assert.equal(calls, 1)
})

test('pending results report progress and wait for the complete chart', async () => {
  const updates = []
  let calls = 0
  const data = { bins: [{ id: 6 }], by_mode: {} }
  const result = await pollScoreDistribution(async () => {
    if (++calls === 1) return { status: 'calculating', retry_after_seconds: 0.001, progress: { completed_games: 1000, total_games: 2000 } }
    return data
  }, { onProgress: value => updates.push(value) })
  assert.equal(result, data)
  assert.equal(calls, 2)
  assert.deepEqual(updates, [{ completed_games: 1000, total_games: 2000 }])
})

test('closing the chart cancels polling without starting another request', async () => {
  const controller = new AbortController()
  let calls = 0
  await assert.rejects(pollScoreDistribution(async () => {
    calls++
    return { status: 'calculating', progress: {} }
  }, { signal: controller.signal, onProgress: () => controller.abort() }), { name: 'AbortError' })
  assert.equal(calls, 1)
})

test('a failed calculation remains an error, not an empty chart', async () => {
  await assert.rejects(pollScoreDistribution(async () => { throw new Error('Database unavailable') }), /Database unavailable/)
})
