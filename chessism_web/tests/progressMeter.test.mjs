import assert from 'node:assert/strict'
import { test } from 'node:test'
import { progressMeter } from '../src/components/progress/progressMeter.js'

test('meters keep real zero and partial progress, clamping only their displayed geometry', () => {
  assert.deepEqual(progressMeter(0, 10), { value: 0, max: 10, percent: 0 })
  assert.deepEqual(progressMeter(5, 20), { value: 5, max: 20, percent: 25 })
  assert.deepEqual(progressMeter(-1, 20), { value: 0, max: 20, percent: 0 })
  assert.deepEqual(progressMeter(30, 20), { value: 20, max: 20, percent: 100 })
  assert.equal(progressMeter(0.25).percent, 0.25)
})

test('unknown or invalid counters never become a fabricated percentage or completion', () => {
  for (const [value, max] of [[null, 100], [undefined, 100], [NaN, 100], [10, 0],
    [Infinity, 100], [10, Infinity], [10, -1], ['50', 100]]) {
    assert.deepEqual(progressMeter(value, max), { value: undefined, max: undefined, percent: null })
  }
  assert.equal('done' in progressMeter(100, 100), false)
})
