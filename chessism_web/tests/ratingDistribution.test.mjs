import test from 'node:test'
import assert from 'node:assert/strict'
import { ratingDistributionPlot } from '../src/pages/players/ratingDistribution.js'

test('preserves percentage bars, including zero bins, rather than cumulating them', () => {
  const plot = ratingDistributionPlot({ x: [2200, 2210, 2220, 2230], y: [0.4, 50.2, 40.8, 8.6], bin_width: 10 })
  assert.deepEqual(plot.bins.map(row => row.percent), [0.4, 50.2, 40.8, 8.6])
  assert.equal(plot.ceiling, 60)
  assert.equal(plot.binWidth, 10)
  assert.deepEqual(plot.ticks, [0, 1, 2, 3])
})
test('empty, single and wide distributions use bounded labels', () => {
  assert.deepEqual(ratingDistributionPlot(null).bins, [])
  assert.deepEqual(ratingDistributionPlot({ x: [1000], y: [100] }).ticks, [0])
  const plot = ratingDistributionPlot({ x: Array.from({ length: 500 }, (_, i) => i * 10), y: Array(500).fill(0.2) })
  assert.equal(plot.ceiling, 0.2)
  assert.equal(plot.ticks.length, 5)
})
