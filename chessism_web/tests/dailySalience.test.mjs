import assert from 'node:assert/strict'
import test from 'node:test'
import { relativeDailySalience, filterDailySalience, moveSalienceBound } from '../src/pages/players/dailySalience.js'

const points = (values) => values.map((dailySalience, i) => ({ date: `day-${i}`, dailySalience, games: 20, gameEfficiency: 80 + i }))

test('uses actual min/max, not percentiles; preserves accuracy and source points', () => {
  const source = points([2, 7, 12, 12, 12])
  const scale = relativeDailySalience(source)
  assert.equal(scale.minimum, 0.1)
  assert.equal(scale.maximum, 0.6)
  assert.equal(scale.enabled, true)
  scale.points.forEach((point, index) => {
    assert.ok(Math.abs(point.relativeSalience - [0, 50, 100, 100, 100][index]) < 1e-9)
  })
  assert.deepEqual(scale.points.map(p => p.gameEfficiency), source.map(p => p.gameEfficiency))
  assert.equal(Object.hasOwn(source[0], 'relativeSalience'), false)
  assert.equal(Object.hasOwn(source[0], 'averageGameSalience'), false)
  assert.deepEqual(scale.points.map(p => p.dailySalience), source.map(p => p.dailySalience))
})

test('game volume does not change the filter value for equal average salience', () => {
  const source = [
    {dailySalience: 5, games: 10}, {dailySalience: 50, games: 100},
    {dailySalience: 2, games: 10}, {dailySalience: 8, games: 10},
  ]
  const scale = relativeDailySalience(source)
  assert.deepEqual(scale.points.map(p => p.averageGameSalience), [0.5, 0.5, 0.2, 0.8])
  assert.ok(Math.abs(scale.points[0].relativeSalience - 50) < 1e-9)
  assert.equal(scale.points[0].relativeSalience, scale.points[1].relativeSalience)
  assert.equal(filterDailySalience(scale.points, [50, 50], true).length, 2)
  assert.equal(relativeDailySalience(source.slice(0, 2)).enabled, false)
})

test('identical daily sums can represent very different average game saliences', () => {
  const scale = relativeDailySalience([{dailySalience: 5, games: 10}, {dailySalience: 5, games: 100}])
  assert.deepEqual(scale.points.map(p => p.relativeSalience), [100, 0])
})

test('invalid or absent counts do not become zero salience or divide by zero', () => {
  const source = [0, null, undefined, -1, NaN, Infinity, 1.5].map(games => ({dailySalience: 1, games}))
  const scale = relativeDailySalience([...source, {dailySalience: 0, games: 10}, {dailySalience: 1, games: 10}])
  assert.ok(scale.points.slice(0, source.length).every(p => p.averageGameSalience === null && p.relativeSalience === null))
  assert.equal(filterDailySalience(scale.points, [0, 99], true).length, 1)
})

test('inclusive filtering keeps the original scale and does not round scores', () => {
  const scale = relativeDailySalience(points([2, 5, 7, 9, 12]))
  assert.deepEqual(filterDailySalience(scale.points, [30, 70], true).map(p => p.dailySalience), [5, 7, 9])
  assert.deepEqual(filterDailySalience(scale.points, [50, 50], true).map(p => p.dailySalience), [7])
  assert.deepEqual(filterDailySalience(scale.points, [51, 69], true), [])
  assert.strictEqual(filterDailySalience(scale.points, [0, 100], true), scale.points)
  assert.equal(scale.minimum, 0.1)
  assert.equal(scale.maximum, 0.6)
  const decimal = relativeDailySalience(points([0, 0.499, 1]))
  assert.equal(filterDailySalience(decimal.points, [50, 100], true).length, 1)
})

test('zero is real salience; missing data is not coerced to zero', () => {
  const scale = relativeDailySalience(points([null, undefined, NaN, Infinity, -1, 0, 0.001]))
  assert.equal(scale.minimum, 0)
  assert.equal(scale.maximum, 0.001 / 20)
  assert.deepEqual(scale.points.map(p => p.relativeSalience), [null, null, null, null, null, 0, 100])
  assert.equal(filterDailySalience(scale.points, [0, 100], true).length, 7)
  assert.equal(filterDailySalience(scale.points, [0, 99], true).length, 1)
})

test('empty, missing, single or identical values disable filtering without hiding days', () => {
  for (const values of [[], [null], [7], [0, 0], [7, 7, null]]) {
    const scale = relativeDailySalience(points(values))
    assert.equal(scale.enabled, false)
    assert.strictEqual(filterDailySalience(scale.points, [30, 70], scale.enabled), scale.points)
    assert.ok(scale.points.every(p => p.relativeSalience === null))
  }
})

test('two handles stay ordered, within bounds, and can reopen a collapsed range', () => {
  assert.deepEqual(moveSalienceBound([20, 80], 0, 90), [80, 80])
  assert.deepEqual(moveSalienceBound([20, 80], 1, 10), [20, 20])
  assert.deepEqual(moveSalienceBound([20, 80], 0, -10), [0, 80])
  assert.deepEqual(moveSalienceBound([20, 80], 1, 110), [20, 100])
  assert.deepEqual(moveSalienceBound([50, 50], 1, 70.3), [50, 70])
  assert.deepEqual(moveSalienceBound([50, 50], 0, 29.6), [30, 50])
})
