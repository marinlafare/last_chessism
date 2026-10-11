import test from 'node:test'
import assert from 'node:assert/strict'
import { circleDistribution, pieSegments } from '../src/pages/scored-positions/appearanceCircles.js'
import { allAppearanceHistogram } from '../src/pages/scored-positions/pendingRepetitions.js'

const histogram = rows => allAppearanceHistogram(rows.map(([appearances, total_fens]) => ({ appearances, total_fens }))).bars

test('uniqueness includes every repeated FEN but the right pie only weights 2–10 by distinct FEN count', () => {
  const view = circleDistribution(histogram([[0, 50], [1, 100], [2, 3], [10, 1], [11, 900]]))
  assert.equal(view.highlightValue, 100)
  assert.equal(view.nonUniqueValue, 904)
  assert.deepEqual(pieSegments([{value: view.highlightValue}, {value: view.nonUniqueValue}]).map(slice => slice.fraction), [100/1004, 904/1004])
  assert.deepEqual(view.items.map(item => [item.appearances, item.value]), [[2, 3], [10, 1]])
  assert.deepEqual(pieSegments(view.items).map(slice => slice.fraction), [.75, .25])
})

test('empty and missing singleton groups do not invent counts', () => {
  assert.deepEqual(circleDistribution([]), { highlightValue: 0, nonUniqueValue: 0, items: [] })
  assert.deepEqual(circleDistribution(histogram([[1, 100]])), { highlightValue: 100, nonUniqueValue: 0, items: [] })
  assert.equal(circleDistribution(histogram([[2, 5]])).highlightValue, 0)
  assert.equal(circleDistribution(histogram([[2, 5]])).nonUniqueValue, 5)
})

test('pie geometry handles one full circle, large sectors and empty or zero-sized groups', () => {
  assert.deepEqual(pieSegments([]), [])
  assert.deepEqual(pieSegments([{ value: 0 }, { value: -1 }, { value: NaN }]), [])
  const single = pieSegments([{ value: 10 }])[0]
  assert.equal(single.full, true)
  assert.equal(single.fraction, 1)
  const slices = pieSegments([{ value: 9 }, { value: 1 }])
  assert.ok(slices[0].path.includes('A 94 94 0 1 1'))
  assert.ok(slices[1].path.includes('A 94 94 0 0 1'))
  assert.ok(slices.every(slice => !slice.full && !/NaN|Infinity/.test(slice.path)))
})
