import assert from 'node:assert/strict'
import test from 'node:test'
import { SALIENCE_BAND_BOUNDARIES, attachSalienceBands, filterSalienceBands, moveSalienceBandBound, salienceBoundStops, salienceBandNote } from '../src/pages/players/dailySalienceBands.js'

test('independent salience totals and ties do not duplicate accuracy points', () => {
  const points = [{ date: '2026-01-01', games: 2, band: 0 }, { date: '2026-01-01', games: 2, band: 1 }, { date: '2026-01-02' }]
  const snapshot = { status: 'ready', points: [['2026-01-01', [3,5], [0,0,0,4,0,4], 8]] }
  const mapped = attachSalienceBands(points, snapshot)
  assert.equal(mapped.length, 3)
  assert.equal(mapped[0].games, 2)
  assert.equal(mapped[0].salienceBands.games, 8)
  assert.equal(mapped[2].salienceBands, null)
  assert.equal(filterSalienceBands(mapped, [70,80], true).length, 2)
  assert.equal(filterSalienceBands(mapped, [90,100], true).length, 2)
  assert.equal(filterSalienceBands(mapped, [70,100], true).length, 2)
  assert.equal(filterSalienceBands(mapped, [0,50], true).length, 0)
  assert.equal(filterSalienceBands(mapped, [0,100], true).length, 3)
  assert.equal(filterSalienceBands(mapped, [0,50], false).length, 3)
  assert.match(salienceBandNote(mapped[0]), /8 games with calculated salience/)
  assert.match(salienceBandNote(mapped[0]), /Tied most common salience bands: >70–80, >90–100/)
  assert.match(salienceBandNote(mapped[0]), /Salience >90–100: 4 games/)
  assert.equal(points[0].salienceBands, undefined)
})

test('all snapped ranges select complete modal bins with exact boundary membership', () => {
  const stops = SALIENCE_BAND_BOUNDARIES
  const points = stops.slice(1).map((_, band) => ({ band, salienceBands: { modalBands: [band] } }))
  for (let low = 0; low < stops.length - 1; low++) {
    for (let high = low + 1; high < stops.length; high++) {
      assert.deepEqual(filterSalienceBands(points, [stops[low], stops[high]], true), points.slice(low, high))
    }
  }
  assert.deepEqual(filterSalienceBands(points, [70,90], true).map(p => p.band), [3,4])
})

test('range movement snaps to the closest boundary and always retains at least one bin', () => {
  assert.deepEqual(moveSalienceBandBound([0,100], 0, 26), [50,100])
  assert.deepEqual(moveSalienceBandBound([0,100], 0, 73), [70,100])
  assert.deepEqual(moveSalienceBandBound([70,100], 1, 86), [70,90])
  assert.deepEqual(moveSalienceBandBound([70,90], 0, 100), [80,90])
  assert.deepEqual(moveSalienceBandBound([70,90], 1, 0), [70,80])
  assert.deepEqual(moveSalienceBandBound([0,50], 0, 100), [0,50])
  assert.deepEqual(moveSalienceBandBound([90,100], 1, -20), [90,100])
  assert.deepEqual(moveSalienceBandBound([0,100], 0, NaN), [0,100])
  assert.deepEqual(salienceBoundStops([70,90], 0), [0,50,60,70,80])
  assert.deepEqual(salienceBoundStops([70,90], 1), [80,90,100])
})

test('filtering uses modal bins, never daily averages, and excludes unknown days only when filtered', () => {
  const points = [
    { date: 'a', dailySalience: .2, games: 1, salienceBands: { modalBands: [5] } },
    { date: 'b', dailySalience: .99, games: 1, salienceBands: { modalBands: [0] } },
    { date: 'c', salienceBands: null },
  ]
  assert.deepEqual(filterSalienceBands(points, [90,100], true), [points[0]])
  assert.deepEqual(filterSalienceBands(points, [0,50], true), [points[1]])
  assert.equal(filterSalienceBands(points, [0,100], true), points)
})

test('pending, missing and malformed summaries never turn into zero salience', () => {
  const points = [{ date: '2026-01-01' }]
  for (const snapshot of [null, { status: 'pending' }, { status: 'ready', points: [['2026-01-01', [0], [0,0,0,0,0,0], 0]] }]) {
    assert.equal(attachSalienceBands(points, snapshot)[0].salienceBands, null)
  }
})
