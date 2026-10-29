import assert from 'node:assert/strict'
import test from 'node:test'
import { attachSalienceBands, filterSalienceBands, salienceBandNote } from '../src/pages/players/dailySalienceBands.js'

test('independent salience totals and ties do not duplicate accuracy points', () => {
  const points = [{ date: '2026-01-01', games: 2, band: 0 }, { date: '2026-01-01', games: 2, band: 1 }, { date: '2026-01-02' }]
  const snapshot = { status: 'ready', points: [['2026-01-01', [3,5], [0,0,0,4,0,4], 8]] }
  const mapped = attachSalienceBands(points, snapshot)
  assert.equal(mapped.length, 3)
  assert.equal(mapped[0].games, 2)
  assert.equal(mapped[0].salienceBands.games, 8)
  assert.equal(mapped[2].salienceBands, null)
  assert.equal(filterSalienceBands(mapped, 3, true).length, 2)
  assert.equal(filterSalienceBands(mapped, 5, true).length, 2)
  assert.equal(filterSalienceBands(mapped, 0, true).length, 0)
  assert.equal(filterSalienceBands(mapped, null, true).length, 3)
  assert.equal(filterSalienceBands(mapped, 0, false).length, 3)
  assert.match(salienceBandNote(mapped[0]), /8 games with calculated salience/)
  assert.match(salienceBandNote(mapped[0]), /Tied most common salience bands: >70–80, >90–100/)
  assert.match(salienceBandNote(mapped[0]), /Salience >90–100: 4 games/)
  assert.equal(points[0].salienceBands, undefined)
})

test('pending, missing and malformed summaries never turn into zero salience', () => {
  const points = [{ date: '2026-01-01' }]
  for (const snapshot of [null, { status: 'pending' }, { status: 'ready', points: [['2026-01-01', [0], [0,0,0,0,0,0], 0]] }]) {
    assert.equal(attachSalienceBands(points, snapshot)[0].salienceBands, null)
  }
})
