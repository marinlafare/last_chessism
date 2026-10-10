import assert from 'node:assert/strict'
import test from 'node:test'
import { buildYearScale, yearAtX } from '../src/pages/players/dailyYearNavigation.js'

const point = date => ({ timestamp: Date.parse(`${date}T00:00:00Z`) })

test('year bands follow calendar dates, including leap years, without changing the input', () => {
  const points = [point('2025-01-01'), point('2024-02-29')]
  const before = structuredClone(points)
  const scale = buildYearScale(points, 112, 1182)
  assert.equal(scale.start, Date.UTC(2024, 0, 1))
  assert.equal(scale.end, Date.UTC(2026, 0, 1) - 1)
  assert.deepEqual(scale.bands.map(b => b.year), ['2024', '2025'])
  assert.equal(scale.bands[0].left, 112)
  assert.equal(scale.bands[1].right, 1182)
  assert.equal(scale.bands[0].right, scale.bands[1].left)
  assert.ok(scale.bands[0].right - scale.bands[0].left > scale.bands[1].right - scale.bands[1].left)
  assert.deepEqual(points, before)
})

test('single-day and single-year timelines still have a full selectable year', () => {
  for (const points of [[point('2025-01-01')], [point('2025-01-01'), point('2025-12-31')]]) {
    const scale = buildYearScale(points, 10, 100)
    assert.deepEqual(scale.bands, [{ year: '2025', left: 10, right: 100 }])
    assert.equal(yearAtX(scale.bands, 100), '2025')
    assert.equal(yearAtX(scale.bands, 55), '2025')
  }
})

test('exact year boundaries select the new year, and empty calendar years are not selectable', () => {
  const { bands } = buildYearScale([point('2023-06-01'), point('2025-06-01')], 0, 300)
  assert.equal(yearAtX(bands, bands[0].left), '2023')
  assert.equal(yearAtX(bands, bands[0].right), null)
  assert.equal(yearAtX(bands, (bands[0].right + bands[1].left) / 2), null)
  assert.equal(yearAtX(bands, bands[1].left), '2025')
  assert.equal(yearAtX(bands, -1), null)
  assert.equal(yearAtX(bands, 301), null)
  assert.equal(yearAtX(bands, NaN), null)
  const adjacent = buildYearScale([point('2024-12-31'), point('2025-01-01')], 0, 300).bands
  assert.equal(yearAtX(adjacent, adjacent[0].right), '2025')
})

test('empty or invalid dates do not create year bands', () => {
  assert.deepEqual(buildYearScale([], 0, 100), { bands: [], start: null, end: null })
  assert.deepEqual(buildYearScale([{ timestamp: NaN }], 0, 100).bands, [])
  assert.equal(yearAtX([], 50), null)
})
