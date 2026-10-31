import assert from 'node:assert/strict'
import test from 'node:test'
import { buildYearScale, yearAtX, buildDateScale, periodAtX, periodTicks, dateInPeriod, datePeriodLabel } from '../src/pages/players/dailyYearNavigation.js'

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

test('year zoom offers calendar months with observations and preserves empty gaps', () => {
  const points = [point('2024-01-31'), point('2024-02-29'), point('2024-04-01'), point('2025-01-01')]
  const scale = buildDateScale(points, 0, 366, '2024')
  assert.equal(scale.start, Date.UTC(2024,0,1))
  assert.equal(scale.end, Date.UTC(2025,0,1)-1)
  assert.deepEqual(scale.bands.map(b => b.key), ['2024-01','2024-02','2024-04'])
  assert.deepEqual(scale.bands.map(b => b.label), ['Jan','Feb','Apr'])
  assert.equal(periodAtX(scale.bands, scale.bands[1].left), '2024-02')
  assert.equal(periodAtX(scale.bands, (scale.bands[1].right + scale.bands[2].left)/2), null)
  assert.deepEqual(periodTicks(scale.start,scale.end,0,366,'2024').map(t => t.label),
    ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'])
})

test('month zoom spans the whole month including leap days, even with only one point', () => {
  for (const [period, last] of [['2024-02',29],['2025-02',28],['2024-12',31]]) {
    const year=Number(period.slice(0,4)), month=Number(period.slice(5))-1
    const scale=buildDateScale([point(`${period}-01`)],10,100,period)
    assert.equal(scale.start,Date.UTC(year,month,1))
    assert.equal(scale.end,Date.UTC(year,month+1,1)-1)
    assert.deepEqual(scale.bands,[])
    const ticks=periodTicks(scale.start,scale.end,10,100,period)
    assert.equal(ticks[0].label,'1')
    assert.equal(ticks.at(-1).label,String(last))
    assert.ok(ticks.every(t => t.x >= 10 && t.x <= 100))
  }
  assert.equal(datePeriodLabel('2024-02'),'Feb 2024')
  assert.equal(dateInPeriod('2024-02-29','2024-02'),true)
  assert.equal(dateInPeriod('2024-03-01','2024-02'),false)
  assert.equal(dateInPeriod('2024-12-31','2024'),true)
  assert.equal(dateInPeriod('2025-01-01','2024'),false)
})
