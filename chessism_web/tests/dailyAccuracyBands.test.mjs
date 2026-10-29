import assert from 'node:assert/strict'
import { accuracyBandPoints, accuracyBandNote, DAILY_ACCURACY_LABELS } from '../src/pages/players/dailyAccuracyBands.js'
import { relativeDailySalience, filterDailySalience } from '../src/pages/players/dailySalience.js'

const rows = [['2025-01-01', [3, 5], [1, 0, 1, 4, 2, 4], 6, 12], ['2025-01-02', [0], [3, 0, 0, 0, 0, 0], 2.4, 3]]
const points = relativeDailySalience(accuracyBandPoints(rows, true))
assert.equal(points.points.length, 3)
assert.equal(points.points[0].band, 3)
assert.equal(points.points[1].band, 5)
assert.equal(points.points[0].timestamp, points.points[1].timestamp)
assert.equal(points.points[0].averageGameSalience, .5)
assert.equal(filterDailySalience(points.points, [0, 50], points.enabled).length, 2)
const note = accuracyBandNote(points.points[0])
assert.match(note, /Tied most common bands: 71–80, 91–100/)
assert.match(note, /12 analyzed games/)
for (const label of DAILY_ACCURACY_LABELS) assert.ok(note.includes(label))
assert.match(note, /0–50: 1 games/)
assert.equal(accuracyBandPoints([], true).length, 0)
assert.equal(accuracyBandPoints([['2025-01-03', [], [0,0,0,0,0,0], null,0]],false).length,0)
