import test from 'node:test'
import assert from 'node:assert/strict'
import { allAppearanceHistogram, appearanceScale, appearanceHistogram, appearanceWindow } from '../src/pages/scored-positions/pendingRepetitions.js'

test('appearances already start at one; exact counts, ten bars and capped heights', () => {
  const view = appearanceHistogram([{ repetitions: 0, fen_count: 67443 }, { repetitions: 1, fen_count: 9000000 },
    { repetitions: 2, fen_count: 2 }, { repetitions: 4, fen_count: 1000 }])
  assert.equal(view.total, 9001002)
  assert.equal(view.withoutAppearances, 67443)
  assert.deepEqual(view.bars.map(bar => bar.appearances), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
  assert.deepEqual(view.bars[1], { appearances: 2, count: 2, height: 2, clipped: false })
  assert.equal(view.bars[0].height, 1000)
  assert.equal(view.bars[0].count, 9000000)
  assert.equal(view.bars[0].clipped, true)
  assert.equal(view.bars[2].count, 0)
  assert.equal(view.bars[3].clipped, false)
})

test('zero and counts above ten stay separate; the last bar means exactly ten', () => {
  const view = appearanceHistogram([{ repetitions: 0, fen_count: 3 }, { repetitions: 10, fen_count: 4 },
    { repetitions: 11, fen_count: 2 }, { repetitions: 1000000, fen_count: 9 }])
  assert.equal(view.bars.length, 10)
  assert.equal(view.total, 15)
  assert.equal(view.withoutAppearances, 3)
  assert.equal(view.aboveRange, 11)
  assert.equal(view.bars[9].count, 4)
})

test('empty or invalid data does not invent FEN counts', () => {
  assert.equal(appearanceHistogram().total, 0)
  assert.equal(appearanceHistogram([{ repetitions: 0, fen_count: 4 }]).total, 0)
  assert.equal(appearanceHistogram([{ repetitions: -1, fen_count: 3 },
    { repetitions: NaN, fen_count: 3 }, { repetitions: 2, fen_count: -1 }]).total, 0)
})

test('all FEN appearances omit empty categories and retain the exact long-tail labels', () => {
  const view = allAppearanceHistogram([
    { appearances: 1000000, total_fens: 2, unscored_fens: 0 },
    { appearances: 1, total_fens: 40000000, unscored_fens: 30000000 },
    { appearances: 0, total_fens: 100, unscored_fens: 75 },
    { appearances: 2, total_fens: 3, unscored_fens: 1 },
    { appearances: 3, total_fens: 0, unscored_fens: 0 },
    { appearances: 2, total_fens: 1, unscored_fens: 0 },
  ])
  assert.deepEqual(view.bars.map(b => b.appearances), [1, 2, 1000000])
  assert.equal(view.bars[1].count, 4)
  assert.equal(view.bars[1].unscored, 1)
  assert.equal(view.total, 40000006)
  assert.equal(view.withoutAppearances, 100)
})

test('all-FEN scaling preserves zero and one, uses the same domain across pages, and accepts large datasets', () => {
  const bars = Array.from({ length: 200000 }, (_, i) => ({ count: i + 1 }))
  for (const log of [true, false]) {
    const scale = appearanceScale(bars, log)
    assert.equal(scale.fraction(0), 0)
    assert.ok(scale.fraction(1) > 0)
    assert.ok(scale.fraction(200000) <= 1)
    assert.ok(scale.fraction(1) < scale.fraction(200000))
    assert.deepEqual(scale.ticks, [0, .2, .4, .6, .8, 1])
    assert.equal(scale.fraction(scale.upper), 1)
  }
  assert.equal(allAppearanceHistogram().bars.length, 0)
  assert.equal(allAppearanceHistogram([{ appearances: -1, total_fens: 9 }, { appearances: 2, total_fens: NaN }]).bars.length, 0)
  assert.equal(appearanceScale([{ count: 1 }]).fraction(1), 1)
})

test('normalizing the log tick labels preserves the previous full-range bar heights', () => {
  const scale = appearanceScale([{ count: 2 }, { count: 5000000 }])
  assert.equal(scale.upper, 10000000)
  assert.equal(scale.fraction(2), Math.log10(3) / Math.log10(10000001))
  assert.equal(scale.fraction(5000000), Math.log10(5000001) / Math.log10(10000001))
  assert.ok(scale.fraction(5000000) < 1)
  assert.equal(appearanceScale([]).fraction(0), 0)
})

test('Uniqueness starts with the ten lowest values; Communess with the ten highest, never a partial tail', () => {
  const bars = Array.from({ length: 24 }, (_, i) => ({ appearances: i + 1, count: 30 - i }))
  assert.deepEqual(appearanceWindow(bars).bars.map(b => b.appearances), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
  assert.deepEqual(appearanceWindow(bars, 0, 'commonness').bars.map(b => b.appearances), [15, 16, 17, 18, 19, 20, 21, 22, 23, 24])
  const seen = []
  for (let page = 0; page < 3; page++) seen.push(...appearanceWindow(bars, page, 'commonness').bars.map(b => b.appearances))
  assert.equal(new Set(seen).size, 23)
  assert.deepEqual([...seen].sort((a, b) => a - b), bars.slice(1).map(b => b.appearances))
  assert.equal(appearanceWindow(bars, 100, 'commonness').currentPage, 2)
  assert.equal(appearanceWindow(bars, -1).currentPage, 0)
  assert.equal(appearanceWindow(bars, NaN).currentPage, 0)
  assert.deepEqual(appearanceWindow([], 0, 'commonness').bars, [])
  assert.deepEqual(appearanceWindow([{ appearances: 1, count: 1 }], 0, 'commonness').bars, [])
})

test('both end views retain sparse labels and share the full distribution normalization', () => {
  const view = allAppearanceHistogram(Array.from({ length: 24 }, (_, i) => ({ appearances: 1 + i * 10000, total_fens: i ? 2 : 5000000 })))
  const scale = appearanceScale(view.bars)
  const tail = appearanceWindow(view.bars, 0, 'commonness')
  assert.equal(tail.bars[0].appearances, 140001)
  assert.equal(tail.bars.at(-1).appearances, 230001)
  assert.equal(scale.upper, 10000000)
  assert.ok(tail.bars.every(bar => scale.fraction(bar.count) < .1))
  assert.equal(appearanceWindow(view.bars, 2).bars.length, 4)
})
