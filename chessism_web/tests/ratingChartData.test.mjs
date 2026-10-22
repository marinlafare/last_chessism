import test from 'node:test'
import assert from 'node:assert/strict'
import { groupedRatingPlayers, overlappingRatingBars, playerRating, ratingPlayerPage, RATING_BAR_MODES } from '../src/pages/home/ratingChartData.js'

test('overlapping bars keep Hours geometry with Rapid behind Blitz behind Bullet', () => {
  const bars = overlappingRatingBars(100, 80)
  assert.deepEqual(bars, [
    { mode: 'rapid', x: 110, width: 60 },
    { mode: 'blitz', x: 108, width: 44 },
    { mode: 'bullet', x: 128, width: 44 },
  ])
  assert.ok(bars[1].x + bars[1].width > bars[2].x)
  assert.equal(bars[0].x + bars[0].width / 2, 140)
})

test('grouped ratings keep the blitz roster and requested left-to-right bar order', () => {
  const players = [{ player_name: 'A', rating: 3000 }, { player_name: 'b', rating: 2800 }]
  const modes = { rapid: [{ player_name: 'b', rating: 3500 }, { player_name: 'a', rating: 2600 }],
    bullet: [{ player_name: 'a', rating: 3200 }, { player_name: 'not-in-blitz', rating: 4000 }] }
  const before = structuredClone({ players, modes })
  const grouped = groupedRatingPlayers(players, modes)
  assert.deepEqual(RATING_BAR_MODES, ['blitz', 'rapid', 'bullet'])
  assert.deepEqual(grouped.map(player => player.player_name), ['A', 'b'])
  assert.deepEqual(grouped[0].ratings, { blitz: 3000, rapid: 2600, bullet: 3200 })
  assert.deepEqual(grouped[1].ratings, { blitz: 2800, rapid: 3500, bullet: null })
  assert.deepEqual({ players, modes }, before)
})

test('ten-player pages are bounded, non-overlapping and use a fixed zero-based scale', () => {
  const players = groupedRatingPlayers(Array.from({ length: 24 }, (_, i) => ({ player_name: `p${i}`, rating: 3200 - i * 100 })))
  assert.deepEqual([0, 1, 2].map(page => ratingPlayerPage(players, page).players.length), [10, 10, 4])
  assert.equal(ratingPlayerPage(players, 1).start, 10)
  assert.equal(ratingPlayerPage(players, 100).page, 2)
  assert.equal(ratingPlayerPage(players, -1).page, 0)
  assert.equal(ratingPlayerPage(players, NaN).page, 0)
  assert.equal(ratingPlayerPage(players, 0).axisMax, 3500)
  assert.equal(ratingPlayerPage(players, 2).axisMax, 3500)
  assert.equal(ratingPlayerPage().total, 0)
  assert.equal(ratingPlayerPage().pages, 1)
  assert.equal(ratingPlayerPage().axisMax, 500)
})

test('absent or invalid mode ratings are not silently drawn as zero', () => {
  for (const rating of [null, undefined, '', 'oops', Infinity, -1, 0]) assert.equal(playerRating({ rating }), null)
  assert.equal(playerRating({ last_rating: 1500 }), 1500)
  assert.equal(playerRating({ rating: '2500', last_rating: 1500 }), 2500)
  assert.deepEqual(groupedRatingPlayers([{ player_name: 'single', rating: 2000 }])[0].ratings,
    { blitz: 2000, rapid: null, bullet: null })
})
