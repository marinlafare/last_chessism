import test from 'node:test'
import assert from 'node:assert/strict'
import { ratingScatterBounds } from '../src/pages/scored-positions/ratingScatter.js'

test('rating bounds retain the full extent and one-based count axis', () => {
  assert.deepEqual(ratingScatterBounds([]), { minRating: 0, maxRating: 0, maxIndex: 1 })
  assert.deepEqual(ratingScatterBounds([{ rating: 1500, x_index: 0 }]),
    { minRating: 1500, maxRating: 1500, maxIndex: 1 })
  assert.deepEqual(ratingScatterBounds([
    { rating: '2500', x_index: '3' }, { rating: 800, x_index: 1 }, { rating: 1600, x_index: 2 },
  ]), { minRating: 800, maxRating: 2500, maxIndex: 3 })
  assert.deepEqual(ratingScatterBounds([{ rating: null, x_index: null }]),
    { minRating: 0, maxRating: 0, maxIndex: 1 })
})

test('one million ratings do not exceed the JavaScript function-argument limit', () => {
  const points = Array.from({ length: 1000000 }, (_, i) => ({ rating: 1400, x_index: i + 1 }))
  points[123456].rating = 300
  points[900000].rating = 3200
  assert.deepEqual(ratingScatterBounds(points), { minRating: 300, maxRating: 3200, maxIndex: 1000000 })
  assert.equal(points[123456].rating, 300)
  assert.equal(points[900000].rating, 3200)
})
