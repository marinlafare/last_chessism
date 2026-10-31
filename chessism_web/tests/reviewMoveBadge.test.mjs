import test from 'node:test'
import assert from 'node:assert/strict'
import { reviewMoveBadge } from '../src/pages/players/reviewMoveBadge.js'

test('badges reflect the existing review labels without deriving assessments', () => {
  for (const [classification, symbol] of [['best','!'],['good','✓'],['inaccuracy','?!'],['mistake','?'],['blunder','??']]) {
    assert.equal(reviewMoveBadge({ classification, uci: 'e2e4' }).symbol, symbol)
  }
  for (const move of [null, {}, { classification: 'unknown', uci: 'e2e4' },
    { classification: 'good', uci: null }, { classification: 'good', uci: 'a9a8' }]) {
    assert.equal(reviewMoveBadge(move), null)
  }
})
test('the destination follows either orientation including board edges', () => {
  for (const uci of ['e2e4', 'e7e5', 'h7h8q', 'a2a1q']) {
    const move = { classification: 'good', uci }
    const white = reviewMoveBadge(move), black = reviewMoveBadge(move, 'black')
    assert.equal(white.square, uci.slice(2, 4))
    assert.equal(white.column + black.column, 7)
    assert.equal(white.row + black.row, 7)
  }
})
test('captures, en passant, castling and promotion use the actual moved piece destination', () => {
  for (const [uci, square] of [['c4f7','f7'], ['e5d6','d6'], ['e1g1','g1'], ['e8c8','c8'], ['a7b8n','b8']]) {
    assert.equal(reviewMoveBadge({ classification: 'best', uci }).square, square)
  }
})
test('book annotation replaces the green badge but does not change the assessment', () => {
  const move = { uci: 'e2e4', classification: 'good', book: { openings: [{ name: 'King’s Pawn', eco: 'B00' }] } }
  const badge = reviewMoveBadge(move)
  assert.equal(badge.classification, 'book')
  assert.match(badge.title, /King’s Pawn/)
  assert.equal(move.classification, 'good')
  assert.equal(reviewMoveBadge({ ...move, classification: null }).classification, 'book')
})
