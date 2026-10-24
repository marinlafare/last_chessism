import test from 'node:test'
import assert from 'node:assert/strict'
import { planReviewMotion, reviewPositionPieces } from '../src/pages/players/reviewBoardMotion.js'

const plan = (before, after) => planReviewMotion(new Map(Object.entries(before)), new Map(Object.entries(after)))
test('FEN placement parsing ignores clocks and keeps piece colors', () => {
  const pieces = reviewPositionPieces('start')
  assert.equal(pieces.size, 32)
  assert.equal(pieces.get('e1'), 'wK')
  assert.equal(pieces.get('e8'), 'bK')
  assert.deepEqual(reviewPositionPieces('8/8/8/8/4P3/8/8/8 b - - 12 99'), new Map([['e4','wP']]))
})
test('unchanged positions do not animate stationary pieces', () => {
  assert.deepEqual(plan({a1:'wR',e8:'bK'},{a1:'wR',e8:'bK'}), {moves:[],appearing:[],disappearing:[]})
})
test('forward and backward moves match their source without duplicating pieces', () => {
  assert.deepEqual(plan({e2:'wP',d2:'wP'},{e4:'wP',d2:'wP'}), {moves:[{from:'e2',to:'e4'}],appearing:[],disappearing:[]})
  assert.deepEqual(plan({e4:'wP',d2:'wP'},{e2:'wP',d2:'wP'}), {moves:[{from:'e4',to:'e2'}],appearing:[],disappearing:[]})
})
test('captures fade the captured piece, undo restores it', () => {
  assert.deepEqual(plan({c4:'wB',f7:'bP'},{f7:'wB'}), {moves:[{from:'c4',to:'f7'}],appearing:[],disappearing:['f7']})
  assert.deepEqual(plan({f7:'wB'},{c4:'wB',f7:'bP'}), {moves:[{from:'f7',to:'c4'}],appearing:['f7'],disappearing:[]})
})
test('castling moves both king and rook in either direction', () => {
  assert.deepEqual(plan({e1:'wK',h1:'wR'},{g1:'wK',f1:'wR'}).moves, [{from:'e1',to:'g1'},{from:'h1',to:'f1'}])
  assert.deepEqual(plan({c8:'bK',d8:'bR'},{e8:'bK',a8:'bR'}).moves, [{from:'c8',to:'e8'},{from:'d8',to:'a8'}])
})
test('en passant fades the pawn on its own square', () => {
  assert.deepEqual(plan({e5:'wP',d5:'bP'},{d6:'wP'}), {moves:[{from:'e5',to:'d6'}],appearing:[],disappearing:['d5']})
})
test('promotion and reverse promotion remain one moving piece, including captures', () => {
  assert.deepEqual(plan({a7:'wP',b8:'bR'},{b8:'wN'}), {moves:[{from:'a7',to:'b8'}],appearing:[],disappearing:['b8']})
  assert.deepEqual(plan({b8:'wN'},{a7:'wP',b8:'bR'}), {moves:[{from:'b8',to:'a7'}],appearing:['b8'],disappearing:[]})
  assert.deepEqual(plan({h2:'bP'},{h1:'bQ'}).moves, [{from:'h2',to:'h1'}])
})
test('each old piece can only supply one new piece', () => {
  const result = plan({a2:'wP',h2:'wP'},{a4:'wP',h4:'wP',d4:'wP'})
  assert.equal(new Set(result.moves.map(move=>move.from)).size, 2)
  assert.equal(result.appearing.length, 1)
})
