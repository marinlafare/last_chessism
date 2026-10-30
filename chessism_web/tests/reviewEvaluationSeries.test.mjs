import test from 'node:test'
import assert from 'node:assert/strict'
import { reviewPlotValue, reviewEvaluationSeries } from '../src/pages/players/reviewEvaluationSeries.js'
import { evaluationDescription } from '../src/pages/players/gameEvaluationLabel.js'

test('evaluation plot preserves White perspective and bounds CP without altering the actual score', () => {
  assert.equal(reviewPlotValue({ kind:'cp', white_cp:125 }), 125)
  assert.equal(reviewPlotValue({ kind:'cp', white_cp:-150 }), -150)
  const evaluation = { kind:'cp', white_cp:999 }
  assert.equal(reviewPlotValue(evaluation), 500)
  assert.equal(evaluationDescription(evaluation), '+9.99')
  assert.equal(reviewPlotValue(null), null)
  assert.equal(reviewPlotValue({ kind:'cp', white_cp:null }), null)
  assert.equal(reviewPlotValue({ kind:'unknown', white_cp:0 }), null)
})
test('mate and tablebase encodings are plotted as outcomes, with exact original descriptions', () => {
  const mate = { kind:'mate', white_cp:-9998, mate_moves:2 }
  assert.equal(reviewPlotValue(mate), -500)
  assert.equal(evaluationDescription(mate), 'Mate in 2 against White')
  assert.equal(reviewPlotValue({ kind:'tablebase', white_cp:0 }), 0)
  assert.equal(reviewPlotValue({ kind:'tablebase', white_cp:10000 }), 500)
})
test('missing evaluations break the line instead of inventing evaluations or joining across gaps', () => {
  const moves = [0, 100, null, -100, 50].map((cp, index) => ({ move_number:Math.ceil((index+1)/2),
    evaluation_after:cp===null ? null : { kind:'cp', white_cp:cp } }))
  const series = reviewEvaluationSeries(moves, 360)
  assert.deepEqual(series.segments.map(segment=>segment.map(point=>point.index)), [[1,2],[4,5]])
  assert.equal(series.points[0].y, series.zero)
  assert.equal(series.points[0].x, series.left)
  assert.equal(series.points.at(-1).x, series.right)
  assert.equal(series.points[2].y, null)
  assert.equal(reviewEvaluationSeries([]).segments.length, 0)
  assert.ok(Number.isFinite(reviewEvaluationSeries(moves.slice(0,1)).points[0].x))
})
