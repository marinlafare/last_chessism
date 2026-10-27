import assert from 'node:assert/strict'
import { test } from 'node:test'
import { combineScoreModes } from '../src/pages/players/scoreDistribution.js'
import { evaluationLabel, evaluationDescription } from '../src/pages/players/gameEvaluationLabel.js'

test('game review shows distance and perspective without inventing missing distances', () => {
  const evaluation = {kind:'mate',white_cp:-9996,mate_moves:4}
  assert.equal(evaluationLabel(evaluation),'M−4')
  assert.equal(evaluationLabel(evaluation,'black'),'M+4')
  assert.equal(evaluationDescription(evaluation),'Mate in 4 against White')
  assert.equal(evaluationLabel({...evaluation,mate_moves:0}),'CHECKMATE')
  assert.equal(evaluationLabel({...evaluation,mate_moves:null}),'M−?')
  assert.match(evaluationDescription({...evaluation,mate_moves:null}),/distance unavailable/)
  assert.equal(evaluationLabel({kind:'tablebase',white_cp:9998}),'TB WIN')
  assert.equal(evaluationLabel({kind:'cp',white_cp:50}),'+0.50')
})

test('mate counters combine selected modes, both perspectives and unknown distances', () => {
  const row = mates => ({ counts:[2],endings:{games:1},cp_positions:2,mate_positions:4,
    tablebase_positions:1,unscored_positions:3,mates })
  const data = { bins:[{}], by_mode:{blitz:row([{moves:2,side:'for',positions:3},{moves:null,side:'against',positions:1}]),
    rapid:row([{moves:2,side:'against',positions:4}])} }
  const all = combineScoreModes(data,['blitz','rapid'])
  assert.deepEqual(all.mateDistances,[{moves:2,for:3,against:4},{moves:null,for:0,against:1}])
  assert.equal(all.mates,8)
  assert.deepEqual(combineScoreModes(data,['rapid']).mateDistances,[{moves:2,for:0,against:4}])
  assert.deepEqual(combineScoreModes(data,[]).mateDistances,[])
})
