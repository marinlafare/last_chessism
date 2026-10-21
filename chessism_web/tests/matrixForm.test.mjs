import assert from 'node:assert/strict'
import { test } from 'node:test'
import { createMatrixForm, formFromDefinition, matrixFormPayload, mergeDefinitions, moveScoreColumns, toggleFormColumn } from '../src/pages/research/matrices/matrixForm.js'

test('form defaults and copies do not share mutable collections', () => {
  const first = createMatrixForm()
  first.features.push('rating')
  assert.deepEqual(createMatrixForm().features, [])
  const config = matrixFormPayload({ ...createMatrixForm(), features: ['moves'], labels: ['result'] })
  const copy = formFromDefinition({ name: 'test', config })
  copy.features.push('rating')
  assert.deepEqual(config.feature_columns, ['moves'])
  assert.equal(Object.hasOwn(config, 'filters'), false)
  assert.equal(copy.name, 'test copy')
})

test('saved formulas specify columns; player, date and row scope belong to execution', () => {
  const form = createMatrixForm()
  assert.deepEqual(matrixFormPayload(form), { name: 'Custom game matrix', definition_kind: 'template',
    row_type: 'game', feature_columns: [], label_columns: [] })
  const scores = matrixFormPayload({ ...form, rowTypeKey: 'move_scores' })
  assert.deepEqual(scores.move_scores, { all_moves: true, first_move: 1, last_move: null, score_perspective: 'player' })
})

test('columns cannot remain both a feature and a label', () => {
  let form = { ...createMatrixForm(), features: ['moves'], labels: ['result'] }
  form = toggleFormColumn(form, 'moves', 'label')
  assert.deepEqual(form.features, [])
  assert.deepEqual(form.labels, ['result', 'moves'])
  form = toggleFormColumn(form, 'moves', 'feature')
  assert.deepEqual(form.features, ['moves'])
  assert.deepEqual(form.labels, ['result'])
})

test('move score columns retain identity and validate explicit move ranges', () => {
  assert.deepEqual(moveScoreColumns({ all_moves: true }), ['player', 'game', 'result'])
  assert.deepEqual(moveScoreColumns({ first_move: 2, last_move: 3 }), ['player', 'game', 'result', 'move_score_2', 'move_score_3'])
  assert.deepEqual(moveScoreColumns({ first_move: 3, last_move: 2 }), [])
  assert.deepEqual(moveScoreColumns({ first_move: 1, last_move: 501 }), [])
})

test('definition pages preserve ordering without duplicate cards', () => {
  assert.deepEqual(mergeDefinitions([{ id: 'a' }, { id: 'b' }], [{ id: 'b' }, { id: 'c' }]), [{ id: 'a' }, { id: 'b' }, { id: 'c' }])
})
