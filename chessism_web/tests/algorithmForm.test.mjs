import assert from 'node:assert/strict'
import test from 'node:test'
import { newAlgorithmForm, numericColumns, updateAlgorithmForm, duration } from '../src/pages/research/algorithms/algorithmForm.js'

const matrix = { id: 'a', name: 'Test', config: { row_type: 'game_player', feature_columns: ['rating', 'opponent_rating', 'mode'], label_columns: ['result'], filters: { max_rows: 100 } } }
const catalog = { row_types: [{ key: 'game_player', columns: ['rating', 'opponent_rating', 'result'].map((key) => ({ key, data_type: 'number' })).concat({ key: 'mode', data_type: 'category' }) }] }
test('only selected numerical columns become inputs; row cap is inherited', () => {
  assert.deepEqual(numericColumns(matrix, catalog).map((item) => item.key), ['rating', 'opponent_rating', 'result'])
  assert.equal(newAlgorithmForm(matrix, catalog).max_rows, 100)
})
test('changing columns clears invalid derived columns and scatter axes', () => {
  const form = { ...newAlgorithmForm(matrix, catalog), rating_difference: true, x: 'rating_difference' }
  const next = updateAlgorithmForm(form, { columns: ['rating', 'result'] })
  assert.equal(next.rating_difference, false)
  assert.equal(next.x, 'rating'); assert.equal(next.y, 'result')
})
test('changing X cannot leave both scatter axes the same', () => {
  const form = newAlgorithmForm(matrix, catalog)
  const next = updateAlgorithmForm(form, { x: form.y })
  assert.notEqual(next.x, next.y)
  assert.equal(duration(3661), '1:01:01')
})
