import assert from 'node:assert/strict'
import test from 'node:test'
import { duration } from '../src/pages/research/algorithms/algorithmForm.js'
import { blankBuilder, draftSchemas, inputColumns, nextStepId, openBuilder, relationshipsTemplate } from '../src/pages/research/algorithms/builderForm.js'

const matrix = { id: 'a', name: 'Test', config: { row_type: 'game_player', feature_columns: ['rating', 'opponent_rating', 'mode'], label_columns: ['result'], filters: { max_rows: 100 } } }
const catalog = { row_types: [{ key: 'game_player', columns: ['rating', 'opponent_rating', 'result'].map((key) => ({ key, data_type: 'number' })).concat({ key: 'mode', data_type: 'category' }) }] }
test('typed selected columns become inputs, blank builder has no predefined algorithm', () => {
  assert.deepEqual(inputColumns(matrix, catalog).map((item) => item.key), ['rating', 'opponent_rating', 'result', 'mode'])
  const form = blankBuilder(matrix, catalog)
  assert.equal(form.max_rows, 100)
  assert.deepEqual(form.steps, [])
  assert.deepEqual(form.outputs, [])
  assert.equal(form.operation, 'pipeline')
})
test('template is editable and outputs refer to independent steps', () => {
  const form = blankBuilder(matrix, catalog)
  const template = relationshipsTemplate(form, draftSchemas(form, matrix, catalog).input)
  assert.equal(template.steps.length, 2)
  assert.equal(template.outputs.length, 3)
  assert.equal(template.outputs[2].input, 'input')
  assert.deepEqual(form.steps, [])
})
test('opening preserves saved definitions and retains lineage and frozen matrix', () => {
  const definition = { id: 'previous', name: 'Old run', matrix_definition_id: 'a', config: {
    operation: 'feature_relationships', matrix: matrix.config, columns: ['rating', 'opponent_rating'],
    missing: 'drop_rows', scaling: 'standardize', rating_difference: true,
    x: 'rating_difference', y: 'rating', max_rows: 200000, seed: 42,
  } }
  const before = structuredClone(definition)
  const opened = openBuilder(definition, catalog)
  assert.equal(opened.form.parent_definition_id, 'previous')
  assert.equal(opened.form.max_rows, 100000)
  assert.equal(opened.form.outputs[2].input, 'scaled')
  assert.deepEqual(definition, before)
  assert.equal(nextStepId([{ id: 'step_1' }, { id: 'step_3' }]), 'step_2')
  assert.equal(duration(3661), '1:01:01')
})
