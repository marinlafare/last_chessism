import assert from 'node:assert/strict'
import { test } from 'node:test'
import { createMatrixForm, formFromDefinition, matrixFormPayload, mergeDefinitions, toggleFormColumn, toggleFormMode } from '../src/pages/research/matrices/matrixForm.js'

test('form defaults and copies do not share mutable collections', () => {
  const first = createMatrixForm()
  first.filters.modes.pop()
  assert.equal(createMatrixForm().filters.modes.length, 3)
  const config = matrixFormPayload({ ...createMatrixForm(), features: ['moves'], labels: ['result'] })
  const copy = formFromDefinition({ name: 'test', config })
  copy.features.push('rating')
  copy.filters.modes.pop()
  assert.deepEqual(config.feature_columns, ['moves'])
  assert.equal(config.filters.modes.length, 3)
  assert.equal(copy.name, 'test copy')
})

test('payload conversion handles dates and preserves invalid zero for API validation', () => {
  const form = createMatrixForm()
  form.filters.players = ' lafareto , , hikaru '
  form.filters.max_rows = 0
  assert.deepEqual(matrixFormPayload(form).filters.players, ['lafareto', 'hikaru'])
  assert.equal(matrixFormPayload(form).filters.max_rows, 0)
  assert.equal(matrixFormPayload(form).filters.date_from, null)
  form.filters.max_rows = ''
  assert.equal(matrixFormPayload(form).filters.max_rows, 100000)
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

test('mode selection does not mutate the previous form', () => {
  const previous = createMatrixForm()
  const next = toggleFormMode(previous, 'rapid')
  assert.deepEqual(previous.filters.modes, ['bullet', 'blitz', 'rapid'])
  assert.deepEqual(next.filters.modes, ['bullet', 'blitz'])
})

test('definition pages preserve ordering without duplicate cards', () => {
  assert.deepEqual(mergeDefinitions([{ id: 'a' }, { id: 'b' }], [{ id: 'b' }, { id: 'c' }]), [{ id: 'a' }, { id: 'b' }, { id: 'c' }])
})
