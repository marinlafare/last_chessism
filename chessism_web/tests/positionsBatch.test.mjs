import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  clampAnalysisBatchInput,
  MAX_ANALYSIS_BATCH_SIZE,
  MAX_GLOBAL_ANALYSIS_BATCH_SIZE,
} from '../src/pages/positions/positionPageSupport.js'

test('Analyze All accepts integers through 1000', () => {
  assert.equal(MAX_GLOBAL_ANALYSIS_BATCH_SIZE, 1000)
  for (const value of ['1', '500', '501', '999', '1000']) {
    assert.equal(clampAnalysisBatchInput(value, MAX_GLOBAL_ANALYSIS_BATCH_SIZE), Number(value))
  }
  assert.equal(clampAnalysisBatchInput('1001', MAX_GLOBAL_ANALYSIS_BATCH_SIZE), 1000)
  assert.equal(clampAnalysisBatchInput('999.9', MAX_GLOBAL_ANALYSIS_BATCH_SIZE), 999)
  assert.equal(clampAnalysisBatchInput('', MAX_GLOBAL_ANALYSIS_BATCH_SIZE), '')
})

test('regular player batch limit and default remain 500', () => {
  assert.equal(MAX_ANALYSIS_BATCH_SIZE, 500)
  assert.equal(clampAnalysisBatchInput('1000'), 500)
})
