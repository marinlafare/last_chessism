import test from 'node:test'
import assert from 'node:assert/strict'
import { draftEntryForm, draftSubmitError, draftTaskSummary } from '../src/pages/positions/cloudGameDraft.js'

const draft = () => ({ id: 'a', revision: 2, entries: [{ id: 'b' }], max_fens: 10000000,
  preview: { revision: 2, fens_to_analyze: 1300000 } })

test('combined submission requires current preview, positive count and valid VM concurrency', () => {
  assert.equal(draftSubmitError(draft(), 1), '')
  for (const row of [null, { ...draft(), entries: [] }, { ...draft(), preview: null },
    { ...draft(), revision: 3 }, { ...draft(), submitted_job_id: 'sent' },
    { ...draft(), preview: { revision: 2, fens_to_analyze: 0 } },
    { ...draft(), preview: { revision: 2, fens_to_analyze: 10000001 } }]) {
    assert.notEqual(draftSubmitError(row, 1), '')
  }
  for (const n of [0, 11, 1.5, 'bad']) assert.notEqual(draftSubmitError(draft(), n), '')
})

test('task sizes depend on VM count without changing the selected set', () => {
  assert.match(draftTaskSummary(1300000), /^1 × 1,000,000 \+ 300,000 FENs across 2 cycle/)
  assert.match(draftTaskSummary(1000000), /^1 × 1,000,000 FENs across 1 cycle/)
  assert.match(draftTaskSummary(10000), /^10,000 FENs across 1 cycle/)
  assert.match(draftTaskSummary(1963096, 2), /^1,963,096 FENs across 1 cycle.*2 parallel Spot VMs/)
  assert.match(draftTaskSummary(4000001, '2'), /^2 × 2,000,000 \+ 1 FENs across 3 cycle/)
  for (const n of [0,11,'',1.5]) assert.match(draftTaskSummary(1963096, n), /valid Spot VM/)
  assert.equal(draftTaskSummary(0), 'No cloud tasks.')
})

test('editing restores saved selection parameters, not another live game selection', () => {
  const form = draftEntryForm({ player_name: 'hikaru', selection_mode: 'range', selection_order: 'oldest',
    requested_games: null, date_from: '2025-01-01', date_to: '2025-12-31' })
  assert.equal(form.player, 'hikaru')
  assert.equal(form.allGames, true)
  assert.equal(form.order, 'oldest')
  assert.equal(form.from, '2025-01-01')
  assert.equal(draftEntryForm({ requested_games: 123 }).games, 123)
})
