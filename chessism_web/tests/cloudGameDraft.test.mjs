import test from 'node:test'
import assert from 'node:assert/strict'
import { draftEntryForm, draftSubmitError, draftTaskSummary, draftVmPlan, hasSavedCloudDraft } from '../src/pages/positions/cloudGameDraft.js'

const draft = () => ({ id: 'a', revision: 2, entries: [{ id: 'b' }], max_fens: 10000000,
  preview: { revision: 2, fens_to_analyze: 1300000 } })

test('only nonempty unsubmitted drafts auto-open, even before a combined preview', () => {
  assert.equal(hasSavedCloudDraft(draft()), true)
  assert.equal(hasSavedCloudDraft({ ...draft(), preview: null }), true)
  for (const value of [null, {}, { ...draft(), entries: [] }, { ...draft(), submitted_job_id: 'sent' }]) {
    assert.equal(hasSavedCloudDraft(value), false)
  }
})

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

test('VM preview mirrors full cycles and evenly splits each cycle, preserving the FEN total', () => {
  assert.deepEqual(draftVmPlan(2616289, 1), [
    { cycle: 1, total: 1000000, vmFens: [1000000] },
    { cycle: 2, total: 1000000, vmFens: [1000000] },
    { cycle: 3, total: 616289, vmFens: [616289] },
  ])
  assert.deepEqual(draftVmPlan(2616289, '2'), [
    { cycle: 1, total: 2000000, vmFens: [1000000, 1000000] },
    { cycle: 2, total: 616289, vmFens: [308145, 308144] },
  ])
  assert.deepEqual(draftVmPlan(2000000, 2), [
    { cycle: 1, total: 2000000, vmFens: [1000000, 1000000] },
  ])
  assert.deepEqual(draftVmPlan(2, 10), [{ cycle: 1, total: 2, vmFens: [1, 1] }])
  assert.deepEqual(draftVmPlan(4000001, 2).at(-1), { cycle: 3, total: 1, vmFens: [1] })
  for (const count of [1, 3, 500000, 1000001, 2616289, 10000000]) {
    for (let vms = 1; vms <= 10; vms++) {
      const plan = draftVmPlan(count, vms)
      assert.equal(plan.reduce((sum, row) => sum + row.total, 0), count)
      for (const row of plan) {
        assert.equal(row.vmFens.reduce((sum, n) => sum + n, 0), row.total)
        assert.ok(row.vmFens.length <= vms)
        assert.ok(row.vmFens.every(n => n >= 1 && n <= 1000000))
        assert.ok(row.vmFens[0] - row.vmFens.at(-1) <= 1)
      }
    }
  }
})

test('invalid or oversized selections do not render a misleading or unbounded VM plan', () => {
  for (const count of [undefined, null, 0, -1, 1.5, '1000', NaN, Infinity, 10000001]) {
    assert.equal(draftVmPlan(count, 2), null)
  }
  for (const vms of [undefined, null, '', 0, -1, 11, 1.5, 'bad', Infinity]) {
    assert.equal(draftVmPlan(1000, vms), null)
  }
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
