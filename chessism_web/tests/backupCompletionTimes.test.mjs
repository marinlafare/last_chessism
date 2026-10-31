import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { backupCompletionTimes } from '../src/pages/backups/backupCompletionTimes.js'
import { appearanceCss } from '../src/appearance/styleCore.js'

const first = '2026-10-13T03:00:00.000Z'
const second = '2026-10-20T04:00:00.000Z'
const third = '2026-10-22T05:00:00.000Z'

test('timestamps use latest completed records, even when the newest backup is not tested', () => {
  assert.deepEqual(backupCompletionTimes({ backups: [
    { completed_at: first, restore_tested_at: second },
    { completed_at: third },
  ] }), { backup: third, test: second })
})

test('running or failed backups do not replace the last usable backup date', () => {
  for (const status of ['running', 'failed']) {
    assert.deepEqual(backupCompletionTimes({
      status: { status, last_verified_at: first, completed_at: third },
      restore_test: { status: 'running', completed_at: third },
      backups: [{ completed_at: first, restore_tested_at: second }],
    }), { backup: first, test: second })
  }
})

test('a finished test updates its date even if it failed; successful status can supply missing catalog dates', () => {
  assert.deepEqual(backupCompletionTimes({
    status: { status: 'success', completed_at: second },
    restore_test: { status: 'failed', completed_at: third },
  }), { backup: second, test: third })
})

test('missing or invalid timestamps never produce a fake date', () => {
  assert.deepEqual(backupCompletionTimes(null), { backup: null, test: null })
  assert.deepEqual(backupCompletionTimes({ backups: [{ completed_at: 'bad', restore_tested_at: '' }] }), { backup: null, test: null })
})

test('summary and both badges have independent page-scoped normal and hover styles', () => {
  const read = name => JSON.parse(readFileSync(new URL(`../../chessism_api/${name}.json`, import.meta.url)))
  const base = read('appearance_registry'), scopes = read('appearance_scopes')
  const registry = { ...base, scopes: scopes.scopes }
  for (const target of ['summary.panel', 'backup.last', 'restore.last']) {
    for (const property of ['background', 'hoverBackground', 'borderColor', 'hoverBorderColor', 'borderWidth', 'radius', 'padding']) {
      assert.ok(registry.scopes.backups.targets[target].properties.includes(property))
    }
    const row = { scope: 'backups', target, property: 'hoverBackground', value: '#123456' }
    const css = appearanceCss(registry, [row])
    assert.ok(css.includes(`[data-style-scope="backups"][data-style-id="${target}"]`))
    assert.ok(css.includes(':hover{background-color:#123456;'))
    assert.equal(appearanceCss(registry, [{ ...row, scope: 'home' }]), '')
  }
})
