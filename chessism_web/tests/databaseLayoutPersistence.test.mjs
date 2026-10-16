import test from 'node:test'
import assert from 'node:assert/strict'
import { legacyLayoutStorageKey, savedLayoutStorageKey, readSavedLayout, writeSavedLayout } from '../src/pages/database/layoutPersistence.js'
import { normalizeLayout } from '../src/pages/database/workspaceGeometry.js'

const table = { id: 'public.game', name: 'game', schema: 'public', columns: [] }
const storage = () => {
  const data = new Map(), writes = []
  return { data, writes, getItem: key => data.get(key) ?? null, setItem: (key, value) => { writes.push(key); data.set(key, value) } }
}

test('saved layouts are separate from the default, and reading never writes or migrates old arrangements', () => {
  const store = storage(), old = { version: 1, positions: { 'public.game': { x: -400, y: 300 } }, view: { x: 10, y: 20, zoom: 1 } }
  store.data.set(legacyLayoutStorageKey('one'), JSON.stringify(old))
  assert.deepEqual(readSavedLayout(store, 'one'), old)
  assert.deepEqual(store.writes, [])
  assert.notDeepEqual(normalizeLayout(null, [table]).positions, old.positions)
  const personal = normalizeLayout({ ...old, expanded: ['public.game'] }, [table])
  writeSavedLayout(store, 'one', personal)
  assert.deepEqual(store.writes, [savedLayoutStorageKey('one')])
  assert.deepEqual(JSON.parse(store.data.get(legacyLayoutStorageKey('one'))), old)
  assert.deepEqual(readSavedLayout(store, 'one'), { ...personal, autoFrame: false })
  assert.equal(readSavedLayout(store, 'two'), null)
  assert.notEqual(savedLayoutStorageKey('a/b'), savedLayoutStorageKey('a%2Fb'))
})

test('bad storage is reported and failed saves cannot replace the last good layout', () => {
  const store = storage(), personal = normalizeLayout(null, [table])
  writeSavedLayout(store, 'one', personal)
  const before = store.data.get(savedLayoutStorageKey('one'))
  assert.throws(() => writeSavedLayout({ ...store, setItem: () => { throw Error('Quota exceeded') } }, 'one', { ...personal, expanded: ['public.game'] }))
  assert.equal(store.data.get(savedLayoutStorageKey('one')), before)
  for (const invalid of ['bad json', JSON.stringify({ version: 9, layout: personal }), JSON.stringify({ version: 1, layout: { version: 1, positions: [] } })]) {
    store.data.set(savedLayoutStorageKey('one'), invalid)
    assert.throws(() => readSavedLayout(store, 'one'))
  }
  assert.throws(() => readSavedLayout({ getItem: () => { throw Error('Blocked') } }, 'one'))
  assert.throws(() => writeSavedLayout(store, 'one', {}))
})
