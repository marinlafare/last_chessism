import test from 'node:test'
import assert from 'node:assert/strict'
import { CARD_WIDTH, CARD_HEIGHT, HEADER_HEIGHT, DEFAULT_VIEW, MIN_ZOOM, MAX_ZOOM, defaultPositions, normalizeLayout,
  zoomAt, fitTables, fitBounds, workspaceBounds, tableBounds, centerTable, inViewport, relatedTables, relationshipPath, searchTables } from '../src/pages/database/workspaceGeometry.js'

const tables = ['public.game', 'public.fen', 'public.game_fen'].map(id => ({ id, columns: [{ name: `${id.split('.')[1]}_id` }] }))
test('layout validation bounds browser data, retains saved positions and reconciles schema changes', () => {
  const fresh = normalizeLayout(null, tables)
  assert.deepEqual(fresh.positions, defaultPositions(tables))
  assert.deepEqual(fresh.expanded, [])
  const saved = { ...fresh, positions: { ...fresh.positions, 'public.game': { x: 123, y: 456 }, deleted: { x: 5, y: 5 } } }
  const reconciled = normalizeLayout(saved, [...tables, { id: 'public.new_table' }])
  assert.deepEqual(reconciled.positions['public.game'], { x: 123, y: 456 })
  assert.ok(reconciled.positions['public.new_table'])
  assert.ok(reconciled.positions['public.new_table'].y >= saved.positions['public.game'].y + CARD_HEIGHT)
  assert.ok(!reconciled.positions.deleted)
  assert.deepEqual(normalizeLayout({ version: 8 }, tables), fresh)
  const bad = normalizeLayout({ version: 1, view: { x: Infinity, y: -1e9, zoom: 'evil' }, positions: { 'public.game': { x: NaN, y: 1e9 } } }, tables)
  assert.equal(bad.view.x, DEFAULT_VIEW.x)
  assert.equal(bad.view.y, -200000)
  assert.equal(bad.view.zoom, DEFAULT_VIEW.zoom)
  assert.equal(bad.positions['public.game'].y, 100000)
  assert.deepEqual(normalizeLayout({ ...fresh, expanded: ['public.game', 'deleted', 'public.game', null] }, tables).expanded, ['public.game'])
  assert.deepEqual(normalizeLayout({ ...fresh, expanded: 'invalid' }, tables).expanded, [])
})
test('workspace adds three layout spans in every direction without moving saved tables', () => {
  const positions = defaultPositions(Array.from({ length: 160 }, (_, i) => ({ id: `table_${i}` })))
  const original = structuredClone(positions), bounds = tableBounds(positions)
  const field = workspaceBounds(positions, positions)
  const width = bounds.right - bounds.left, height = bounds.bottom - bounds.top
  assert.equal(field.left, bounds.left - width * 3)
  assert.equal(field.right, bounds.right + width * 3)
  assert.equal(field.top, bounds.top - height * 3)
  assert.equal(field.bottom, bounds.bottom + height * 3)
  assert.equal(field.width, width * 7)
  assert.equal(field.height, height * 7)
  assert.deepEqual(positions, original)
  const moved = { ...positions, table_0: { x: -50000, y: -50000 }, table_1: { x: 50000, y: 50000 } }
  const extended = workspaceBounds(positions, moved, ['table_1'])
  assert.ok(extended.left < moved.table_0.x && extended.top < moved.table_0.y)
  assert.ok(extended.right > moved.table_1.x + CARD_WIDTH && extended.bottom > moved.table_1.y + CARD_HEIGHT)
  const size = { width: 1200, height: 700 }, view = fitBounds(field, size)
  assert.ok(field.left * view.zoom + view.x >= 0 && field.right * view.zoom + view.x <= size.width)
  assert.ok(field.top * view.zoom + view.y >= 0 && field.bottom * view.zoom + view.y <= size.height)
  assert.ok(Number.isFinite(workspaceBounds({}, {}).width))
})
test('collapsed tables use only header height in fit and culling', () => {
  const positions = { one: { x: -200, y: -200 } }
  assert.equal(tableBounds(positions).bottom, -200 + HEADER_HEIGHT)
  assert.equal(tableBounds(positions, ['one']).bottom, -200 + CARD_HEIGHT)
  const view = { x: 0, y: 0, zoom: 1 }, size = { width: 1000, height: 600 }
  assert.equal(inViewport(positions.one, view, size, false), false)
  assert.equal(inViewport(positions.one, view, size, true), true)
})
test('zoom is cursor-anchored and bounded; fit and center preserve visibility', () => {
  const point = { x: 300, y: 200 }, view = { x: -50, y: 80, zoom: .5 }
  const zoomed = zoomAt(view, 1.3, point)
  assert.equal((point.x - zoomed.x) / zoomed.zoom, (point.x - view.x) / view.zoom)
  assert.equal((point.y - zoomed.y) / zoomed.zoom, (point.y - view.y) / view.zoom)
  assert.equal(zoomAt(view, 99, point).zoom, MAX_ZOOM)
  assert.equal(zoomAt(view, 0, point).zoom, MIN_ZOOM)
  const positions = defaultPositions(tables), size = { width: 1200, height: 700 }
  const fit = fitTables(positions, size)
  for (const pos of Object.values(positions)) assert.ok(inViewport(pos, fit, size))
  const centered = centerTable(positions['public.fen'], size)
  assert.equal(centered.x + (positions['public.fen'].x + CARD_WIDTH / 2) * centered.zoom, size.width / 2)
  assert.equal(centered.y + (positions['public.fen'].y + CARD_HEIGHT / 2) * centered.zoom, size.height / 2)
  assert.ok(!inViewport({ x: 50000, y: 50000 }, DEFAULT_VIEW, size))
  assert.deepEqual(fitTables({}, size), DEFAULT_VIEW)
})
test('search matches qualified tables or columns; selected links include incoming/outgoing and self', () => {
  assert.equal(searchTables(tables, 'GAME_ID')[0].id, 'public.game')
  assert.equal(searchTables(tables, ' PUBLIC.FEN ')[0].id, 'public.fen')
  assert.deepEqual(searchTables(tables, ''), [])
  const links = [{ source_table: 'a', target_table: 'b' }, { source_table: 'c', target_table: 'a' }, { source_table: 'a', target_table: 'a' }, { source_table: 'd', target_table: 'e' }]
  assert.deepEqual([...relatedTables('a', links)].sort(), ['a', 'b', 'c'])
  assert.equal(relatedTables(null, links).size, 0)
  assert.ok(!relationshipPath({ x: -20, y: 40 }, { x: -20, y: 40 }, true).includes('NaN'))
})
