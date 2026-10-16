import test from 'node:test'
import assert from 'node:assert/strict'
import { TABLE_GROUPS, HIERARCHY_PRESET, CARD_WIDTH, CARD_HEIGHT, buildDatabaseHierarchy, regionsForPositions, tableGroup } from '../src/pages/database/workspaceHierarchy.js'
import { normalizeLayout } from '../src/pages/database/workspaceGeometry.js'

const table = (name, schema = 'public') => ({ id: `${schema}.${name}`, name, schema, columns: [] })
const tables = [...TABLE_GROUPS.flatMap(group => group.names), 'cloud_control_config', 'cloud_control_record', 'future_table'].map(name => table(name))
const overlaps = (a, b) => a.x < b.x + b.width && a.x + a.width > b.x && a.y < b.y + b.height && a.y + a.height > b.y

test('game-centered hierarchy places every table exactly once, deterministically, without expanded-card or group collisions', () => {
  const result = buildDatabaseHierarchy(tables)
  assert.equal(Object.keys(result.positions).length, tables.length)
  assert.deepEqual(result, buildDatabaseHierarchy([...tables].reverse()))
  assert.ok(result.positions['public.game'].y < result.positions['public.player'].y)
  assert.ok(result.positions['public.player'].x < result.positions['public.game'].x)
  assert.equal(result.positions['public.moves'].x, result.positions['public.game'].x)
  assert.ok(result.positions['public.fen'].x > result.positions['public.game'].x)
  assert.ok(result.positions['public.game_fen_association'].y < result.positions['public.fen'].y)
  assert.ok(result.positions['public.fen'].y < result.positions['public.fen_continuation'].y)
  for (const [summary, source] of [['game_analysis_summary', 'game_opening'], ['player_salience_summary', 'player'], ['fen_appearance_summary', 'fen_continuation']]) {
    assert.ok(result.positions[`public.${summary}`].y > result.positions[`public.${source}`].y)
  }
  assert.ok(result.positions['public.matrix_definition'].x < result.positions['public.player'].x)
  assert.ok(result.positions['public.cloud_analysis_job'].x > result.positions['public.fen'].x)
  assert.ok(result.positions['public.account'].y > result.positions['public.fen_appearance_summary'].y)
  const cards = Object.entries(result.positions).map(([id, point]) => ({ id, ...point, width: CARD_WIDTH, height: CARD_HEIGHT }))
  for (let i = 0; i < cards.length; i++) for (let j = i + 1; j < cards.length; j++) {
    assert.ok(!overlaps(cards[i], cards[j]), `${cards[i].id} overlaps ${cards[j].id}`)
  }
  for (let i = 0; i < result.regions.length; i++) for (let j = i + 1; j < result.regions.length; j++) {
    assert.ok(!overlaps(result.regions[i], result.regions[j]), `${result.regions[i].id} overlaps ${result.regions[j].id}`)
  }
  for (const card of cards) {
    const region = result.regions.find(region => region.id === result.tableGroups[card.id])
    assert.ok(card.x >= region.x && card.y >= region.y + 50)
    assert.ok(card.x + card.width <= region.x + region.width && card.y + card.height <= region.y + region.height)
  }
})

test('classification covers known families and separates unknown tables and other schemas without inventing relationships', () => {
  assert.equal(tableGroup(table('cloud_control_receipts')).id, 'cloud-control')
  assert.equal(tableGroup(table('cloud_new_history')).id, 'cloud-jobs')
  assert.equal(tableGroup(table('algorithm_new_output')).id, 'research-results')
  assert.equal(tableGroup(table('home_new_summary')).id, 'dashboard')
  assert.equal(tableGroup(table('fen_commonness_new')).id, 'position-summaries')
  assert.equal(tableGroup(table('future_table')).id, 'other')
  assert.equal(tableGroup(table('game', 'archive')).id, 'other')
  const result = buildDatabaseHierarchy([table('game'), table('game', 'archive')])
  assert.notDeepEqual(result.positions['public.game'], result.positions['archive.game'])
  assert.ok(!('relationships' in result))
  assert.deepEqual(buildDatabaseHierarchy([]).regions, [])
  assert.equal(buildDatabaseHierarchy([table('future_table')]).overviewBounds, null)
})

test('explicitly loading an old arrangement preserves its positions and view; fresh layouts use the hierarchy', () => {
  const old = { version: 1, positions: { 'public.game': { x: -900, y: 456 } }, expanded: ['public.game'], view: { x: 85, y: 65, zoom: .5 } }
  const preserved = normalizeLayout(old, tables)
  assert.deepEqual(preserved.positions['public.game'], old.positions['public.game'])
  assert.deepEqual(preserved.view, old.view)
  assert.deepEqual(preserved.expanded, ['public.game'])
  assert.equal(preserved.preset, null)
  assert.equal(preserved.autoFrame, false)
  assert.ok(preserved.regions.length > 0)
  const fresh = normalizeLayout(null, tables)
  assert.equal(fresh.preset, HIERARCHY_PRESET)
  assert.equal(fresh.autoFrame, true)
  assert.deepEqual(fresh.expanded, [])
  assert.ok(fresh.regions.length > 10)
})

test('manual moves and expansions survive catalog refreshes while group outlines follow their tables', () => {
  const fresh = normalizeLayout(null, tables)
  const moved = { ...fresh, autoFrame: false, positions: { ...fresh.positions, 'public.game': { x: 12000, y: 15000 } }, expanded: ['public.game'] }
  const next = normalizeLayout(moved, [...tables, table('new_table')])
  assert.deepEqual(next.positions['public.game'], moved.positions['public.game'])
  const gameRegion = next.regions.find(region => region.id === 'games')
  assert.equal(gameRegion.x, 12000 - 24)
  assert.equal(gameRegion.y, 15000 - 56)
  assert.deepEqual(next.expanded, moved.expanded)
  assert.equal(next.autoFrame, false)
  assert.ok(next.positions['public.new_table'].y > 15000 + CARD_HEIGHT)
  const bad = normalizeLayout({ ...fresh, regions: [{ id: 'evil', x: 1, y: 1, width: 350, height: 300 }, { ...fresh.regions[0], x: Infinity }, fresh.regions[1], fresh.regions[1]] }, tables)
  assert.deepEqual(bad.regions, fresh.regions)
})

test('single-table boxes translate with their table; multi-table boxes resize to keep every member inside', () => {
  const fresh = buildDatabaseHierarchy(tables)
  const before = regionsForPositions(tables, fresh.positions)
  const moved = { ...fresh.positions,
    'public.game': { x: fresh.positions['public.game'].x - 60, y: fresh.positions['public.game'].y + 40 },
    'public.player': { x: -300, y: -100 },
  }
  const after = regionsForPositions(tables, moved)
  const oldGame = before.find(region => region.id === 'games'), game = after.find(region => region.id === 'games')
  assert.equal(game.x, oldGame.x - 60); assert.equal(game.y, oldGame.y + 40)
  assert.equal(game.width, oldGame.width); assert.equal(game.height, oldGame.height)
  const playerBox = after.find(region => region.id === 'players')
  for (const name of ['player', 'game_player', 'player_stats', 'months']) {
    const point = moved[`public.${name}`]
    assert.ok(point.x >= playerBox.x && point.y >= playerBox.y)
    assert.ok(point.x + CARD_WIDTH <= playerBox.x + playerBox.width && point.y + CARD_HEIGHT <= playerBox.y + playerBox.height)
  }
  assert.deepEqual(after.find(region => region.id === 'positions'), before.find(region => region.id === 'positions'))
})
