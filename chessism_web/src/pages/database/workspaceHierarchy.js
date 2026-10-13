// A semantic map, not an inferred FK tree. Actual FK lines come from the catalog.
export const CARD_WIDTH = 300
export const CARD_HEIGHT = 300
export const HEADER_HEIGHT = 54
export const HIERARCHY_PRESET = 'game-centered-v1'
const GAP = 52, PADDING = 24, LABEL_HEIGHT = 56, SECTION_GAP = 80
const widthFor = columns => columns * CARD_WIDTH + (columns - 1) * GAP + PADDING * 2
const BRANCH_WIDTH = widthFor(1), BRANCH_STEP = BRANCH_WIDTH + SECTION_GAP
const CORE_WIDTH = BRANCH_WIDTH * 3 + SECTION_GAP * 2

export const TABLE_GROUPS = [
  { id: 'games', label: 'Games', family: 'chess', names: ['game'] },
  { id: 'players', label: 'Players', family: 'chess', names: ['player', 'game_player', 'player_stats', 'months'] },
  { id: 'details', label: 'Game details', family: 'chess', names: ['moves', 'game_opening'] },
  { id: 'positions', label: 'Positions', family: 'chess', names: ['game_fen_association', 'fen', 'fen_continuation'] },
  { id: 'player-summaries', label: 'Player calculations', family: 'derived', names: ['player_position_frequency', 'game_player_salience', 'player_salience_summary', 'main_character_mode_summary'] },
  { id: 'game-summaries', label: 'Game calculations', family: 'derived', names: ['game_analysis_summary', 'game_player_engine_summary'] },
  { id: 'position-summaries', label: 'Position calculations', family: 'derived', names: ['scored_position_summary', 'scored_rating_summary', 'fen_appearance_summary', 'fen_appearance_bin', 'fen_commonness_summary', 'fen_commonness_position', 'pending_repetition_summary', 'pending_repetition_bin'] },
  { id: 'dashboard', label: 'Dashboard summaries', family: 'derived', names: ['database_summary', 'home_summary_source', 'home_titled_summary', 'home_titled_player_summary'] },
  { id: 'research', label: 'Research · definitions', family: 'research', names: ['matrix_definition', 'algorithm_definition'] },
  { id: 'research-results', label: 'Research · artifacts and runs', family: 'research', names: ['matrix_artifact', 'algorithm_run', 'coefficient_research_experiment'] },
  { id: 'cloud-jobs', label: 'Processing · cloud jobs', family: 'processing', names: ['cloud_analysis_job', 'cloud_analysis_run', 'cloud_batch_sequence', 'cloud_batch_cycle', 'cloud_batch_unit'] },
  { id: 'cloud-selection', label: 'Processing · cloud selections', family: 'processing', names: ['cloud_game_draft', 'cloud_game_draft_entry', 'cloud_game_draft_game', 'cloud_fen_claim'] },
  { id: 'cloud-recovery', label: 'Processing · recovery', family: 'processing', names: ['cloud_sequence_control', 'cloud_execution_attempt', 'cloud_controller_heartbeat'] },
  { id: 'cloud-results', label: 'Processing · imports and usage', family: 'processing', names: ['cloud_result_batch', 'cloud_usage', 'cloud_phase_timing'] },
  { id: 'cloud-control', label: 'Processing · cloud-control documents', family: 'processing', names: ['cloud_control_document'] },
  { id: 'ingestion', label: 'Processing · ingestion and timing', family: 'processing', names: ['ingestion_pipeline_run', 'ingestion_pipeline_stage_timing', 'fen_pipeline_summary', 'player_salience_pending_game', 'no_moves_games', 'analysis_time', 'analysis_times'] },
  { id: 'accounts', label: 'Application · accounts', family: 'application', names: ['account', 'auth_session'] },
  { id: 'appearance', label: 'Application · appearance', family: 'application', names: ['appearance_preset', 'appearance_setting', 'appearance_override'] },
  { id: 'other', label: 'Other tables and views', family: 'other', names: [] },
]
const exactGroups = new Map(TABLE_GROUPS.flatMap(group => group.names.map(name => [name, group])))
const groupById = new Map(TABLE_GROUPS.map(group => [group.id, group]))
export const tableName = table => table.name || table.id.split('.').at(-1)

export function tableGroup(table) {
  const name = tableName(table)
  // Known names only describe the application's public schema. Same-name tables
  // in another schema stay separate rather than silently posing as core data.
  if (table.schema && table.schema !== 'public') return groupById.get('other')
  if (exactGroups.has(name)) return exactGroups.get(name)
  const prefix = name.startsWith('cloud_control_') ? 'cloud-control'
    : name.startsWith('cloud_') ? 'cloud-jobs'
    : /^(matrix_|algorithm_|coefficient_research_)/.test(name) ? 'research-results'
    : name.startsWith('appearance_') ? 'appearance'
    : name.startsWith('home_') ? 'dashboard'
    : /^(fen_appearance_|fen_commonness_|pending_repetition_|scored_)/.test(name) ? 'position-summaries'
    : name.startsWith('ingestion_') ? 'ingestion' : 'other'
  return groupById.get(prefix)
}

function ordered(tables, group) {
  const rank = table => { const index = group.names.indexOf(tableName(table)); return index < 0 ? Infinity : index }
  return [...tables].sort((a, b) => rank(a) - rank(b) || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))
}

export function buildDatabaseHierarchy(tables) {
  const members = new Map(TABLE_GROUPS.map(group => [group.id, []]))
  for (const table of tables) members.get(tableGroup(table).id).push(table)
  const positions = {}, regions = [], tableGroups = {}
  const place = (id, x, y, columns = 1) => {
    const group = groupById.get(id), list = ordered(members.get(id), group)
    if (!list.length) return y
    const rows = Math.ceil(list.length / columns)
    const region = { id, x, y, width: widthFor(columns), height: LABEL_HEIGHT + rows * CARD_HEIGHT + (rows - 1) * GAP + PADDING }
    regions.push(region)
    list.forEach((table, index) => {
      tableGroups[table.id] = id
      positions[table.id] = { x: x + PADDING + (index % columns) * (CARD_WIDTH + GAP), y: y + LABEL_HEIGHT + Math.floor(index / columns) * (CARD_HEIGHT + GAP) }
    })
    return y + region.height + SECTION_GAP
  }
  const rootBottom = place('games', BRANCH_STEP, 0)
  const branchesY = Math.max(420, rootBottom)
  const playersBottom = place('players', 0, branchesY)
  const detailsBottom = place('details', BRANCH_STEP, branchesY)
  const positionsBottom = place('positions', BRANCH_STEP * 2, branchesY)
  const coreBottom = Math.max(
    place('player-summaries', 0, playersBottom),
    place('game-summaries', BRANCH_STEP, detailsBottom),
    place('position-summaries', BRANCH_STEP * 2, positionsBottom),
  )
  const dashboardBottom = place('dashboard', (CORE_WIDTH - widthFor(3)) / 2, coreBottom + SECTION_GAP, 3)
  const applicationY = Math.max(coreBottom, dashboardBottom) + SECTION_GAP
  const accountsBottom = place('accounts', 0, applicationY)
  const appearanceBottom = place('appearance', BRANCH_STEP, applicationY, 2)
  const researchX = -BRANCH_WIDTH - 200
  const researchBottom = place('research', researchX, branchesY)
  place('research-results', researchX, researchBottom)
  const processingX = CORE_WIDTH + 200
  let processingY = branchesY
  for (const id of ['cloud-jobs', 'cloud-selection', 'cloud-recovery', 'cloud-results', 'ingestion']) {
    processingY = place(id, processingX, processingY, 2)
  }
  place('cloud-control', processingX + widthFor(2) + 120, branchesY, 4)
  place('other', 0, Math.max(accountsBottom, appearanceBottom) + SECTION_GAP, 4)
  const hasCore = ['games', 'players', 'details', 'positions'].some(id => members.get(id).length)
  // Start with readable chess tables, not every small operational table at once.
  const overviewBounds = hasCore ? { left: -PADDING, top: -PADDING, right: CORE_WIDTH + PADDING,
    bottom: branchesY + LABEL_HEIGHT + CARD_HEIGHT + PADDING } : null
  return { positions, regions, tableGroups, overviewBounds }
}

export function regionsForPositions(tables, positions) {
  const bounds = new Map()
  for (const table of tables) {
    const point = positions[table.id]
    if (!point || !Number.isFinite(point.x) || !Number.isFinite(point.y)) continue
    const id = tableGroup(table).id, previous = bounds.get(id)
    bounds.set(id, {
      left: Math.min(previous?.left ?? Infinity, point.x), top: Math.min(previous?.top ?? Infinity, point.y),
      right: Math.max(previous?.right ?? -Infinity, point.x + CARD_WIDTH),
      // Keep space for expanding a table; collapse does not make its region jump.
      bottom: Math.max(previous?.bottom ?? -Infinity, point.y + CARD_HEIGHT),
    })
  }
  return TABLE_GROUPS.filter(group => bounds.has(group.id)).map(group => {
    const box = bounds.get(group.id)
    return { id: group.id, x: box.left - PADDING, y: box.top - LABEL_HEIGHT,
      width: box.right - box.left + PADDING * 2, height: box.bottom - box.top + LABEL_HEIGHT + PADDING }
  })
}
