export const cloudDefaults = {
  player: '', total: 1000, positions: 1000, runs: 1,
  selection: 'latest', games: 200, from: '', to: '',
  allGames: false, order: 'latest', scope: 'all',
}

export const MAX_CLOUD_FENS = 200000
export const MAX_CLOUD_RUNS = MAX_CLOUD_FENS / 1000
// Display-only mirror of the server policy; never included in job requests.
export const CLOUD_STALL_TIMEOUT_SECONDS = 300

export function cloudGameSelectionError(preview) {
  if (!preview?.plan_id) return 'Preview the games first.'
  if (!Number.isInteger(preview.fens_to_analyze) || preview.fens_to_analyze < 0) {
    return 'Game preview has no valid FEN count; preview again.'
  }
  if (preview.fens_to_analyze === 0) return 'No missing FENs in this game selection.'
  if (preview.fens_to_analyze > MAX_CLOUD_FENS) {
    return `This selection exceeds the ${MAX_CLOUD_FENS.toLocaleString('en-US')}-FEN cloud safety limit. Select fewer games and preview again.`
  }
  return ''
}

export function cloudPayload(mode, form, preview) {
  if (mode === 'games' && cloudGameSelectionError(preview)) {
    throw new Error(cloudGameSelectionError(preview))
  }
  const player = mode === 'all' || (mode === 'loop' && form.scope === 'all') ? '' : form.player.trim()
  if ((mode === 'player' || (mode === 'loop' && form.scope === 'player')) && !player) {
    throw new Error('Enter a player name.')
  }
  return {
    mode, player_name: player,
    ...(mode === 'games' ? {} : { total_fens: Number(form.total) }),
    positions_per_run: Number(form.positions), runs: Number(form.runs),
    plan_id: mode === 'games' ? preview.plan_id : null,
  }
}

export function cloudGamePreview(form) {
  return {
    player_name: form.player.trim(), selection_mode: form.selection,
    game_limit: Number(form.games), use_all: form.selection === 'range' && form.allGames,
    date_from: ['range', 'fair_range'].includes(form.selection) ? form.from || null : null,
    date_to: ['range', 'fair_range'].includes(form.selection) ? form.to || null : null,
    range_order: form.order,
  }
}

export function cloudBound(mode, form, preview) {
  if (mode === 'games') {
    if (!preview) return 'Preview games to calculate the FEN count for one cloud job.'
    if (cloudGameSelectionError(preview)) return cloudGameSelectionError(preview)
  }
  const total = mode === 'games' ? preview.fens_to_analyze : mode === 'loop'
    ? Number(form.positions) * Number(form.runs) : Number(form.total)
  return `One cloud request for ${total.toLocaleString('en-US')} FENs on one 4-vCPU Spot VM at a time. Confirmed Google preemptions retry automatically in the cloud; application failures have two attempts total. Uploads every 500 results. During analysis, the system stops after ${CLOUD_STALL_TIMEOUT_SECONDS} seconds without a new completed FEN, not total runtime. Startup, recovery and uploads have separate progress checks. No fixed task-hour or monetary cap; cloud charges continue while it runs. Recovery obeys cancellation and Google platform limits.`
}
