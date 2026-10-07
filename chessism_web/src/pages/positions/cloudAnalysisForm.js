export const cloudDefaults = {
  player: '', total: 1000, positions: 1000, runs: 1,
  selection: 'latest', games: 200, from: '', to: '',
  allGames: false, order: 'latest', scope: 'all',
  backend: 'batch_spot', n_cpus: 4, n_vms: 1,
}

// The dedicated Cloud Run card cannot accidentally submit to the legacy Batch backend.
export const cloudRunDefaults = { ...cloudDefaults, backend: 'cloud_run' }

export const MAX_CLOUD_FENS = 200000
export const MAX_CLOUD_RUNS = MAX_CLOUD_FENS / 1000
export const MAX_BATCH_VMS = 10
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
  const backend = form.backend || 'batch_spot'
  if (!['batch_spot', 'cloud_run'].includes(backend)) throw new Error('Select a valid cloud backend.')
  if (backend === 'cloud_run' && (!Number.isInteger(Number(form.n_cpus)) || Number(form.n_cpus) < 1 || Number(form.n_cpus) > 64)) {
    throw new Error('Cloud Run requires 1–64 simultaneous CPUs, subject to Google quota.')
  }
  if (backend === 'batch_spot' && (!Number.isInteger(Number(form.n_vms)) || Number(form.n_vms) < 1 || Number(form.n_vms) > MAX_BATCH_VMS)) {
    throw new Error(`Batch Spot requires 1–${MAX_BATCH_VMS} VMs, subject to Google quota.`)
  }
  if (mode === 'games' && cloudGameSelectionError(preview)) {
    throw new Error(cloudGameSelectionError(preview))
  }
  const player = mode === 'all' || (mode === 'loop' && form.scope === 'all') ? '' : form.player.trim()
  if ((mode === 'player' || (mode === 'loop' && form.scope === 'player')) && !player) {
    throw new Error('Enter a player name.')
  }
  return {
    mode, player_name: player,
    backend, ...(backend === 'cloud_run' ? { n_cpus: Number(form.n_cpus) } : { n_vms: Number(form.n_vms) }),
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
  if (form.backend === 'cloud_run') return `Cloud Run: ${total.toLocaleString('en-US')} FENs, up to ${form.n_cpus} simultaneous 1-vCPU/1-GiB workers (quota checked before launch). One thread, 100,000 nodes, 256 MiB hash per engine. One automatic retry per task. Uploads every 500 results; no completed FEN for 300 seconds stops the attempt. No time-based FEN search limit; Google task limit is seven days per attempt. More expensive than Batch Spot in our benchmark; charges continue until tasks stop. Failed work retains checkpoints.`
  return `One cloud request for ${total.toLocaleString('en-US')} FENs across up to ${form.n_vms} Spot VMs (${Number(form.n_vms) * 16} vCPUs total), quota checked before launch. Each n2d-highcpu-16 VM has 16 vCPUs and 16 GiB RAM, with 16 single-thread engines, 100,000 nodes and 256 MiB hash per engine. The FEN set is split evenly without duplicates; each VM keeps its own checkpoints. Confirmed Google preemptions retry automatically in the cloud; application failures have two attempts total per VM. Uploads every 500 results per VM. During analysis, the system stops after ${CLOUD_STALL_TIMEOUT_SECONDS} seconds without a new completed FEN on that VM, not total runtime. Startup, recovery and uploads have separate progress checks. No fixed task-hour or monetary cap; cloud charges continue while it runs. Recovery obeys cancellation and Google platform limits.`
}
