export const cloudDefaults = {
  player: '', total: 1000, positions: 1000, runs: 1,
  selection: 'latest', games: 200, from: '', to: '',
  allGames: false, order: 'latest', scope: 'all',
  backend: 'batch_spot', n_cpus: 4, n_vms: 1,
}

// The dedicated Cloud Run card cannot accidentally submit to the legacy Batch backend.
export const cloudRunDefaults = { ...cloudDefaults, backend: 'cloud_run' }

export const MAX_CLOUD_FENS = 500000
export const MAX_BATCH_FENS_PER_VM = 1000000
export const batchCycleCapacity = nVms => MAX_BATCH_FENS_PER_VM * Number(nVms)
export const cloudLimit = (backend) => backend === 'cloud_run' ? 200000 : MAX_CLOUD_FENS * MAX_BATCH_REPEATS
export const MAX_CLOUD_RUNS = MAX_CLOUD_FENS * 20 / 1000
export const MAX_CLOUD_RUN_CPUS = 100
export const MAX_BATCH_VMS = 10
export const MAX_BATCH_REPEATS = 20
// Display-only mirror of the server policy; never included in job requests.
export const CLOUD_STALL_TIMEOUT_SECONDS = 300

export function cloudGameSelectionError(preview, backend = 'batch_spot') {
  const maximum = cloudLimit(backend)
  if (!preview?.plan_id) return 'Preview the games first.'
  if (!Number.isInteger(preview.fens_to_analyze) || preview.fens_to_analyze < 0) {
    return 'Game preview has no valid FEN count; preview again.'
  }
  if (preview.fens_to_analyze === 0) return 'No missing FENs in this game selection.'
  if (preview.fens_to_analyze > maximum) {
    return `This selection exceeds the ${maximum.toLocaleString('en-US')}-FEN cloud safety limit. Select fewer games and preview again.`
  }
  return ''
}

export function cloudPayload(mode, form, preview) {
  const backend = form.backend || 'batch_spot'
  if (!['batch_spot', 'cloud_run'].includes(backend)) throw new Error('Select a valid cloud backend.')
  if (backend === 'cloud_run' && (!Number.isInteger(Number(form.n_cpus)) || Number(form.n_cpus) < 1 || Number(form.n_cpus) > MAX_CLOUD_RUN_CPUS)) {
    throw new Error(`Cloud Run requires 1–${MAX_CLOUD_RUN_CPUS} simultaneous CPUs, subject to Google quota.`)
  }
  if (backend === 'batch_spot' && (!Number.isInteger(Number(form.n_vms)) || Number(form.n_vms) < 1 || Number(form.n_vms) > MAX_BATCH_VMS)) {
    throw new Error(`Batch Spot requires 1–${MAX_BATCH_VMS} VMs, subject to Google quota.`)
  }
  if (mode === 'games' && cloudGameSelectionError(preview, backend)) {
    throw new Error(cloudGameSelectionError(preview, backend))
  }
  const total = mode === 'loop' ? Number(form.positions) * Number(form.runs) : Number(form.total)
  if (mode !== 'games' && (!Number.isInteger(total) || total < 1 || total > cloudLimit(backend))) {
    throw new Error(`Select 1–${cloudLimit(backend).toLocaleString('en-US')} FENs.`)
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
    if (cloudGameSelectionError(preview, form.backend)) return cloudGameSelectionError(preview, form.backend)
  }
  const total = mode === 'games' ? preview.fens_to_analyze : mode === 'loop'
    ? Number(form.positions) * Number(form.runs) : Number(form.total)
  if (form.backend === 'cloud_run') return `Cloud Run: ${total.toLocaleString('en-US')} FENs, up to ${form.n_cpus} simultaneous 1-vCPU/1-GiB workers (quota checked before launch). One thread, 100,000 nodes, 256 MiB hash per engine. One automatic retry per task. Uploads every 500 results; no completed FEN for 300 seconds stops the attempt. No time-based FEN search limit; Google task limit is seven days per attempt. More expensive than Batch Spot in our benchmark; charges continue until tasks stop. Failed work retains checkpoints.`
  const capacity = batchCycleCapacity(form.n_vms)
  if (!Number.isInteger(Number(form.n_vms)) || Number(form.n_vms) < 1 || Number(form.n_vms) > MAX_BATCH_VMS) return 'Choose a valid Spot VM count.'
  const tasks = Math.ceil(total / capacity)
  const last = total % capacity || capacity
  return `One fixed set of up to ${total.toLocaleString('en-US')} unscored FENs → ${tasks} automatic task(s), at most ${capacity.toLocaleString('en-US')} FENs each (1,000,000 per VM)${tasks > 1 ? `; final task: ${last.toLocaleString('en-US')} FENs` : ''}. Tasks run sequentially in Google, each using up to ${form.n_vms} Spot VMs (${Number(form.n_vms) * 16} vCPUs concurrently, subject to quota). All inputs and the shared image are uploaded first. Keep your PC on for uploads, result imports and cleanup. After Google acknowledges the complete sequence, it advances tasks even while you are offline; importing catches up when Docker returns. Compute stops after each task; results remain until the entire sequence is imported and verified, then cleanup runs. Each VM: 16 vCPUs / 16 GiB, single-thread FENs, 100,000 nodes, 256 MiB hash. Uploads every 500 results; ${CLOUD_STALL_TIMEOUT_SECONDS}s without a completed FEN stops the attempt. Confirmed preemptions retry; two application attempts per task. No total-runtime or monetary cap. Compute, storage and workflow charges apply.`
}
