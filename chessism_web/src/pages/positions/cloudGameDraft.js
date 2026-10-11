import { cloudDefaults, MAX_BATCH_VMS, batchCycleCapacity, cloudLimit } from './cloudAnalysisForm.js'

export function hasSavedCloudDraft(draft) {
  return Boolean(draft?.id && !draft.submitted_job_id && draft.entries?.length)
}

export function draftEntryForm(entry) {
  return { ...cloudDefaults, player: entry.player_name, selection: entry.selection_mode,
    games: entry.requested_games ?? 200, allGames: entry.requested_games === null,
    from: entry.date_from || '', to: entry.date_to || '',
    order: entry.selection_order === 'oldest' ? 'oldest' : 'latest' }
}

export function draftSubmitError(draft, nVms) {
  if (!draft || !draft.entries.length) return 'Add at least one game selection.'
  if (draft.submitted_job_id) return 'This draft has already been submitted.'
  const preview = draft.preview
  if (!preview || preview.revision !== draft.revision) return 'Preview the combined selection before submitting.'
  if (!Number.isInteger(preview.fens_to_analyze) || preview.fens_to_analyze < 1) return 'No available unscored FENs in this selection.'
  if (preview.fens_to_analyze > draft.max_fens) return `Reduce the selection to at most ${draft.max_fens.toLocaleString('en-US')} FENs.`
  if (!Number.isInteger(Number(nVms)) || Number(nVms) < 1 || Number(nVms) > MAX_BATCH_VMS) return `Choose 1–${MAX_BATCH_VMS} Spot VMs.`
  return ''
}

export function draftTaskSummary(count, nVms = 1) {
  if (!Number.isInteger(count) || count <= 0) return 'No cloud tasks.'
  if (!Number.isInteger(Number(nVms)) || Number(nVms) < 1 || Number(nVms) > MAX_BATCH_VMS) return 'Choose a valid Spot VM count.'
  const capacity = batchCycleCapacity(nVms)
  const full = Math.floor(count / capacity), last = count % capacity
  const sizes = [full ? `${full} × ${capacity.toLocaleString('en-US')}` : '', last ? last.toLocaleString('en-US') : '']
    .filter(Boolean).join(' + ')
  const tasks = Math.ceil(count / capacity)
  return `${sizes} FENs across ${tasks} cycle(s) in Google, ${tasks > 1 ? 'sequentially, ' : ''}using up to ${nVms} parallel Spot VMs; at most 1,000,000 FENs per VM per cycle.`
}

export function draftVmPlan(count, nVms) {
  const vms = Number(nVms)
  if (!Number.isSafeInteger(count) || count < 1 || count > cloudLimit('batch_spot')
      || !Number.isInteger(vms) || vms < 1 || vms > MAX_BATCH_VMS) return null
  const capacity = batchCycleCapacity(vms)
  // Mirror sequencing.cycle_sizes and work_units.ranges: full cycles first,
  // then an even VM split within each cycle (the first VMs take remainders).
  return Array.from({ length: Math.ceil(count / capacity) }, (_, index) => {
    const total = Math.min(capacity, count - index * capacity)
    const usedVms = Math.min(total, vms)
    const size = Math.floor(total / usedVms), extra = total % usedVms
    return { cycle: index + 1, total,
      vmFens: Array.from({ length: usedVms }, (_, vm) => size + (vm < extra ? 1 : 0)) }
  })
}
