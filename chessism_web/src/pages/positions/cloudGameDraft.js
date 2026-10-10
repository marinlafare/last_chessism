import { cloudDefaults, MAX_BATCH_VMS, batchCycleCapacity } from './cloudAnalysisForm.js'

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
