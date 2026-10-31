const labels = {
  parsing_games: 'Download, parse and save games',
  fen_extraction: 'Extract positions',
  saving_positions: 'Save unique positions',
  linking_positions: 'Link positions to games',
  updating_summaries: 'Refresh ingestion summaries',
  tablebase_marking: 'Endgame analysis',
}
const phases = {
  saving_fens: 'saving_positions', saving_games: 'linking_positions',
  refreshing_statistics: 'updating_summaries', discovering_tablebase: 'tablebase_marking',
  extracting: 'fen_extraction', tablebase: 'tablebase_marking',
}

export function pipelineSteps(run, jobs = {}) {
  if (run?.steps?.length) {
    if (!run.step_catalog) return run.steps
    const recorded = new Map(run.steps.map(step => [step.step, { ...step }]))
    const rows = run.step_catalog.map(step => recorded.get(step.step) || {
      ...step, status: run.status === 'complete' ? 'skipped' : 'pending',
      processed: 0, total: null, elapsed_seconds: 0, unit: 'items',
    })
    const queued = jobs.fen?.status
    const extraction = rows.find(row => row.step === 'extract_positions')
    if (run.status === 'running' && extraction?.status === 'pending'
      && ['queued', 'deferred'].includes(queued?.status)
      && queued.progress?.ingestion_run_id === run.run_id) {
      Object.assign(extraction, { status: 'queued', total: queued.progress.total || null, unit: 'games',
        detail: 'Queued for position extraction — waiting for an ingestion worker. Downloading games is complete; their positions are not ready for analysis yet.' })
    }
    return rows
  }
  const rows = (run?.stages || []).map(stage => ({
    step: stage.stage, label: labels[stage.stage] || stage.stage.replaceAll('_', ' '),
    status: stage.status, processed: stage.processed || 0,
    total: stage.total || null, elapsed_seconds: (stage.elapsed_ms || 0) / 1000,
    unit: 'items', group: 'Ingestion',
  }))
  for (const job of Object.values(jobs)) {
    const progress = job?.status?.progress
    if (!progress || (run?.run_id && progress.ingestion_run_id !== run.run_id)) continue
    const phase = phases[progress.phase]
    const row = rows.find(item => item.step === phase)
    if (!row || ['complete', 'failed'].includes(row.status)) continue
    Object.assign(row, {
      processed: progress.processed || 0, total: progress.total || null,
      status: 'working', detail: progress.detail, updated_at: progress.updated_at,
    })
  }
  return rows
}

export function stepEstimate(step) {
  // Unknown or growing batch totals do not justify an ETA for the whole run.
  if (!step.fixed_total || !step.rate || step.total == null || step.status !== 'working') return null
  return Math.max(0, step.total - step.processed) / step.rate
}
