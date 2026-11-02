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

export const graphIngestionActive = run => ['working', 'running', 'queued', 'waiting', 'retrying', 'unavailable'].includes(run?.graph_sync?.status)
const withGraph = (run, rows) => run?.graph_sync ? [...rows.filter(row => row.step !== 'neo4j_ingestion'), run.graph_sync] : rows

export function pipelineSteps(run, jobs = {}) {
  if (run?.steps?.length) {
    if (!run.step_catalog) return withGraph(run, run.steps)
    const recorded = new Map(run.steps.map(step => [step.step, { ...step }]))
    const rows = run.step_catalog.map(step => recorded.get(step.step) || {
      ...step, status: run.status === 'complete' ? 'skipped' : 'pending',
      processed: 0, total: null, elapsed_seconds: 0, unit: 'items',
    })
    // Historical shared jobs may have only a durable enclosing endgame stage.
    // Show that verified completion instead of implying the work was skipped.
    const endgames = run.stages?.find(stage => stage.stage === 'tablebase_marking')
    if (endgames?.status === 'complete' && !run.steps.some(step =>
      ['discover_endgames', 'probe_endgames', 'save_endgames'].includes(step.step))) {
      rows.push({ step: 'tablebase_marking', label: 'Endgame analysis', group: 'Endgames',
        status: 'complete', processed: endgames.processed || 0, total: endgames.total || null,
        unit: 'checks', elapsed_seconds: (endgames.elapsed_ms || 0) / 1000 })
    }
    const queued = jobs.fen?.status
    const extraction = rows.find(row => row.step === 'extract_positions')
    if (run.status === 'running' && extraction?.status === 'pending'
      && ['queued', 'deferred'].includes(queued?.status)
      && queued.progress?.ingestion_run_id === run.run_id) {
      Object.assign(extraction, { status: 'queued', total: queued.progress.total || null, unit: 'games',
        detail: 'Queued for position extraction — waiting for an ingestion worker. Downloading games is complete; their positions are not ready for analysis yet.' })
    }
    return withGraph(run, rows)
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
  return withGraph(run, rows)
}

export function stepEstimate(step) {
  // Unknown or growing batch totals do not justify an ETA for the whole run.
  if (!step.fixed_total || !step.rate || step.total == null || step.status !== 'working') return null
  return Math.max(0, step.total - step.processed) / step.rate
}

const gamesSteps = new Set(['download_archives', 'filter_games', 'parse_games', 'save_games', 'parsing_games'])
const doneStates = new Set(['complete', 'skipped'])
const issues = ['failed', 'error', 'retrying', 'unavailable', 'warning', 'partial', 'cancelled', 'canceled']

// Show real counters for the CURRENT operation, not an invented percentage
// combining games, FENs and database writes with incompatible units.
export function compactPipelineSteps(run, jobs = {}) {
  const all = pipelineSteps(run, jobs)
  if (!all.length) return []
  const definitions = [
    { step: 'games', label: 'Games', rows: all.filter(row => gamesSteps.has(row.step)) },
    { step: 'positions', label: 'Positions', rows: all.filter(row => row.step !== 'neo4j_ingestion' && !gamesSteps.has(row.step)) },
    ...(run?.graph_sync ? [{ step: 'neo4j', label: 'Neo4j', rows: [run.graph_sync] }] : []),
  ]
  return definitions.map(({ step, label, rows }) => {
    let current = issues.map(status => rows.find(row => row.status === status)).find(Boolean)
      || rows.find(row => ['working', 'running'].includes(row.status))
      || rows.find(row => ['queued', 'waiting'].includes(row.status))
      || rows.find(row => !doneStates.has(row.status)) || rows.at(-1)
    const allDone = rows.length > 0 && rows.every(row => doneStates.has(row.status))
    if (allDone) current = rows.find(row => row.step === (step === 'games' ? 'save_games' : 'link_positions'))
      || rows.find(row => row.step === 'extract_positions') || rows.find(row => row.status === 'complete') || current
    let status = !rows.length && run?.status === 'complete' ? 'skipped'
      : allDone ? (rows.every(row => row.status === 'skipped') ? 'skipped' : 'complete')
      : current?.status || 'pending'
    if (!current) current = { processed: 0, total: null, elapsed_seconds: 0, unit: 'items' }
    if (run && run.status !== 'running' && step !== 'neo4j' && !allDone
      && ['working', 'running', 'waiting', 'queued'].includes(status)) {
      status = issues.includes(run.status) ? run.status : 'stopped'
    }
    return { ...current, step, label, status, currentLabel: current.label,
      currentStep: current.step, totalSteps: rows.length,
      completedSteps: rows.filter(row => doneStates.has(row.status)).length }
  })
}
