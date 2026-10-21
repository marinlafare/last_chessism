import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useState } from 'react'
import AnalysisDisclosure from './AnalysisDisclosure'
import LocalGameSelection from './LocalGameSelection'
import LocalLoopSelection from './LocalLoopSelection'
import LocalAnalysisActivity from './LocalAnalysisActivity'
import { JobStatus } from './JobStatus'
import { MAX_ANALYSIS_BATCH_SIZE, MAX_GLOBAL_ANALYSIS_BATCH_SIZE,
  clampAnalysisBatchInput, formatNumber, getAnalysisButtonLabel, isTrackedJobActive } from './positionPageSupport'

export default function LocalAnalysisSection({ page }) {
  const [mode, setMode] = useState('all')
  const activeCount = page.analysisProcessViews.length
  return <AnalysisDisclosure id="analysis-local" label="Local"
    indicator={activeCount > 0 ? <span className="analysis-disclosure-live" role="img"
      aria-label="Local analysis running or queued" title="Local analysis running or queued" /> : null}>
    <section {...pageStyleTarget('local.selection')} className="local-analysis-selection" aria-label="Local FEN selection">
      <label {...pageStyleTarget('label')} className="local-selection-picker">
        <span {...pageStyleTarget('label')} className="field-label">FEN selection</span>
        <select {...pageStyleTarget('field')} className="text-input" aria-label="Local FEN selection" value={mode}
          onChange={event => setMode(event.target.value)}>
          <option value="all">All positions — unscored FENs</option>
          <option value="player">Player positions — unscored FENs</option>
          <option value="games">Player games — missing FENs</option>
          <option value="loop">Repeated local runs — all or player</option>
        </select>
      </label>
      {mode === 'games' ? <LocalGameSelection page={page} /> : mode === 'loop'
        ? <LocalLoopSelection page={page} /> : <LocalPositionSelection page={page} mode={mode} />}
    </section>
    <LocalAnalysisActivity page={page} />
  </AnalysisDisclosure>
}

function LocalPositionSelection({ page, mode }) {
  const player = mode === 'player'
  const state = player ? page.playerJob : page.globalJob
  const update = player ? page.setPlayerJob : page.setGlobalJob
  const key = player ? 'player' : 'global'
  const maximum = player ? MAX_ANALYSIS_BATCH_SIZE : MAX_GLOBAL_ANALYSIS_BATCH_SIZE
  const status = page.jobState[key]
  return <form className="local-selection-form" aria-label="Local position analysis"
    onSubmit={player ? page.handlePlayerAnalysis : page.handleGlobalAnalysis}>
    {player && <>
      <label {...pageStyleTarget('label')}>
        <span {...pageStyleTarget('label')} className="field-label">Chess.com nickname</span>
        <div className="input-action-row">
          <input {...pageStyleTarget('field')} className="text-input" type="text" required value={state.playerName}
            onChange={event => {
              update(current => ({ ...current, playerName: event.target.value }))
              page.setPlayerInspection(current => ({ ...current, error: '', data: null }))
            }} onKeyDown={event => {
              if (event.key !== 'Enter') return
              event.preventDefault()
              page.handleInspectPlayer()
            }} placeholder="hikaru" />
          <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button" onClick={page.handleInspectPlayer}
            disabled={page.playerInspection.loading}>{page.playerInspection.loading ? 'Inspecting' : 'Inspect player'}</button>
        </div>
      </label>
      {page.playerInspection.error && <div className="status-banner warn">{page.playerInspection.error}</div>}
      {page.playerInspection.data && <p {...pageStyleTarget('text')} className="local-selection-counts">
        {formatNumber(page.playerInspection.data.total_positions)} positions ·
        {' '}{formatNumber(page.playerInspection.data.analyzed_positions)} analyzed
      </p>}
    </>}
    {!player && <p {...pageStyleTarget('text')} className="local-selection-counts">
      {page.analysisCounts || page.coverage ? formatNumber(page.pendingPositions) : '—'} positions pending ·
      {' '}{page.analysisCounts ? formatNumber(page.scoredPositions) : '—'} scored
    </p>}
    <div className="local-position-fields">
      <label {...pageStyleTarget('label')}><span {...pageStyleTarget('label')} className="field-label">FEN positions</span>
        <input {...pageStyleTarget('field')} className="text-input number-input-clean" type="number" min="1" step="1" required
          value={state.totalFens} onWheel={event => event.currentTarget.blur()}
          onChange={event => update(current => ({ ...current, totalFens: event.target.value }))} />
      </label>
      <label {...pageStyleTarget('label')}><span {...pageStyleTarget('label')} className="field-label">Batch size</span>
        <input {...pageStyleTarget('field')} className="text-input number-input-clean" type="number" min="1" max={maximum} step="1" required
          value={state.batchSize} onWheel={event => event.currentTarget.blur()}
          onChange={event => update(current => ({ ...current, batchSize: clampAnalysisBatchInput(event.target.value, maximum) }))} />
      </label>
    </div>
    <button {...pageStyleTarget('button')} className="btn btn-primary local-analyze-button" type="submit" disabled={status?.loading || isTrackedJobActive(status)}>
      {status?.loading || isTrackedJobActive(status) ? getAnalysisButtonLabel(status) : 'Analyze locally'}
    </button>
    <JobStatus jobKey={key} page={page} />
  </form>
}
