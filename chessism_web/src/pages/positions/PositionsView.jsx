import { Chessboard } from 'react-chessboard'
import Header from '../../components/layout/Header'
import Footer from '../../components/layout/Footer'
import SideRail from '../../components/layout/SideRail'
import { EstimatedTime, JobStatus } from './JobStatus'
import {
  MAX_ANALYSIS_BATCH_SIZE,
  MAX_LOOP_ANALYSIS_BATCH_SIZE,
  clampAnalysisBatchInput,
  formatDuration,
  formatNumber,
  getPlayerGameSelectionLabel,
  getScoreLabel,
  isTrackedJobActive,
  moveToSan,
} from './positionPageSupport'

export default function PositionsView({ page }) {
  const {
    activeLoopJobCount, analysisCounts, analysisLines, analysisProcessViews,
    analysisProcessesError, bestLine, boardWidth, boardWrapRef, coverage,
    coverageBarItems, coverageError, deletingAnalysisJobIds, error, etaClockMs,
    etaEstimatesRef, fenInput, globalJob, handleAnalysisLoop, handleAnalyze,
    handleConfirmPlayerGameAnalysis, handleDeleteAnalysisProcess,
    handleGlobalAnalysis, handleInspectPlayer, handleInspectPlayerGameScope,
    handlePlayerAnalysis, handlePreviewPlayerGameAnalysis, jobState, loading,
    loopJob, loopJobIsQueueing, multipv, nodesLimit, pendingPositions,
    playerGameAnalysis, playerGamePreview, playerGamePreviewTotalSeconds,
    playerInspection, playerJob, remainingFenGames, result, scoredPositions,
    setFenInput, setGlobalJob, setLoopJob, setMultipv, setNodesLimit,
    setPlayerGameAnalysis, setPlayerJob, turnLabel, validation,
  } = page

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="positions-main">
          <section className="coverage-bar-chart" aria-label="Position coverage">
            {coverageError ? <div className="status-banner warn">{coverageError}</div> : null}

            {coverageBarItems.map((item) => (
              <div className={`coverage-bar-row ${item.capped ? 'is-capped' : ''}`} key={item.key}>
                <div className="coverage-bar-meta">
                  <span>{item.label}</span>
                  <strong>{item.ready ? formatNumber(item.value) : '-'}</strong>
                </div>
                <div className="coverage-bar-track" aria-hidden="true">
                  <div
                    className={`coverage-bar-fill coverage-bar-fill-${item.key}`}
                    style={{ width: `${item.percent}%` }}
                  />
                </div>
              </div>
            ))}
          </section>

          <div className={`pipeline-grid ${playerInspection.data || playerInspection.error ? 'has-player-inspection' : ''}`}>
            <section className="pipeline-card" aria-live="polite">
              <div>
                <p className="eyebrow">AUTOMATIC FEN EXTRACTION</p>
              </div>
              <div className="pipeline-pending">
                <span>Games pending</span>
                <strong>{remainingFenGames === null ? '-' : formatNumber(remainingFenGames)}</strong>
              </div>
              <div className="status-banner">
                {isTrackedJobActive(jobState.tablebase)
                  ? 'Caching exact endgame results automatically.'
                  : isTrackedJobActive(jobState.fen)
                    ? 'Extracting positions automatically.'
                  : remainingFenGames > 0
                    ? 'Pending games are queued for automatic extraction.'
                    : 'New games are extracted automatically, then eligible endgames are solved with Syzygy.'}
              </div>
              <JobStatus jobKey="fen" page={page} />
              <JobStatus jobKey="tablebase" page={page} />
            </section>

            <form className="pipeline-card" onSubmit={handleGlobalAnalysis}>
              <div>
                <p className="eyebrow">ANALYZE ALL</p>
              </div>
              <div className="pipeline-summary-grid">
                <div className="pipeline-pending">
                  <span>Positions pending</span>
                  <strong>{analysisCounts || coverage ? formatNumber(pendingPositions) : '-'}</strong>
                </div>
                <div className="pipeline-pending">
                  <span>Scored positions</span>
                  <strong>{analysisCounts ? formatNumber(scoredPositions) : '-'}</strong>
                </div>
              </div>
              <div className="pipeline-inline pipeline-inline-compact">
                <label>
                  <span className="field-label">Positions</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    value={globalJob.totalFens}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setGlobalJob((current) => ({ ...current, totalFens: event.target.value }))}
                  />
                </label>
                <label>
                  <span className="field-label">Batch</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    max={MAX_ANALYSIS_BATCH_SIZE}
                    value={globalJob.batchSize}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setGlobalJob((current) => ({
                      ...current,
                      batchSize: clampAnalysisBatchInput(event.target.value)
                    }))}
                  />
                </label>
              </div>
              <button className="btn btn-primary" type="submit" disabled={jobState.global?.loading || isTrackedJobActive(jobState.global)}>
                {jobState.global?.loading ? 'Queueing' : isTrackedJobActive(jobState.global) ? 'Analyzing' : 'Analyze'}
              </button>
              <JobStatus jobKey="global" page={page} />
            </form>

            <form className="pipeline-card" onSubmit={handlePlayerAnalysis}>
              <div>
                <p className="eyebrow">ANALYZE PLAYER</p>
              </div>
              <label className="pipeline-player-name-row">
                <div className="input-action-row">
                  <input
                    className="text-input"
                    type="text"
                    value={playerJob.playerName}
                    onChange={(event) => {
                      setPlayerJob((current) => ({ ...current, playerName: event.target.value }))
                      setPlayerInspection((current) => ({ ...current, error: '', data: null }))
                    }}
                    onKeyDown={(event) => {
                      if (event.key !== 'Enter') return
                      event.preventDefault()
                      handleInspectPlayer()
                    }}
                    placeholder="...chess.com nickname..."
                  />
                  <button
                    className="btn btn-secondary btn-inline"
                    type="button"
                    onClick={handleInspectPlayer}
                    disabled={playerInspection.loading}
                  >
                    {playerInspection.loading ? 'inspecting' : 'inspect'}
                  </button>
                </div>
              </label>
              {playerInspection.error ? <div className="status-banner warn">{playerInspection.error}</div> : null}
              {playerInspection.data ? (
                <div className="player-inspection-card">
                  <div>
                    <span>Total Positions</span>
                    <strong>{formatNumber(playerInspection.data.total_positions)}</strong>
                  </div>
                  <div>
                    <span>Analyzed</span>
                    <strong>{formatNumber(playerInspection.data.analyzed_positions)}</strong>
                  </div>
                </div>
              ) : null}
              <div className="pipeline-inline">
                <label>
                  <span className="field-label">Positions</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    value={playerJob.totalFens}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setPlayerJob((current) => ({ ...current, totalFens: event.target.value }))}
                  />
                </label>
                <label>
                  <span className="field-label">Batch</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    max={MAX_ANALYSIS_BATCH_SIZE}
                    value={playerJob.batchSize}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setPlayerJob((current) => ({
                      ...current,
                      batchSize: clampAnalysisBatchInput(event.target.value)
                    }))}
                  />
                </label>
              </div>
              <button className="btn btn-primary" type="submit" disabled={jobState.player?.loading || isTrackedJobActive(jobState.player)}>
                {jobState.player?.loading ? 'Queueing' : isTrackedJobActive(jobState.player) ? 'Analyzing' : 'Analyze'}
              </button>
              <JobStatus jobKey="player" page={page} />
            </form>

            <form className="pipeline-card analysis-loop-card" onSubmit={handleAnalysisLoop}>
              <div className="analysis-loop-heading">
                <div>
                  <p className="eyebrow">ANALYSIS LOOPS</p>
                  <small>Run sequential Stockfish passes with a cooling pause between them.</small>
                </div>
                <span className="stat-chip">
                  {formatNumber(Number(loopJob.positionsPerRun || 0) * Number(loopJob.runs || 0))} total positions
                </span>
              </div>

              <div className="analysis-loop-scope" role="radiogroup" aria-label="Analysis loop scope">
                <label className={`analysis-loop-scope-option ${loopJob.scope === 'all' ? 'selected' : ''}`}>
                  <input
                    type="radio"
                    name="analysis-loop-scope"
                    value="all"
                    checked={loopJob.scope === 'all'}
                    onChange={() => setLoopJob((current) => ({ ...current, scope: 'all' }))}
                  />
                  <span>
                    <strong>Analyze all</strong>
                    <small>Most repeated positions across the database</small>
                  </span>
                </label>
                <label className={`analysis-loop-scope-option ${loopJob.scope === 'player' ? 'selected' : ''}`}>
                  <input
                    type="radio"
                    name="analysis-loop-scope"
                    value="player"
                    checked={loopJob.scope === 'player'}
                    onChange={() => setLoopJob((current) => ({ ...current, scope: 'player' }))}
                  />
                  <span>
                    <strong>Analyze player</strong>
                    <small>Most repeated positions belonging to one player</small>
                  </span>
                </label>
              </div>

              <p className="analysis-loop-selection" aria-live="polite">
                Selected scope: <strong>{loopJob.scope === 'player' ? 'one player' : 'all database positions'}</strong>
              </p>

              {loopJob.scope === 'player' ? (
                <label>
                  <span className="field-label">Chess.com nickname</span>
                  <input
                    className="text-input"
                    type="text"
                    value={loopJob.playerName}
                    onChange={(event) => setLoopJob((current) => ({ ...current, playerName: event.target.value }))}
                    placeholder="...Chess.com nickname..."
                  />
                </label>
              ) : null}

              <div className="analysis-loop-fields">
                <label>
                  <span className="field-label">Positions / run</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    value={loopJob.positionsPerRun}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setLoopJob((current) => ({ ...current, positionsPerRun: event.target.value }))}
                  />
                </label>
                <label>
                  <span className="field-label">Runs</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    max="100"
                    value={loopJob.runs}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setLoopJob((current) => ({ ...current, runs: event.target.value }))}
                  />
                </label>
                <label>
                  <span className="field-label">Batch</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="1"
                    max={MAX_LOOP_ANALYSIS_BATCH_SIZE}
                    value={loopJob.batchSize}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setLoopJob((current) => ({
                      ...current,
                      batchSize: clampAnalysisBatchInput(event.target.value, MAX_LOOP_ANALYSIS_BATCH_SIZE)
                    }))}
                  />
                </label>
                <label>
                  <span className="field-label">Cool-off seconds</span>
                  <input
                    className="text-input number-input-clean"
                    type="number"
                    min="0"
                    max="3600"
                    value={loopJob.coolOff}
                    onWheel={(event) => event.currentTarget.blur()}
                    onChange={(event) => setLoopJob((current) => ({ ...current, coolOff: event.target.value }))}
                  />
                </label>
              </div>

              <p className="analysis-loop-note">
                Batch 500 is recommended because it commits progress more frequently while keeping all four engines busy. The maximum is 1,000.
                {' '}When a schedule is already running, the next one waits in the queue and starts automatically afterward.
              </p>
              <button className="btn btn-primary" type="submit" disabled={loopJobIsQueueing || activeLoopJobCount >= 2}>
                {loopJobIsQueueing
                  ? 'Queueing'
                  : activeLoopJobCount >= 2
                    ? 'Next schedule queued'
                    : activeLoopJobCount === 1
                      ? 'Queue next schedule'
                    : loopJob.scope === 'player' ? 'Start player loops' : 'Start all-position loops'}
              </button>
              <JobStatus jobKey="loop" page={page} />
              <JobStatus jobKey="loopNext" page={page} />
            </form>

            <section className="pipeline-card player-game-analysis-card">
              <div className="analysis-processes-heading">
                <div>
                  <p className="eyebrow">COMPLETE PLAYER GAMES</p>
                  <small>Select exact incomplete games, preview their unique missing FENs, then confirm or cancel.</small>
                </div>
                {playerGameAnalysis.scope ? (
                  <span className="stat-chip">
                    {formatNumber(playerGameAnalysis.scope.incomplete_games)} incomplete games
                  </span>
                ) : null}
              </div>

              <form className="player-game-scope-form" onSubmit={handleInspectPlayerGameScope}>
                <label>
                  <span className="field-label">Chess.com nickname</span>
                  <input
                    className="text-input"
                    type="text"
                    value={playerGameAnalysis.playerName}
                    placeholder="hikaru"
                    onChange={(event) => setPlayerGameAnalysis((current) => ({
                      ...current,
                      playerName: event.target.value,
                      scope: null,
                      preview: null,
                      error: ''
                    }))}
                  />
                </label>

                <div className="player-game-mode" role="radiogroup" aria-label="Game selection method">
                  {[
                    ['latest', 'Latest games', 'Newest incomplete games first'],
                    ['oldest', 'Oldest games', 'Oldest incomplete games first'],
                    ['range', 'Date range', 'Inspect all games between two dates'],
                    ['fair_range', 'Fair range', 'Balanced samples across every available month']
                  ].map(([value, label, description]) => (
                    <label
                      className={`analysis-loop-scope-option ${playerGameAnalysis.selectionMode === value ? 'selected' : ''}`}
                      key={value}
                    >
                      <input
                        type="radio"
                        name="player-game-selection-mode"
                        value={value}
                        checked={playerGameAnalysis.selectionMode === value}
                        onChange={() => setPlayerGameAnalysis((current) => ({
                          ...current,
                          selectionMode: value,
                          useAll: false,
                          scope: null,
                          preview: null,
                          error: ''
                        }))}
                      />
                      <span><strong>{label}</strong><small>{description}</small></span>
                    </label>
                  ))}
                </div>

                {playerGameAnalysis.selectionMode === 'range' ? (
                  <div className="player-game-date-fields">
                    <label>
                      <span className="field-label">Start date</span>
                      <input
                        className="text-input"
                        type="date"
                        value={playerGameAnalysis.dateFrom}
                        onChange={(event) => setPlayerGameAnalysis((current) => ({
                          ...current,
                          dateFrom: event.target.value,
                          scope: null,
                          preview: null,
                          error: ''
                        }))}
                      />
                    </label>
                    <label>
                      <span className="field-label">End date</span>
                      <input
                        className="text-input"
                        type="date"
                        value={playerGameAnalysis.dateTo}
                        onChange={(event) => setPlayerGameAnalysis((current) => ({
                          ...current,
                          dateTo: event.target.value,
                          scope: null,
                          preview: null,
                          error: ''
                        }))}
                      />
                    </label>
                  </div>
                ) : null}

                <button className="btn btn-secondary" type="submit" disabled={playerGameAnalysis.loadingScope}>
                  {playerGameAnalysis.loadingScope ? 'Inspecting games' : 'Inspect games'}
                </button>
              </form>

              {playerGameAnalysis.error ? <div className="status-banner warn">{playerGameAnalysis.error}</div> : null}

              {playerGameAnalysis.scope ? (
                <div className="player-game-scope-results">
                  <div><small>Games with FENs</small><strong>{formatNumber(playerGameAnalysis.scope.games_with_fens)}</strong></div>
                  <div><small>Already complete</small><strong>{formatNumber(playerGameAnalysis.scope.complete_games)}</strong></div>
                  <div><small>Incomplete available</small><strong>{formatNumber(playerGameAnalysis.scope.incomplete_games)}</strong></div>
                  <div>
                    <small>Available dates</small>
                    <strong>
                      {playerGameAnalysis.scope.earliest_game
                        ? `${String(playerGameAnalysis.scope.earliest_game).slice(0, 10)} → ${String(playerGameAnalysis.scope.latest_game).slice(0, 10)}`
                        : '--'}
                    </strong>
                  </div>
                </div>
              ) : null}

              {playerGameAnalysis.scope && Number(playerGameAnalysis.scope.incomplete_games || 0) > 0 ? (
                <div className="player-game-selection-controls">
                  {playerGameAnalysis.selectionMode === 'range' ? (
                    <div className="player-game-range-choice">
                      <label className="player-game-check-option">
                        <input
                          type="checkbox"
                          checked={playerGameAnalysis.useAll}
                          onChange={(event) => setPlayerGameAnalysis((current) => ({
                            ...current,
                            useAll: event.target.checked,
                            preview: null
                          }))}
                        />
                        <span>Use all {formatNumber(playerGameAnalysis.scope.incomplete_games)} incomplete games in this range</span>
                      </label>
                    </div>
                  ) : null}

                  <div className="player-game-selection-fields">
                    {(playerGameAnalysis.selectionMode !== 'range' || !playerGameAnalysis.useAll) ? (
                      <label>
                        <span className="field-label">Number of games</span>
                        <input
                          className="text-input number-input-clean"
                          type="number"
                          min="1"
                          max={playerGameAnalysis.scope.incomplete_games}
                          value={playerGameAnalysis.gameLimit}
                          onWheel={(event) => event.currentTarget.blur()}
                          onChange={(event) => setPlayerGameAnalysis((current) => ({
                            ...current,
                            gameLimit: event.target.value,
                            preview: null
                          }))}
                        />
                      </label>
                    ) : null}
                    {playerGameAnalysis.selectionMode === 'range' ? (
                      <label>
                        <span className="field-label">Take from range</span>
                        <select
                          className="text-input"
                          value={playerGameAnalysis.rangeOrder}
                          onChange={(event) => setPlayerGameAnalysis((current) => ({
                            ...current,
                            rangeOrder: event.target.value,
                            preview: null
                          }))}
                        >
                          <option value="latest">Latest first</option>
                          <option value="oldest">Oldest first</option>
                        </select>
                      </label>
                    ) : null}
                  </div>

                  <button
                    className="btn btn-primary"
                    type="button"
                    disabled={playerGameAnalysis.loadingPreview}
                    onClick={handlePreviewPlayerGameAnalysis}
                  >
                    {playerGameAnalysis.loadingPreview ? 'Calculating FENs' : 'Calculate FEN workload'}
                  </button>
                </div>
              ) : null}

              {playerGamePreview ? (
                <div className="player-game-confirmation">
                  <div className="player-game-confirmation-heading">
                    <div>
                      <span>CONFIRM FROZEN ANALYSIS PLAN</span>
                      <strong>{playerGamePreview.player_name}</strong>
                    </div>
                    <span className="analysis-process-phase queued">Awaiting confirmation</span>
                  </div>

                  <div className="player-game-preview-grid">
                    <div><small>Games selected</small><strong>{formatNumber(playerGamePreview.selected_games)}</strong></div>
                    <div><small>Position occurrences</small><strong>{formatNumber(playerGamePreview.position_occurrences)}</strong></div>
                    <div><small>Unique FENs</small><strong>{formatNumber(playerGamePreview.unique_fens)}</strong></div>
                    <div><small>Already analyzed</small><strong>{formatNumber(playerGamePreview.analyzed_unique_fens)}</strong></div>
                    <div><small>Syzygy exact FENs</small><strong>{formatNumber(playerGamePreview.tablebase_fens)}</strong></div>
                    <div className="highlight">
                      <small>Estimated FENs for Stockfish</small>
                      <strong>{formatNumber(Math.max(0, Number(playerGamePreview.fens_to_analyze || 0) - Number(playerGamePreview.tablebase_fens || 0)))}</strong>
                    </div>
                    <div><small>Expected complete games</small><strong>{formatNumber(playerGamePreview.selected_games)}</strong></div>
                    {playerGamePreview.selection_mode === 'fair_range' ? (
                      <div>
                        <small>Months sampled</small>
                        <strong>
                          {formatNumber(playerGamePreview.sampled_periods)} / {formatNumber(playerGamePreview.available_periods)}
                        </strong>
                      </div>
                    ) : null}
                    <div>
                      <small>Selection order</small>
                      <strong>{getPlayerGameSelectionLabel(playerGamePreview.selection_order)}</strong>
                    </div>
                    <div><small>Estimated total time</small><strong>~{formatDuration(playerGamePreviewTotalSeconds)}</strong></div>
                  </div>

                  <div className="player-game-runtime-fields">
                    <label>
                      <span className="field-label">Batch</span>
                      <input
                        className="text-input number-input-clean"
                        type="number"
                        min="1"
                        max={MAX_LOOP_ANALYSIS_BATCH_SIZE}
                        value={playerGameAnalysis.batchSize}
                        onChange={(event) => setPlayerGameAnalysis((current) => ({
                          ...current,
                          batchSize: clampAnalysisBatchInput(event.target.value, MAX_LOOP_ANALYSIS_BATCH_SIZE)
                        }))}
                      />
                    </label>
                    <label>
                      <span className="field-label">Cool-off every 5,000 FENs</span>
                      <input
                        className="text-input number-input-clean"
                        type="number"
                        min="0"
                        max="3600"
                        value={playerGameAnalysis.coolOff}
                        onChange={(event) => setPlayerGameAnalysis((current) => ({ ...current, coolOff: event.target.value }))}
                      />
                    </label>
                  </div>

                  <p>
                    The exact {formatNumber(playerGamePreview.selected_games)} game IDs are frozen. Shared FENs are analyzed once,
                    and the chosen game order is prioritized.
                  </p>
                  <div className="player-game-confirm-actions">
                    <button
                      className="btn btn-secondary"
                      type="button"
                      onClick={() => setPlayerGameAnalysis((current) => ({ ...current, preview: null }))}
                    >
                      Cancel
                    </button>
                    <button
                      className="btn btn-primary"
                      type="button"
                      disabled={jobState.gameCompletion?.loading}
                      onClick={handleConfirmPlayerGameAnalysis}
                    >
                      {jobState.gameCompletion?.loading ? 'Queueing' : 'Yes, queue analysis'}
                    </button>
                  </div>
                </div>
              ) : null}

              <JobStatus jobKey="gameCompletion" page={page} />
            </section>

            <section className="pipeline-card analysis-processes-card" aria-live="polite">
              <div className="analysis-processes-heading">
                <div>
                  <p className="eyebrow">ACTIVE ANALYSIS PROCESSES</p>
                  <small>Server-side Stockfish jobs currently running or waiting in the queue.</small>
                </div>
                <span className="stat-chip">{analysisProcessViews.length} processes</span>
              </div>

              {analysisProcessesError ? <div className="status-banner warn">{analysisProcessesError}</div> : null}
              {!analysisProcessesError && analysisProcessViews.length === 0 ? (
                <div className="analysis-process-empty">No running or queued Stockfish analysis.</div>
              ) : null}

              <div className="analysis-process-list">
                {analysisProcessViews.map((process) => (
                  <article className="analysis-process-card" key={process.job_id}>
                    <div className="analysis-process-title">
                      <div>
                        <strong>{process.scopeLabel}</strong>
                        <small>{String(process.job_id).slice(0, 12)}</small>
                      </div>
                      <span className={`analysis-process-phase ${process.phase}`}>{process.phase}</span>
                    </div>

                    <div className="analysis-process-parameters">
                      <span><small>Positions/run</small><strong>{formatNumber(process.positionsPerRun)}</strong></span>
                      <span><small>Runs</small><strong>{formatNumber(process.runs)}</strong></span>
                      <span><small>Batch</small><strong>{formatNumber(process.batchSize)}</strong></span>
                      <span><small>Cool-off</small><strong>{formatNumber(process.coolOff)}s</strong></span>
                    </div>

                    <div className="job-progress">
                      <div className="job-progress-head">
                        <span>{formatNumber(process.processed)} / {formatNumber(process.total)}</span>
                        <strong>{process.percent}%</strong>
                      </div>
                      <div className="job-progress-track">
                        <div className="job-progress-fill" style={{ width: `${process.percent}%` }} />
                      </div>
                      {process.progress?.detail ? <small>{process.progress.detail}</small> : null}
                      <EstimatedTime
                        jobId={process.job_id}
                        progress={process.progress}
                        status={process.status}
                        etaClockMs={etaClockMs}
                        etaEstimatesRef={etaEstimatesRef}
                      />
                    </div>

                    {process.isQueued ? (
                      <button
                        className="btn job-delete-queued"
                        type="button"
                        disabled={deletingAnalysisJobIds.has(process.job_id)}
                        onClick={() => handleDeleteAnalysisProcess(process)}
                      >
                        {deletingAnalysisJobIds.has(process.job_id) ? 'Deleting' : 'Delete queued analysis'}
                      </button>
                    ) : null}
                  </article>
                ))}
              </div>
            </section>
          </div>
        </main>
        <Footer />
      </div>
    </div>
  )
}
