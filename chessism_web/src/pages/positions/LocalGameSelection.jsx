import { pageStyleTarget } from '../../appearance/styleCore.js'
import { JobStatus } from './JobStatus'
import { MAX_LOOP_ANALYSIS_BATCH_SIZE, clampAnalysisBatchInput, formatDuration, formatNumber, getPlayerGameSelectionLabel } from './positionPageSupport'

export default function LocalGameSelection({ page }) {
  const { handleConfirmPlayerGameAnalysis, handleInspectPlayerGameScope, handlePreviewPlayerGameAnalysis, jobState, playerGameAnalysis, playerGamePreview, playerGamePreviewTotalSeconds, setPlayerGameAnalysis } = page
  return (
    <section {...pageStyleTarget('panel')} className="local-game-selection">
      <div className="analysis-processes-heading">
        <div>
          <p {...pageStyleTarget('text')} className="eyebrow">COMPLETE PLAYER GAMES</p>
          <small {...pageStyleTarget('text')}>Select exact incomplete games, preview their unique missing FENs, then confirm or cancel.</small>
        </div>
        {playerGameAnalysis.scope ? (
          <span className="stat-chip">
            {formatNumber(playerGameAnalysis.scope.incomplete_games)} incomplete games
          </span>
        ) : null}
      </div>

      <form className="player-game-scope-form" onSubmit={handleInspectPlayerGameScope}>
        <label {...pageStyleTarget('label')}>
          <span {...pageStyleTarget('label')} className="field-label">Chess.com nickname</span>
          <input {...pageStyleTarget('field')}
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
            <label {...pageStyleTarget('label')}
              className={`analysis-loop-scope-option ${playerGameAnalysis.selectionMode === value ? 'selected' : ''}`}
              key={value}
            >
              <input {...pageStyleTarget('field')}
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
              <span><strong {...pageStyleTarget('value')}>{label}</strong><small {...pageStyleTarget('text')}>{description}</small></span>
            </label>
          ))}
        </div>

        {playerGameAnalysis.selectionMode === 'range' ? (
          <div className="player-game-date-fields">
            <label {...pageStyleTarget('label')}>
              <span {...pageStyleTarget('label')} className="field-label">Start date</span>
              <input {...pageStyleTarget('field')}
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
            <label {...pageStyleTarget('label')}>
              <span {...pageStyleTarget('label')} className="field-label">End date</span>
              <input {...pageStyleTarget('field')}
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

        <button {...pageStyleTarget('button')} className="btn btn-secondary" type="submit" disabled={playerGameAnalysis.loadingScope}>
          {playerGameAnalysis.loadingScope ? 'Inspecting games' : 'Inspect games'}
        </button>
      </form>

      {playerGameAnalysis.error ? <div className="status-banner warn">{playerGameAnalysis.error}</div> : null}

      {playerGameAnalysis.scope ? (
        <div className="player-game-scope-results">
          <div><small {...pageStyleTarget('text')}>Games with FENs</small><strong {...pageStyleTarget('value')}>{formatNumber(playerGameAnalysis.scope.games_with_fens)}</strong></div>
          <div><small {...pageStyleTarget('text')}>Already complete</small><strong {...pageStyleTarget('value')}>{formatNumber(playerGameAnalysis.scope.complete_games)}</strong></div>
          <div><small {...pageStyleTarget('text')}>Incomplete available</small><strong {...pageStyleTarget('value')}>{formatNumber(playerGameAnalysis.scope.incomplete_games)}</strong></div>
          <div>
            <small {...pageStyleTarget('text')}>Available dates</small>
            <strong {...pageStyleTarget('value')}>
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
              <label {...pageStyleTarget('label')} className="player-game-check-option">
                <input {...pageStyleTarget('field')}
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
              <label {...pageStyleTarget('label')}>
                <span {...pageStyleTarget('label')} className="field-label">Number of games</span>
                <input {...pageStyleTarget('field')}
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
              <label {...pageStyleTarget('label')}>
                <span {...pageStyleTarget('label')} className="field-label">Take from range</span>
                <select {...pageStyleTarget('field')}
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

          <button {...pageStyleTarget('button')}
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
              <strong {...pageStyleTarget('value')}>{playerGamePreview.player_name}</strong>
            </div>
            <span className="analysis-process-phase queued">Awaiting confirmation</span>
          </div>

          <div className="player-game-preview-grid">
            <div><small {...pageStyleTarget('text')}>Games selected</small><strong {...pageStyleTarget('value')}>{formatNumber(playerGamePreview.selected_games)}</strong></div>
            <div><small {...pageStyleTarget('text')}>Position occurrences</small><strong {...pageStyleTarget('value')}>{formatNumber(playerGamePreview.position_occurrences)}</strong></div>
            <div><small {...pageStyleTarget('text')}>Unique FENs</small><strong {...pageStyleTarget('value')}>{formatNumber(playerGamePreview.unique_fens)}</strong></div>
            <div><small {...pageStyleTarget('text')}>Already analyzed</small><strong {...pageStyleTarget('value')}>{formatNumber(playerGamePreview.analyzed_unique_fens)}</strong></div>
            <div><small {...pageStyleTarget('text')}>Syzygy exact FENs</small><strong {...pageStyleTarget('value')}>{formatNumber(playerGamePreview.tablebase_fens)}</strong></div>
            <div className="highlight">
              <small {...pageStyleTarget('text')}>Estimated FENs for Stockfish</small>
              <strong {...pageStyleTarget('value')}>{formatNumber(Math.max(0, Number(playerGamePreview.fens_to_analyze || 0) - Number(playerGamePreview.tablebase_fens || 0)))}</strong>
            </div>
            <div><small {...pageStyleTarget('text')}>Expected complete games</small><strong {...pageStyleTarget('value')}>{formatNumber(playerGamePreview.selected_games)}</strong></div>
            {playerGamePreview.selection_mode === 'fair_range' ? (
              <div>
                <small {...pageStyleTarget('text')}>Months sampled</small>
                <strong {...pageStyleTarget('value')}>
                  {formatNumber(playerGamePreview.sampled_periods)} / {formatNumber(playerGamePreview.available_periods)}
                </strong>
              </div>
            ) : null}
            <div>
              <small {...pageStyleTarget('text')}>Selection order</small>
              <strong {...pageStyleTarget('value')}>{getPlayerGameSelectionLabel(playerGamePreview.selection_order)}</strong>
            </div>
            <div><small {...pageStyleTarget('text')}>Estimated total time</small><strong {...pageStyleTarget('value')}>~{formatDuration(playerGamePreviewTotalSeconds)}</strong></div>
          </div>

          <div className="player-game-runtime-fields">
            <label {...pageStyleTarget('label')}>
              <span {...pageStyleTarget('label')} className="field-label">Batch</span>
              <input {...pageStyleTarget('field')}
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
            <label {...pageStyleTarget('label')}>
              <span {...pageStyleTarget('label')} className="field-label">Cool-off every 5,000 FENs</span>
              <input {...pageStyleTarget('field')}
                className="text-input number-input-clean"
                type="number"
                min="0"
                max="3600"
                value={playerGameAnalysis.coolOff}
                onChange={(event) => setPlayerGameAnalysis((current) => ({ ...current, coolOff: event.target.value }))}
              />
            </label>
          </div>

          <p {...pageStyleTarget('text')}>
            The exact {formatNumber(playerGamePreview.selected_games)} game IDs are frozen. Shared FENs are analyzed once,
            and the chosen game order is prioritized.
          </p>
          <div className="player-game-confirm-actions">
            <button {...pageStyleTarget('button')}
              className="btn btn-secondary"
              type="button"
              onClick={() => setPlayerGameAnalysis((current) => ({ ...current, preview: null }))}
            >
              Cancel
            </button>
            <button {...pageStyleTarget('button')}
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
  )
}
