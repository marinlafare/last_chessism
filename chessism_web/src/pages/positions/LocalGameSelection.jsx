import { pageStyleTarget } from '../../appearance/styleCore.js'
import { JobStatus } from './JobStatus'
import { MAX_LOOP_ANALYSIS_BATCH_SIZE, clampAnalysisBatchInput, formatDuration, formatNumber, getPlayerGameSelectionLabel } from './positionPageSupport'

export default function LocalGameSelection({ page }) {
  const { handleConfirmPlayerGameAnalysis, handleInspectPlayerGameScope, handlePreviewPlayerGameAnalysis, jobState, playerGameAnalysis, playerGamePreview, playerGamePreviewTotalSeconds, setPlayerGameAnalysis } = page
  return (
    <section {...pageStyleTarget('local.games.panel')} className="local-game-selection">
      <div {...pageStyleTarget('local.games.heading')} className="analysis-processes-heading">
        <div>
          <p {...pageStyleTarget('local.games.title')} className="eyebrow">COMPLETE PLAYER GAMES</p>
          <small {...pageStyleTarget('local.games.note')}>Select exact incomplete games, preview their unique missing FENs, then confirm or cancel.</small>
        </div>
        {playerGameAnalysis.scope ? (
          <span {...pageStyleTarget('local.games.count-badge')} className="stat-chip">
            {formatNumber(playerGameAnalysis.scope.incomplete_games)} incomplete games
          </span>
        ) : null}
      </div>

      <form {...pageStyleTarget('local.games.form')} className="player-game-scope-form" onSubmit={handleInspectPlayerGameScope}>
        <label {...pageStyleTarget('local.games.player.group')}>
          <span {...pageStyleTarget('local.games.player.label')} className="field-label">Chess.com nickname</span>
          <input {...pageStyleTarget('local.games.player.input')}
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

        <div {...pageStyleTarget('local.games.modes')} className="player-game-mode" role="radiogroup" aria-label="Game selection method">
          {[
            ['latest', 'Latest games', 'Newest incomplete games first'],
            ['oldest', 'Oldest games', 'Oldest incomplete games first'],
            ['range', 'Date range', 'Inspect all games between two dates'],
            ['fair_range', 'Fair range', 'Balanced samples across every available month']
          ].map(([value, label, description]) => (
            <label {...pageStyleTarget('local.games.mode.group')}
              className={`analysis-loop-scope-option ${playerGameAnalysis.selectionMode === value ? 'selected' : ''}`}
              key={value}
            >
              <input {...pageStyleTarget('local.games.mode.input')}
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
              <span><strong {...pageStyleTarget('local.games.mode.title')}>{label}</strong><small {...pageStyleTarget('local.games.mode.description')}>{description}</small></span>
            </label>
          ))}
        </div>

        {playerGameAnalysis.selectionMode === 'range' ? (
          <div {...pageStyleTarget('local.games.dates')} className="player-game-date-fields">
            <label {...pageStyleTarget('local.games.from.group')}>
              <span {...pageStyleTarget('local.games.from.label')} className="field-label">Start date</span>
              <input {...pageStyleTarget('local.games.from.input')}
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
            <label {...pageStyleTarget('local.games.to.group')}>
              <span {...pageStyleTarget('local.games.to.label')} className="field-label">End date</span>
              <input {...pageStyleTarget('local.games.to.input')}
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

        <button {...pageStyleTarget('local.games.inspect')} className="btn btn-secondary" type="submit" disabled={playerGameAnalysis.loadingScope}>
          {playerGameAnalysis.loadingScope ? 'Inspecting games' : 'Inspect games'}
        </button>
      </form>

      {playerGameAnalysis.error ? <div className="status-banner warn">{playerGameAnalysis.error}</div> : null}

      {playerGameAnalysis.scope ? (
        <div {...pageStyleTarget('local.games.scope-summary')} className="player-game-scope-results">
          <div><small {...pageStyleTarget('local.games.available.label')}>Games with FENs</small><strong {...pageStyleTarget('local.games.available.value')}>{formatNumber(playerGameAnalysis.scope.games_with_fens)}</strong></div>
          <div><small {...pageStyleTarget('local.games.complete.label')}>Already complete</small><strong {...pageStyleTarget('local.games.complete.value')}>{formatNumber(playerGameAnalysis.scope.complete_games)}</strong></div>
          <div><small {...pageStyleTarget('local.games.incomplete.label')}>Incomplete available</small><strong {...pageStyleTarget('local.games.incomplete.value')}>{formatNumber(playerGameAnalysis.scope.incomplete_games)}</strong></div>
          <div>
            <small {...pageStyleTarget('local.games.dates.label')}>Available dates</small>
            <strong {...pageStyleTarget('local.games.dates.value')}>
              {playerGameAnalysis.scope.earliest_game
                ? `${String(playerGameAnalysis.scope.earliest_game).slice(0, 10)} → ${String(playerGameAnalysis.scope.latest_game).slice(0, 10)}`
                : '--'}
            </strong>
          </div>
        </div>
      ) : null}

      {playerGameAnalysis.scope && Number(playerGameAnalysis.scope.incomplete_games || 0) > 0 ? (
        <div {...pageStyleTarget('local.games.controls')} className="player-game-selection-controls">
          {playerGameAnalysis.selectionMode === 'range' ? (
            <div {...pageStyleTarget('local.games.range')} className="player-game-range-choice">
              <label {...pageStyleTarget('local.games.all-games.group')} className="player-game-check-option">
                <input {...pageStyleTarget('local.games.all-games.input')}
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

          <div {...pageStyleTarget('local.games.fields')} className="player-game-selection-fields">
            {(playerGameAnalysis.selectionMode !== 'range' || !playerGameAnalysis.useAll) ? (
              <label {...pageStyleTarget('local.games.count.group')}>
                <span {...pageStyleTarget('local.games.count.label')} className="field-label">Number of games</span>
                <input {...pageStyleTarget('local.games.count.input')}
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
              <label {...pageStyleTarget('local.games.order.group')}>
                <span {...pageStyleTarget('local.games.order.label')} className="field-label">Take from range</span>
                <select {...pageStyleTarget('local.games.order.input')}
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

          <button {...pageStyleTarget('local.games.calculate')}
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
        <div {...pageStyleTarget('local.games.confirmation')} className="player-game-confirmation">
          <div {...pageStyleTarget('local.games.confirmation-heading')} className="player-game-confirmation-heading">
            <div>
              <span>CONFIRM FROZEN ANALYSIS PLAN</span>
              <strong {...pageStyleTarget('local.games.player')}>{playerGamePreview.player_name}</strong>
            </div>
            <span className="analysis-process-phase queued">Awaiting confirmation</span>
          </div>

          <div {...pageStyleTarget('local.games.preview')} className="player-game-preview-grid">
            <div><small {...pageStyleTarget('local.games.selected.label')}>Games selected</small><strong {...pageStyleTarget('local.games.selected.value')}>{formatNumber(playerGamePreview.selected_games)}</strong></div>
            <div><small {...pageStyleTarget('local.games.occurrences.label')}>Position occurrences</small><strong {...pageStyleTarget('local.games.occurrences.value')}>{formatNumber(playerGamePreview.position_occurrences)}</strong></div>
            <div><small {...pageStyleTarget('local.games.unique.label')}>Unique FENs</small><strong {...pageStyleTarget('local.games.unique.value')}>{formatNumber(playerGamePreview.unique_fens)}</strong></div>
            <div><small {...pageStyleTarget('local.games.scored.label')}>Already analyzed</small><strong {...pageStyleTarget('local.games.scored.value')}>{formatNumber(playerGamePreview.analyzed_unique_fens)}</strong></div>
            <div><small {...pageStyleTarget('local.games.tablebase.label')}>Syzygy exact FENs</small><strong {...pageStyleTarget('local.games.tablebase.value')}>{formatNumber(playerGamePreview.tablebase_fens)}</strong></div>
            <div className="highlight">
              <small {...pageStyleTarget('local.games.stockfish.label')}>Estimated FENs for Stockfish</small>
              <strong {...pageStyleTarget('local.games.stockfish.value')}>{formatNumber(Math.max(0, Number(playerGamePreview.fens_to_analyze || 0) - Number(playerGamePreview.tablebase_fens || 0)))}</strong>
            </div>
            <div><small {...pageStyleTarget('local.games.expected.label')}>Expected complete games</small><strong {...pageStyleTarget('local.games.expected.value')}>{formatNumber(playerGamePreview.selected_games)}</strong></div>
            {playerGamePreview.selection_mode === 'fair_range' ? (
              <div>
                <small {...pageStyleTarget('local.games.months.label')}>Months sampled</small>
                <strong {...pageStyleTarget('local.games.months.value')}>
                  {formatNumber(playerGamePreview.sampled_periods)} / {formatNumber(playerGamePreview.available_periods)}
                </strong>
              </div>
            ) : null}
            <div>
              <small {...pageStyleTarget('local.games.selection-order.label')}>Selection order</small>
              <strong {...pageStyleTarget('local.games.selection-order.value')}>{getPlayerGameSelectionLabel(playerGamePreview.selection_order)}</strong>
            </div>
            <div><small {...pageStyleTarget('local.games.duration.label')}>Estimated total time</small><strong {...pageStyleTarget('local.games.duration.value')}>~{formatDuration(playerGamePreviewTotalSeconds)}</strong></div>
          </div>

          <div {...pageStyleTarget('local.games.runtime')} className="player-game-runtime-fields">
            <label {...pageStyleTarget('local.games.batch.group')}>
              <span {...pageStyleTarget('local.games.batch.label')} className="field-label">Batch</span>
              <input {...pageStyleTarget('local.games.batch.input')}
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
            <label {...pageStyleTarget('local.games.cool-off.group')}>
              <span {...pageStyleTarget('local.games.cool-off.label')} className="field-label">Cool-off every 5,000 FENs</span>
              <input {...pageStyleTarget('local.games.cool-off.input')}
                className="text-input number-input-clean"
                type="number"
                min="0"
                max="3600"
                value={playerGameAnalysis.coolOff}
                onChange={(event) => setPlayerGameAnalysis((current) => ({ ...current, coolOff: event.target.value }))}
              />
            </label>
          </div>

          <p {...pageStyleTarget('local.games.note-frozen')}>
            The exact {formatNumber(playerGamePreview.selected_games)} game IDs are frozen. Shared FENs are analyzed once,
            and the chosen game order is prioritized.
          </p>
          <div {...pageStyleTarget('local.games.actions')} className="player-game-confirm-actions">
            <button {...pageStyleTarget('local.games.cancel')}
              className="btn btn-secondary"
              type="button"
              onClick={() => setPlayerGameAnalysis((current) => ({ ...current, preview: null }))}
            >
              Cancel
            </button>
            <button {...pageStyleTarget('local.games.submit')}
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
