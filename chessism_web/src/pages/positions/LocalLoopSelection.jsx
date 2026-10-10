import { JobStatus } from './JobStatus'
import { MAX_LOOP_ANALYSIS_BATCH_SIZE, clampAnalysisBatchInput, formatNumber } from './positionPageSupport'

export default function LocalLoopSelection({ page }) {
  const { activeLoopJobCount, handleAnalysisLoop, loopJob, loopJobIsQueueing, setLoopJob } = page
  return (
    <form className="local-selection-form analysis-loop-card" onSubmit={handleAnalysisLoop}>
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
  )
}
