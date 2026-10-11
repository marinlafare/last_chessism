import { pageStyleTarget } from '../../appearance/styleCore.js'
import { JobStatus } from './JobStatus'
import { MAX_LOOP_ANALYSIS_BATCH_SIZE, clampAnalysisBatchInput, formatNumber } from './positionPageSupport'

export default function LocalLoopSelection({ page }) {
  const { activeLoopJobCount, handleAnalysisLoop, loopJob, loopJobIsQueueing, setLoopJob } = page
  return (
    <form className="local-selection-form analysis-loop-card" onSubmit={handleAnalysisLoop}>
      <div className="analysis-loop-heading">
        <div>
          <p {...pageStyleTarget('text')} className="eyebrow">ANALYSIS LOOPS</p>
          <small {...pageStyleTarget('text')}>Run sequential Stockfish passes with a cooling pause between them.</small>
        </div>
        <span className="stat-chip">
          {formatNumber(Number(loopJob.positionsPerRun || 0) * Number(loopJob.runs || 0))} total positions
        </span>
      </div>

      <div className="analysis-loop-scope" role="radiogroup" aria-label="Analysis loop scope">
        <label {...pageStyleTarget('label')} className={`analysis-loop-scope-option ${loopJob.scope === 'all' ? 'selected' : ''}`}>
          <input {...pageStyleTarget('field')}
            type="radio"
            name="analysis-loop-scope"
            value="all"
            checked={loopJob.scope === 'all'}
            onChange={() => setLoopJob((current) => ({ ...current, scope: 'all' }))}
          />
          <span>
            <strong {...pageStyleTarget('value')}>Analyze all</strong>
            <small {...pageStyleTarget('text')}>Most repeated positions across the database</small>
          </span>
        </label>
        <label {...pageStyleTarget('label')} className={`analysis-loop-scope-option ${loopJob.scope === 'player' ? 'selected' : ''}`}>
          <input {...pageStyleTarget('field')}
            type="radio"
            name="analysis-loop-scope"
            value="player"
            checked={loopJob.scope === 'player'}
            onChange={() => setLoopJob((current) => ({ ...current, scope: 'player' }))}
          />
          <span>
            <strong {...pageStyleTarget('value')}>Analyze player</strong>
            <small {...pageStyleTarget('text')}>Most repeated positions belonging to one player</small>
          </span>
        </label>
      </div>

      <p {...pageStyleTarget('text')} className="analysis-loop-selection" aria-live="polite">
        Selected scope: <strong {...pageStyleTarget('value')}>{loopJob.scope === 'player' ? 'one player' : 'all database positions'}</strong>
      </p>

      {loopJob.scope === 'player' ? (
        <label {...pageStyleTarget('label')}>
          <span {...pageStyleTarget('label')} className="field-label">Chess.com nickname</span>
          <input {...pageStyleTarget('field')}
            className="text-input"
            type="text"
            value={loopJob.playerName}
            onChange={(event) => setLoopJob((current) => ({ ...current, playerName: event.target.value }))}
            placeholder="...Chess.com nickname..."
          />
        </label>
      ) : null}

      <div className="analysis-loop-fields">
        <label {...pageStyleTarget('label')}>
          <span {...pageStyleTarget('label')} className="field-label">Positions / run</span>
          <input {...pageStyleTarget('field')}
            className="text-input number-input-clean"
            type="number"
            min="1"
            value={loopJob.positionsPerRun}
            onWheel={(event) => event.currentTarget.blur()}
            onChange={(event) => setLoopJob((current) => ({ ...current, positionsPerRun: event.target.value }))}
          />
        </label>
        <label {...pageStyleTarget('label')}>
          <span {...pageStyleTarget('label')} className="field-label">Runs</span>
          <input {...pageStyleTarget('field')}
            className="text-input number-input-clean"
            type="number"
            min="1"
            max="100"
            value={loopJob.runs}
            onWheel={(event) => event.currentTarget.blur()}
            onChange={(event) => setLoopJob((current) => ({ ...current, runs: event.target.value }))}
          />
        </label>
        <label {...pageStyleTarget('label')}>
          <span {...pageStyleTarget('label')} className="field-label">Batch</span>
          <input {...pageStyleTarget('field')}
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
        <label {...pageStyleTarget('label')}>
          <span {...pageStyleTarget('label')} className="field-label">Cool-off seconds</span>
          <input {...pageStyleTarget('field')}
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

      <p {...pageStyleTarget('text')} className="analysis-loop-note">
        Batch 500 is recommended because it commits progress more frequently while keeping all four engines busy. The maximum is 1,000.
        {' '}When a schedule is already running, the next one waits in the queue and starts automatically afterward.
      </p>
      <button {...pageStyleTarget('button')} className="btn btn-primary" type="submit" disabled={loopJobIsQueueing || activeLoopJobCount >= 2}>
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
