import { useEffect, useState } from 'react'
import { activeRun, duration, number } from './algorithmForm'

function RunCard({ run, clock, model }) {
  const active = activeRun(run)
  const progress = run.progress || {}
  const elapsed = run.started_at
    ? ((run.finished_at ? Date.parse(run.finished_at) : clock) - Date.parse(run.started_at)) / 1000
    : null
  return (
    <article className="algorithm-card">
      <div className="algorithm-heading">
        <h3>{run.name}</h3>
        <span className={'algorithm-status ' + (active ? 'is-active' : '')}>
          {run.cancel_requested && active ? 'cancelling' : run.status}
        </span>
      </div>
      <p>
        {new Date(run.created_at).toLocaleString()} · elapsed {duration(elapsed)} ·
        {' '}{progress.phase?.replaceAll('_', ' ')}
      </p>
      {active && progress.total > 0 ? (
        <>
          <progress value={progress.processed || 0} max={progress.total} />
          <p>
            {number(progress.processed || 0)} / {number(progress.total)}
            {progress.eta_seconds != null && <> · phase ETA {duration(progress.eta_seconds)}</>}
          </p>
        </>
      ) : active && progress.processed > 0 ? (
        <p>{number(progress.processed)} rows processed; source total not counted in advance.</p>
      ) : null}
      <p>{progress.detail}</p>
      {run.error && run.error !== progress.detail && <p className="algorithm-warning">{run.error}</p>}
      <div className="algorithm-actions">
        {active ? (
          <button type="button" disabled={Boolean(model.working) || run.cancel_requested}
            onClick={() => model.cancel(run)}>Cancel run</button>
        ) : (
          <button type="button" onClick={() => model.inspect(run.id)}>
            {run.status === 'complete' ? 'Show results' : 'Show details'}
          </button>
        )}
      </div>
    </article>
  )
}

function SavedAlgorithms({ model }) {
  return (
    <section className="algorithm-panel">
      <h2>Saved algorithms</h2>
      {!model.definitions.length && <p>No saved algorithms yet. Saving does not start a run.</p>}
      {model.definitions.map((definition) => (
        <article className="algorithm-card" key={definition.id}>
          <div className="algorithm-heading">
            <h3>{definition.name}</h3>
            <div className="algorithm-actions">
              <button type="button" disabled={Boolean(model.working)}
                onClick={() => model.openDefinition(definition)}>Open / new revision</button>
              <button type="button" disabled={Boolean(model.working)}
                onClick={() => model.run(definition)}>Run</button>
              <button type="button" disabled={Boolean(model.working)}
                onClick={() => model.remove(definition)}>Delete instructions</button>
            </div>
          </div>
          <p>
            {definition.config.matrix.row_type} · {definition.config.columns.join(', ')}
            {definition.config.rating_difference ? ', rating_difference' : ''}
          </p>
          <p>
            {number(definition.config.max_rows)} rows maximum ·
            {' '}{definition.config.missing === 'drop_rows' ? 'exclude incomplete rows' : definition.config.missing === 'keep' ? 'keep missing as N/A' : 'column-mean imputation'} ·
            {' '}{definition.config.operation === 'pipeline'
              ? `${definition.config.steps.length} steps · ${definition.config.outputs.length} outputs · revision ${definition.config.revision || 1}`
              : 'legacy Feature relationships'}
          </p>
        </article>
      ))}
      {model.definitionMore && (
        <button type="button" disabled={Boolean(model.working)}
          onClick={model.moreDefinitions}>Load more algorithms</button>
      )}
    </section>
  )
}

export default function AlgorithmHistory({ model }) {
  const [clock, setClock] = useState(Date.now())
  const hasActive = model.runs.some(activeRun)
  useEffect(() => {
    if (!hasActive) return undefined
    const timer = setInterval(() => setClock(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [hasActive])
  return (
    <>
      <SavedAlgorithms model={model} />
      <section className="algorithm-panel" aria-label="Algorithm runs">
        <div className="algorithm-heading">
          <h2>Runs</h2><button type="button" onClick={model.refresh}>Refresh</button>
        </div>
        <p>
          Progress is recorded by the worker. Closing this page does not stop a run.
          Time estimates apply only to a phase with a known total.
        </p>
        {!model.runs.length && <p>No runs on this page.</p>}
        {model.runs.map((run) => <RunCard key={run.id} run={run} clock={clock} model={model} />)}
        <div className="algorithm-actions">
          <button type="button" disabled={model.runOffset === 0}
            onClick={() => model.setRunOffset(Math.max(0, model.runOffset - 30))}>Newer</button>
          <button type="button" disabled={!model.runMore}
            onClick={() => model.setRunOffset(model.runOffset + 30)}>Older</button>
        </div>
      </section>
    </>
  )
}
