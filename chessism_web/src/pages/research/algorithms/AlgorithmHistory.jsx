import { pageStyleTarget } from '../../../appearance/styleCore.js'
import { useEffect, useId, useRef, useState } from 'react'
import { activeRun, duration, number } from './algorithmForm'
import AlgorithmResults from './AlgorithmResults'

function RunCard({ run, clock, model }) {
  const active = activeRun(run)
  const progress = run.progress || {}
  const elapsed = run.started_at
    ? ((run.finished_at ? Date.parse(run.finished_at) : clock) - Date.parse(run.started_at)) / 1000
    : null
  return (
    <article {...pageStyleTarget('card')} className="algorithm-card">
      <div className="algorithm-heading">
        <h3 {...pageStyleTarget('title')}>{run.name}</h3>
        <span className={'algorithm-status ' + (active ? 'is-active' : '')}>
          {run.cancel_requested && active ? 'cancelling' : run.status}
        </span>
      </div>
      <p {...pageStyleTarget('text')}>
        {new Date(run.created_at).toLocaleString()} · elapsed {duration(elapsed)} ·
        {' '}{progress.phase?.replaceAll('_', ' ')}
      </p>
      {active && progress.total > 0 ? (
        <>
          <progress value={progress.processed || 0} max={progress.total} />
          <p {...pageStyleTarget('text')}>
            {number(progress.processed || 0)} / {number(progress.total)}
            {progress.eta_seconds != null && <> · phase ETA {duration(progress.eta_seconds)}</>}
          </p>
        </>
      ) : active && progress.processed > 0 ? (
        <p {...pageStyleTarget('text')}>{number(progress.processed)} rows processed; source total not counted in advance.</p>
      ) : null}
      <p {...pageStyleTarget('text')}>{progress.detail}</p>
      {run.error && run.error !== progress.detail && <p {...pageStyleTarget('text')} className="algorithm-warning">{run.error}</p>}
      <div className="algorithm-actions">
        {active ? (
          <button {...pageStyleTarget('button')} type="button" disabled={Boolean(model.working) || run.cancel_requested}
            onClick={() => model.cancel(run)}>Cancel run</button>
        ) : (
          <button {...pageStyleTarget('button')} type="button" onClick={() => model.inspect(run.id)}>
            {run.status === 'complete' ? 'Show results' : 'Show details'}
          </button>
        )}
      </div>
    </article>
  )
}

export function SavedAlgorithms({ model }) {
  return (
    <section {...pageStyleTarget('panel')} className="algorithm-panel">
      <h2 {...pageStyleTarget('title')}>Saved algorithms</h2>
      {!model.definitions.length && <p {...pageStyleTarget('text')}>No saved algorithms yet. Saving does not start a run.</p>}
      {model.definitions.map((definition) => (
        <article {...pageStyleTarget('card')} className="algorithm-card" key={definition.id}>
          <div className="algorithm-heading">
            <h3 {...pageStyleTarget('title')}>{definition.name}</h3>
            <div className="algorithm-actions">
              <button {...pageStyleTarget('button')} type="button" disabled={Boolean(model.working)}
                onClick={() => model.openDefinition(definition)}>Open / new revision</button>
              <button {...pageStyleTarget('button')} type="button" disabled={Boolean(model.working)}
                onClick={() => model.run(definition)}>Run</button>
              <button {...pageStyleTarget('button')} type="button" disabled={Boolean(model.working)}
                onClick={() => model.remove(definition)}>Delete instructions</button>
            </div>
          </div>
          <p {...pageStyleTarget('text')}>
            {definition.config.matrix.row_type} · {definition.config.columns.join(', ')}
            {definition.config.rating_difference ? ', rating_difference' : ''}
          </p>
          <p {...pageStyleTarget('text')}>
            {number(definition.config.max_rows)} rows maximum ·
            {' '}{definition.config.missing === 'drop_rows' ? 'exclude incomplete rows' : definition.config.missing === 'keep' ? 'keep missing as N/A' : 'column-mean imputation'} ·
            {' '}{definition.config.operation === 'pipeline'
              ? `${definition.config.steps.length} steps · ${definition.config.outputs.length} outputs · revision ${definition.config.revision || 1}`
              : 'legacy Feature relationships'}
          </p>
        </article>
      ))}
      {model.definitionMore && (
        <button {...pageStyleTarget('button')} type="button" disabled={Boolean(model.working)}
          onClick={model.moreDefinitions}>Load more algorithms</button>
      )}
    </section>
  )
}

export function ActiveAlgorithmRuns({ model }) {
  const [clock, setClock] = useState(Date.now())
  const runs = (model.activeRuns || model.runs).filter(activeRun)
  const hasActive = runs.length > 0
  useEffect(() => {
    if (!hasActive) return undefined
    const timer = setInterval(() => setClock(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [hasActive])
  if (!hasActive) return null
  return <section {...pageStyleTarget('panel')} className="algorithm-panel" aria-label="Active algorithm runs">
    <div className="algorithm-heading">
      <h2 {...pageStyleTarget('title')}>Active runs</h2>
      <button {...pageStyleTarget('button')} type="button" onClick={model.refresh}>Refresh</button>
    </div>
    {runs.map(run => <RunCard key={run.id} run={run} clock={clock} model={model} />)}
  </section>
}

export default function AlgorithmHistory({ model }) {
  const [open, setOpen] = useState(false)
  const id = useId()
  const resultsRef = useRef(null)
  const runs = model.runs.filter(run => !activeRun(run))
  useEffect(() => { if (model.result) setOpen(true) }, [model.result?.id])
  useEffect(() => {
    if (open && model.result && resultsRef.current) {
      resultsRef.current.scrollIntoView({ behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'start' })
      resultsRef.current.focus({ preventScroll: true })
    }
  }, [open, model.result?.id])
  return (
    <section {...pageStyleTarget('history.panel')} className="algorithm-panel algorithm-history" aria-label="Algorithm history">
      <button {...pageStyleTarget('button')} className="algorithm-history-toggle" type="button"
        id={`${id}-toggle`} aria-expanded={open} aria-controls={`${id}-content`} onClick={() => setOpen(value => !value)}>
        <span>History</span><svg viewBox="0 0 20 20" aria-hidden="true"><path d="m5 7.5 5 5 5-5" /></svg>
      </button>
      <div id={`${id}-content`} className="algorithm-history-content" hidden={!open}
        role="region" aria-labelledby={`${id}-toggle`}>
        <div className="algorithm-heading">
          <h2 {...pageStyleTarget('title')}>Previous runs</h2><button {...pageStyleTarget('button')} type="button" onClick={model.refresh}>Refresh history</button>
        </div>
        {!runs.length && <p {...pageStyleTarget('text')}>No previous runs on this page.</p>}
        {runs.map(run => <RunCard key={run.id} run={run} clock={Date.now()} model={model} />)}
        <div className="algorithm-actions">
          <button {...pageStyleTarget('button')} type="button" disabled={model.runOffset === 0}
            onClick={() => model.setRunOffset(Math.max(0, model.runOffset - 30))}>Newer</button>
          <button {...pageStyleTarget('button')} type="button" disabled={!model.runMore}
            onClick={() => model.setRunOffset(model.runOffset + 30)}>Older</button>
        </div>
        {model.result && <div ref={resultsRef} tabIndex="-1"><AlgorithmResults run={model.result} onClose={() => model.setResult(null)} /></div>}
      </div>
    </section>
  )
}
