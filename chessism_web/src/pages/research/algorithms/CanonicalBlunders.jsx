import { useEffect, useState } from 'react'
import { pageStyleTarget } from '../../../appearance/styleCore.js'
import { fetchCanonicalBlunders } from './algorithmApi.js'

export default function CanonicalBlunders({ onBenchmark, disabled = false }) {
  const [data, setData] = useState(null)
  const [error, setError] = useState('')
  const [loaded, setLoaded] = useState(false)
  const [revision, setRevision] = useState(0)
  useEffect(() => {
    let stopped = false, timer, request
    const poll = async () => {
      request = new AbortController()
      const timeout = setTimeout(() => request.abort(), 10000)
      try {
        const result = await fetchCanonicalBlunders({ signal: request.signal })
        if (!stopped) { setData(result); setError('') }
      } catch (e) {
        if (!stopped) setError(request.signal.aborted ? 'Progress request timed out. Retrying automatically.' : e.message || 'Unable to read canonical status.')
      } finally {
        clearTimeout(timeout)
        if (!stopped) { setLoaded(true); timer = setTimeout(poll, 5000) }
      }
    }
    poll()
    return () => { stopped = true; request?.abort(); clearTimeout(timer) }
  }, [revision])
  if (loaded && !data?.target_model && !error) return null
  const processed = Number(data?.progress?.processed_games || 0)
  const pending = Number(data?.pending_games || 0)
  const total = processed + pending
  const published = data?.active_model === data?.target_model && data?.status === 'ready' && pending === 0
  const percent = published ? 100 : total ? Math.min(99.9, processed / total * 100) : null
  const waiting = data?.manual_summaries?.manual && !data.manual_summaries.active && pending > 0
  const phase = waiting ? 'Awaiting manual run' : data?.status === 'retrying' ? 'Retrying' : published ? 'Complete' : data?.active_model ? 'Updating' : 'Running'
  return <section {...pageStyleTarget('panel')} className="algorithm-panel algorithm-canonical-progress" aria-label="Canonical GPU blunders">
    <div className="algorithm-heading">
      <h2 {...pageStyleTarget('title')}>Blunder summaries · RTX 4060</h2>
      <div className="algorithm-actions">
        {data?.target_model && <span className={`algorithm-status${published ? '' : ' is-active'}`}>{phase}</span>}
        <button {...pageStyleTarget('button')} type="button" onClick={() => setRevision(value => value + 1)}>Refresh progress</button>
        {onBenchmark && <button {...pageStyleTarget('button')} type="button" disabled={disabled || !published}
          onClick={onBenchmark} title="Time 100,000 games on the RTX 4060 without changing published summaries">Measure pipeline</button>}
      </div>
    </div>
    {!loaded && <p {...pageStyleTarget('text')} role="status">Loading GPU progress…</p>}
    {error && <p role="alert">{error}</p>}
    {data?.target_model && <>
      <div {...pageStyleTarget('card')} className="algorithm-progress-track" role="progressbar"
        aria-label="Blunder summary rebuild" aria-valuemin={0} aria-valuemax={100}
        aria-valuenow={percent == null ? undefined : Math.round(percent * 10) / 10}
        aria-valuetext={`${processed.toLocaleString()} games processed; ${pending.toLocaleString()} pending${published ? '; published' : ''}`}>
        <span {...pageStyleTarget('value')} className="algorithm-progress-fill" style={{ width: `${percent || 0}%` }} />
      </div>
      <div className="algorithm-progress-counts" role="status">
        <strong {...pageStyleTarget('value')}>{processed.toLocaleString()} / {total.toLocaleString()} games processed</strong>
        <span {...pageStyleTarget('value')}>{percent == null ? 'Preparing…' : `${percent.toFixed(1)}%`}</span>
      </div>
      <p {...pageStyleTarget('text')}>{pending.toLocaleString()} games pending · {published ? 'Published — the new summaries are active.'
        : waiting ? 'Use Run CUDA summaries on Analyze Positions. Previous results are retained.'
          : data.active_model ? 'Updating saved summaries — previous published results retained.' : 'Initial rebuild — previous displayed results retained.'}</p>
      {data.updated_at && <p {...pageStyleTarget('text')}>Last saved batch: {new Date(data.updated_at).toLocaleTimeString()}</p>}
      {data.pending_input_refresh_games > 0 && <p {...pageStyleTarget('text')} role="status">
        Preparing game coverage before CUDA: {Number(data.pending_input_refresh_games).toLocaleString()} pending.
        {data.input_refresh_errors > 0 ? ' Failed refresh requests are retained and will retry automatically.' : ''}
      </p>}
      <p {...pageStyleTarget('text')}>Rating-aware · threshold 0.15 · provisional user approval</p>
      {data.error && <p role="alert">{data.error} Previous results are retained; automatic retry is pending.</p>}
      <details><summary {...pageStyleTarget('disclosure')}>Model version and limitations</summary>
        <p {...pageStyleTarget('text')}>{Number(data.progress?.cuda_batch_rows || 0).toLocaleString()} maximum CUDA batch rows</p>
        <p {...pageStyleTarget('text')}>{data.target_model}</p>
        {data.evidence?.limitations?.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
        <div className="algorithm-table-wrap"><table><thead><tr><th>Mode</th><th>Average-rating coefficient</th><th>Rating-gap coefficient</th></tr></thead>
          <tbody>{data.config?.fits?.map(f => <tr key={f.mode}><th>{f.mode}</th><td>{f.a}</td><td>{f.b}</td></tr>)}</tbody>
        </table></div>
      </details>
    </>}
  </section>
}
