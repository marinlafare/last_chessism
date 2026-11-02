import { useId, useRef, useState } from 'react'
import { usePoll } from '../../hooks/usePoll'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import ProgressMeter from '../../components/progress/ProgressMeter'
import { fetchCudaSummaries, runCudaSummaries, retryCudaSummaries } from './positionsApi'
import { cudaSummaryProgress } from './cudaSummaryProgress.js'
import CudaStepTitle from './CudaStepTitle.jsx'
import useCudaCompletedProgress from './useCudaCompletedProgress.js'
import './cuda-summaries.css'

const number = value => Number(value || 0).toLocaleString()
function Fill(props) { return <span {...pageStyleTarget('cuda.progress.fill')} {...props} /> }

export default function CudaSummaryPanel() {
  const { data, error, loading, refresh } = usePoll(fetchCudaSummaries, 5000, { requestTimeoutMs: 10000 })
  const [failure, setFailure] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [history, setHistory] = useState(false)
  const [emptyRequest, setEmptyRequest] = useState(false)
  const [collapsed, setCollapsed] = useState(false)
  const contentId = useId()
  const busy = useRef(false)
  const submit = async (id) => {
    if (busy.current) return
    setHistory(false)
    if (!id && !available) {
      setFailure('')
      setEmptyRequest(true)
      return
    }
    setEmptyRequest(false)
    busy.current = true; setSubmitting(true); setFailure('')
    try { await (id ? retryCudaSummaries(id) : runCudaSummaries()); refresh() }
    catch (e) { setFailure(e.message || 'Unable to request CUDA summaries.') }
    finally { busy.current = false; setSubmitting(false) }
  }
  const run = data?.active || data?.latest
  const showProgress = useCudaCompletedProgress(run)
  if (error?.status === 403) return null
  const pending = data?.pending_games || {}
  const progress = cudaSummaryProgress(run)
  const available = (data?.available_metrics || []).reduce((sum, metric) => sum + Number(pending[metric] || 0), 0)
    + Number(data?.pending_score_players || 0) + Number(data?.pending_accuracy_players || 0)
    + Number(data?.pending_behavioral_players || 0) + Number(data?.pending_unexpected_endings_players || 0)
    + Number(data?.pending_salience_players || 0) + Number(data?.pending_position_frequency_players || 0)
    + Number(Boolean(data?.ply_map?.pending && !data?.ply_map?.syncing))
  const waitingForGraph = Boolean(data?.ply_map?.pending && data?.ply_map?.syncing && !available)
  const noWork = emptyRequest && !data?.active && !available && !data?.preparing_games && !waitingForGraph
  const showHistory = Boolean(run && run.status !== 'complete') || noWork
  const hasContent = showProgress || noWork || showHistory || submitting || failure || error || data?.preparing_games > 0 || waitingForGraph
  const expanded = Boolean(data?.manual && !collapsed && hasContent)
  const toggle = () => {
    if (expanded) { setCollapsed(true); return }
    setCollapsed(false)
    // Opening an existing run is only a disclosure action, never another POST.
    if (busy.current || data?.active || showProgress || noWork || failure || error || data?.preparing_games > 0 || waitingForGraph) return
    submit()
  }
  return <section {...pageStyleTarget('cuda.panel')} className="cuda-summary-panel" aria-label="CUDA summaries">
    <div className="cuda-summary-heading">
      <button {...pageStyleTarget('cuda.run')} type="button" className="cuda-summary-toggle"
        aria-expanded={expanded} aria-controls={contentId} aria-label="Run CUDA summaries"
        disabled={loading || !data?.manual} onClick={toggle}>
        {Boolean(data?.active) && <span {...pageStyleTarget('cuda.run.indicator')} className="cuda-running-indicator"
          role="img" aria-label="CUDA summaries in progress" title="CUDA summaries in progress" />}
        <span {...pageStyleTarget('cuda.run.label')} className="cuda-summary-toggle-label">Run CUDA summaries</span>
      </button>
    </div>
    {expanded && <div {...pageStyleTarget('cuda.disclosure.panel')} id={contentId} className="cuda-summary-content">
      {submitting && !data.active && <CudaStepTitle title="Requesting CUDA summaries" active />}
      {!data.active && data.preparing_games > 0 && <p {...pageStyleTarget('cuda.preparing')} role="status">Preparing {number(data.preparing_games)} games</p>}
      {!data.active && data.ply_map?.pending && data.ply_map?.syncing && <p {...pageStyleTarget('cuda.preparing')} role="status">Waiting for graph synchronization</p>}
      {noWork ? <div {...pageStyleTarget('cuda.progress.panel')} className="cuda-summary-progress">
        <CudaStepTitle title="No pending work" active={false} />
      </div> : showProgress && <div {...pageStyleTarget('cuda.progress.panel')} className="cuda-summary-progress">
        <CudaStepTitle key={`${run.id}:${progress.phase}`} title={progress.phase} active={Boolean(data.active)} />
        <ProgressMeter {...pageStyleTarget('cuda.progress.track')} className="cuda-summary-track"
          fillTag={Fill} aria-label="Current CUDA stage" aria-valuetext={progress.detail || progress.phase}
          value={progress.value} max={progress.max} indeterminateWidth={progress.moving ? 30 : 0}
          data-indeterminate={progress.moving && progress.value == null} />
        {progress.detail && <span {...pageStyleTarget(progress.style)}>{progress.detail}</span>}
        {run.error && <p {...pageStyleTarget('cuda.error')} role="alert">{run.error}</p>}
        {run.status === 'failed' && <button {...pageStyleTarget('cuda.retry')} type="button"
          disabled={submitting || Boolean(data.active)} onClick={() => submit(run.id)}>Retry run</button>}
      </div>}
      {showHistory && <button {...pageStyleTarget('cuda.history.toggle')} type="button" aria-expanded={history}
        onClick={() => setHistory(open => !open)}>History</button>}
      {showHistory && history && <div {...pageStyleTarget('cuda.history.panel')} className="cuda-summary-history">
        {!data.history.length && <p {...pageStyleTarget('cuda.history.entry')}>No previous runs.</p>}
        {data.history.map(item => <article {...pageStyleTarget('cuda.history.entry')} key={item.id}>
          <span>{new Date(item.created_at).toLocaleString()} · {item.status} · {number(item.processed_games)} games processed</span>
          <span>Input: {Number(item.timings?.source_seconds || 0).toFixed(1)} s · CUDA: {Number(item.timings?.cuda_seconds || 0).toFixed(1)} s · Save: {Number(item.timings?.publication_seconds || 0).toFixed(1)} s
            {item.metrics?.includes('score_distribution') ? ' · Finalize: ' + Number(item.timings?.finalization_seconds || 0).toFixed(1) + ' s' : ''}</span>
          {item.timings?.metadata && <span>Metadata input: {Number(item.timings.metadata.preparation_seconds || 0).toFixed(1)} s · Calculate/stage: {Number(item.timings.metadata.cuda_seconds || 0).toFixed(1)} s · GPU batches: {number(item.timings.metadata.batches)}</span>}
          {item.timings?.salience && <span>Salience input: {Number(item.timings.salience.input_seconds || 0).toFixed(1)} s · CUDA: {Number(item.timings.salience.cuda_seconds || 0).toFixed(1)} s · GPU batches: {number(item.timings.salience.batches)}</span>}
          {item.timings?.position_frequency && <span>Frequency input: {Number(item.timings.position_frequency.input_seconds || 0).toFixed(1)} s · CUDA: {Number(item.timings.position_frequency.cuda_seconds || 0).toFixed(1)} s · Stored rows: {number(item.timings.position_frequency.stored_rows)}</span>}
        </article>)}
      </div>}
      {(failure || error) && <p {...pageStyleTarget('cuda.error')} role="alert">{failure || error.message}</p>}
      {failure && <button {...pageStyleTarget('cuda.retry')} type="button" disabled={submitting || Boolean(data.active)}
        onClick={() => submit()}>Retry request</button>}
    </div>}
    {!data?.manual && error && <p {...pageStyleTarget('cuda.error')} role="alert">{error.message}</p>}
  </section>
}
