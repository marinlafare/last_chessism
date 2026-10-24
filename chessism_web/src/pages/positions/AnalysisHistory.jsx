import { useEffect, useId, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { fetchAnalysisHistory, fetchCloudHistoryUsage } from './positionsApi'
import { formatNumber, getAnalysisProcessView } from './positionPageSupport'
import { AnalysisDisclosureGroup, useAnalysisDisclosure } from './AnalysisDisclosure'
import './analysisHistory.css'
import CloudRecoveryHistory from './CloudRecoveryHistory'

const BACKENDS = { local: 'Local', cloud_run: 'Cloud Run', batch_spot: 'Batch Spot' }
const dateLabel = value => value ? new Date(value).toLocaleString(undefined, {
  year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
}) : 'Not recorded'
const count = value => value == null ? 'Not recorded' : formatNumber(value)
const statusLabel = status => ({ complete: 'Completed', limit_reached: 'Completed · available FENs only',
  cancelled: 'Cancelled', failed: 'Failed', paused: 'Paused', waiting: 'Waiting',
  queued: 'Queued', running: 'Running' }[status] || status)
const phaseLabel = value => String(value || '').replaceAll('_', ' ').replace(/^./, letter => letter.toUpperCase())
const duration = value => {
  if (value == null) return 'Not recorded'
  const seconds = Math.max(0, Math.round(value))
  if (seconds < 60) return `${seconds}s`
  const minutes = Math.floor(seconds / 60), hours = Math.floor(minutes / 60)
  return `${hours ? `${hours}h ` : ''}${minutes % 60}m ${seconds % 60}s`
}

function normalizeJob(job, backend) {
  if (backend !== 'local') return { ...job,
    title: job.players?.length ? `Combined games · ${job.players.join(', ')}`
      : job.player_name ? `${job.mode === 'games' ? 'Player games' : 'Player'} · ${job.player_name}`
        : 'All positions',
  }
  const view = getAnalysisProcessView(job), kwargs = job.info?.kwargs || {}
  const result = job.result?.result
  return { id: job.job_id, status: job.status, title: view.scopeLabel,
    created_at: job.result?.start_time || job.info?.enqueue_time,
    finished_at: job.result?.finish_time,
    target: job.progress?.total ?? (view.total || null),
    imported: job.progress?.processed ?? result?.engine_processed ?? null,
    failed: job.progress?.failed ?? result?.failed,
    nodes: kwargs.nodes_limit,
    error: job.status === 'failed' ? result?.message || job.progress?.detail || 'Local analysis failed.' : null,
  }
}

function HistoryJob({ job, backend }) {
  const id = useId()
  const { open, toggle } = useAnalysisDisclosure(id)
  const [usage, setUsage] = useState(null)
  const [error, setError] = useState('')
  const [attempt, setAttempt] = useState(0)
  useEffect(() => {
    if (!open || backend === 'local') return
    const controller = new AbortController()
    setError('')
    fetchCloudHistoryUsage(job.id, { signal: controller.signal })
      .then(value => { if (!controller.signal.aborted) setUsage(value) })
      .catch(reason => { if (!controller.signal.aborted) setError(reason.message) })
    return () => controller.abort()
  }, [open, backend, job.id, job.status, attempt])
  const elapsed = job.finished_at && job.created_at
    ? (Date.parse(job.finished_at) - Date.parse(job.created_at)) / 1000 : null
  return <details {...pageStyleTarget(`history.${backend}.job`)} className="analysis-history-job" open={open}>
    <summary {...pageStyleTarget(`history.${backend}.toggle-entry`)} onClick={event => { event.preventDefault(); toggle() }}>
      <span className="analysis-history-name">{job.title}</span>
      <span className={`analysis-history-status is-${job.status}`}>{statusLabel(job.status)}</span>
      <time dateTime={job.created_at}>{dateLabel(job.created_at)}</time>
    </summary>
    <div className="analysis-history-detail">
      <dl className="analysis-history-metrics">
        <div><dt {...pageStyleTarget(`history.${backend}.imported.label`)}>FENs saved locally</dt><dd {...pageStyleTarget(`history.${backend}.imported.value`)}>{count(job.imported)}</dd></div>
        <div><dt {...pageStyleTarget(`history.${backend}.target.label`)}>Target FENs</dt><dd {...pageStyleTarget(`history.${backend}.target.value`)}>{count(job.target)}</dd></div>
        {job.nodes != null && <div><dt {...pageStyleTarget(`history.${backend}.nodes.label`)}>Nodes per FEN</dt><dd {...pageStyleTarget(`history.${backend}.nodes.value`)}>{count(job.nodes)}</dd></div>}
        {backend !== 'local' && <div><dt {...pageStyleTarget(`history.${backend}.compute.label`)}>{backend === 'cloud_run' ? 'Requested CPUs' : 'Requested VMs'}</dt>
          <dd {...pageStyleTarget(`history.${backend}.compute.value`)}>{count(backend === 'cloud_run' ? job.n_cpus : job.n_vms)}</dd></div>}
        {elapsed != null && <div><dt {...pageStyleTarget(`history.${backend}.elapsed.label`)}>Elapsed</dt><dd {...pageStyleTarget(`history.${backend}.elapsed.value`)}>{duration(elapsed)}</dd></div>}
        {job.failed > 0 && <div><dt {...pageStyleTarget(`history.${backend}.failed.label`)}>Failed FENs</dt><dd {...pageStyleTarget(`history.${backend}.failed.value`)}>{count(job.failed)}</dd></div>}
      </dl>
      {job.error && <p className="status-banner warn">{job.error}</p>}
      {backend !== 'local' && <>
        {!usage && !error && <p role="status">Loading recorded timings…</p>}
        {error && <p role="alert">{error}{' '}<button {...pageStyleTarget(`history.${backend}.retry-timings`)} type="button"
          onClick={() => setAttempt(value => value + 1)}>Retry timings</button></p>}
        {usage && <dl className="analysis-history-phases">
          {usage.phases.map(phase => <div key={phase.id}>
            <dt {...pageStyleTarget(`history.${backend}.phase.label`)}>{phaseLabel(phase.phase)}
              {phase.error && <span title={phase.error} aria-label={phase.error}> ×</span>}</dt>
            <dd {...pageStyleTarget(`history.${backend}.phase.value`)}>{phase.duration_seconds == null && !phase.finished_at
              ? phaseLabel(phase.outcome) : duration(phase.duration_seconds)}</dd>
          </div>)}
          {!usage.phases.length && <div><dt>No phase timings were recorded.</dt></div>}
        </dl>}
        {usage && <CloudRecoveryHistory stylePrefix={`history.${backend}.recovery`} recovery={usage.recovery} />}
      </>}
      <small {...pageStyleTarget(`history.${backend}.job-id`)} className="analysis-history-id">{job.id}</small>
    </div>
  </details>
}

export default function AnalysisHistory({ backend, revision = '' }) {
  const id = useId()
  const { open, toggle } = useAnalysisDisclosure(id)
  const [page, setPage] = useState(0)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [attempt, setAttempt] = useState(0)
  useEffect(() => {
    if (!open) return
    const controller = new AbortController()
    setLoading(true); setError('')
    fetchAnalysisHistory(backend, page * 20, { signal: controller.signal })
      .then(value => { if (!controller.signal.aborted) setData(value) })
      .catch(reason => { if (!controller.signal.aborted) setError(reason.message) })
      .finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [open, backend, page, revision, attempt])
  return <section className="analysis-history" aria-label={`${BACKENDS[backend]} history`}>
    <button {...pageStyleTarget(`history.${backend}.toggle`)} type="button" className="btn analysis-history-toggle"
      aria-expanded={open} aria-controls={id} onClick={toggle}>History</button>
    {open && <div id={id} className="analysis-history-body" aria-busy={loading}>
      {loading && !data && <p role="status">Loading history…</p>}
      {error && <div className="status-banner warn" role="alert">{error}{' '}
        <button {...pageStyleTarget(`history.${backend}.retry`)} type="button" onClick={() => setAttempt(value => value + 1)}>Retry</button></div>}
      {data?.note && <p {...pageStyleTarget(`history.${backend}.note`)} className="analysis-history-note">{data.note}</p>}
      {data?.scan_limited && <p className="status-banner warn">Only part of the retained queue history could be read. Older records may be missing.</p>}
      {data?.jobs.length === 0 && <p {...pageStyleTarget(`history.${backend}.empty`)}>No {backend === 'local' ? 'retained local' : BACKENDS[backend]} processes on this page.</p>}
      <AnalysisDisclosureGroup>
        {data?.jobs.map(job => <HistoryJob key={job.id || job.job_id} job={normalizeJob(job, backend)} backend={backend} />)}
      </AnalysisDisclosureGroup>
      {(page > 0 || data?.has_more) && <nav className="analysis-history-pagination" aria-label={`${BACKENDS[backend]} history pages`}>
        <button {...pageStyleTarget(`history.${backend}.newer`)} type="button" className="btn" disabled={loading || page === 0}
          onClick={() => setPage(value => value - 1)}>Newer</button>
        <button {...pageStyleTarget(`history.${backend}.older`)} type="button" className="btn" disabled={loading || !data?.has_more}
          onClick={() => setPage(value => value + 1)}>Older</button>
      </nav>}
    </div>}
  </section>
}
