import { useCallback, useEffect, useRef, useState } from 'react'
import {
  createCloudAnalysis, fetchCloudAnalysis, resumeCloudAnalysis, retryCloudAnalysis, cancelCloudAnalysis,
  previewPlayerGameAnalysis, fetchPlayerAnalysisCounts,
} from './positionsApi'
import { cloudDefaults, cloudPayload, cloudGamePreview, cloudBound, cloudGameSelectionError,
  MAX_CLOUD_FENS, MAX_CLOUD_RUNS } from './cloudAnalysisForm'
import { DEFAULT_ANALYSIS_NODES } from './positionPageSupport'
import { blockingCloudJob, cloudIsBusy } from './cloudJobState'
import CloudFenProgress from './CloudFenProgress'
import { CloudPerformanceSummary } from './CloudPerformanceSummary'
import './cloudAnalysis.css'

export default function CloudAnalysisSection({ onProgress }) {
  const [data, setData] = useState({ controller_online: false, jobs: [] })
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const onProgressRef = useRef(onProgress)
  const progressStamp = useRef(null)
  onProgressRef.current = onProgress
  const refresh = useCallback(async () => {
    const next = await fetchCloudAnalysis()
    setData(next)
    setError('')
  }, [])
  useEffect(() => {
    let stopped = false
    let timer
    const poll = async () => {
      try {
        const next = await fetchCloudAnalysis()
        if (!stopped) {
          setData(next); setError('')
          const stamp = next.jobs.map((job) => `${job.id}:${job.imported}:${job.status}:${job.runs.map((run) => run.status).join(',')}`).join('|')
          if (progressStamp.current !== null && progressStamp.current !== stamp) onProgressRef.current?.()
          progressStamp.current = stamp
        }
      } catch (err) {
        if (!stopped) { setError(err.message); setData((old) => ({ ...old, controller_online: false })) }
      }
      if (!stopped) timer = setTimeout(poll, 5000)
    }
    poll()
    return () => { stopped = true; clearTimeout(timer) }
  }, [])
  const action = async (fn) => {
    setBusy(true)
    try { await fn(); await refresh() } catch (err) { setError(err.message) }
    finally { setBusy(false) }
  }
  const activeJob = blockingCloudJob(data)
  const cloudBusy = cloudIsBusy(data)
  return (
    <section className="cloud-analysis" aria-label="Google Cloud analysis">
      <h2>GOOGLE CLOUD ANALYSIS</h2>
      <p>Same FEN selection, same database results. Local analysis can run alongside cloud jobs;
        reserved FENs are skipped by the other workers.</p>
      <p role="status">Controller: {data.controller_online ? 'online' : 'offline — start cloud-controller with Docker Compose'}.</p>
      <p>Billable Spot compute: one 4-vCPU VM at a time, uploads every 500 completed results.
        {' '}{DEFAULT_ANALYSIS_NODES.toLocaleString('en-US')} nodes per FEN (fixed).
        The whole selection shares one VM and four workers; Spot retries may replace the VM.
        Google-confirmed Spot interruptions resume automatically in the cloud without using the
        application retry allowance (two application attempts total).
        Successful jobs clean up automatically after the final database import.</p>
      {error && <div role="alert" className="status-banner warn">{error}</div>}
      {cloudBusy && <p role="status" className="cloud-busy-notice">
        A cloud job is {activeJob?.status || 'active'}. New cloud selections are disabled until it finishes
        importing and cleaning up. Paused/failed jobs must be recovered first. Local analysis remains available.
      </p>}
      <div className="cloud-analysis-grid">
        {['all', 'player', 'loop', 'games'].map((mode) => (
          <CloudSelection key={mode} mode={mode} disabled={busy || !data.controller_online || cloudBusy}
            job={activeJob?.selection.mode === mode ? activeJob : null}
            create={(payload) => action(() => createCloudAnalysis(payload))} />
        ))}
      </div>
      <h3>CLOUD JOBS</h3>
      {!data.jobs.length && <p>No cloud jobs requested yet.</p>}
      {data.jobs.map((job) => {
        const completed = job.runs.filter((run) => run.status === 'complete').length
        const active = job.runs.find((run) => run.status !== 'complete')
        return (
          <article className="cloud-job" key={job.id}>
            <strong>{job.selection.mode} {job.selection.player_name} — {job.status.replaceAll('_', ' ')}</strong>
            <small>{job.id}</small>
            <CloudFenProgress job={job} />
            <p>Cloud: {active?.cloud_state || (active ? 'preparing' : 'idle')}.
              {' '}Stage: {active?.status.replaceAll('_', ' ') || job.status.replaceAll('_', ' ')}.
              {' '}Cleanup: {completed} / {job.runs.length} executions finished.</p>
            {job.runs.map((run) => <CloudPerformanceSummary key={run.id} run={run} />)}
            {active?.batch_jobs.map((id) => <small key={id}>Batch: {id}</small>)}
            {active?.recovery && <p>Google interruptions: {active.recovery.preemptions}.
              {' '}Application failures: {active.recovery.application_failures} / 2.
              {' '}Recovery: {active.recovery.status.toLowerCase()}.</p>}
            {job.error && <p className="status-banner warn">{job.error}</p>}
            {['paused', 'waiting'].includes(job.status) && (
              <button className="btn" disabled={busy || !data.controller_online || (activeJob && activeJob.id !== job.id)}
                onClick={() => action(() => resumeCloudAnalysis(job.id))}>Resume saved job</button>
            )}
            {active?.recovery_session && active.status === 'running' && ['running', 'paused'].includes(job.status) && (
              <button className="btn btn-secondary" disabled={busy || !data.controller_online || job.selection.cancel_requested}
                onClick={() => action(() => cancelCloudAnalysis(job.id))}>
                {job.selection.cancel_requested ? 'Cancellation requested' : 'Stop cloud job and recovery'}
              </button>
            )}
            {job.status === 'failed' && (active?.recovery_session ? active.recovery_session < 3 : active?.batch_jobs.length < 3) && (
              <button className="btn" disabled={busy || !data.controller_online || (activeJob && activeJob.id !== job.id)}
                onClick={() => action(() => retryCloudAnalysis(job.id))}>Retry cloud — billable</button>
            )}
          </article>
        )
      })}
      <p>Temporary job data and its image are deleted; the empty bucket/repository, recovery workflow and permissions remain.
        Eligible, job-owned VM log streams are removed after a performance report is saved locally.
        Shared logs and Google audit/billing/monitoring/workflow history remain. Failed jobs retain checkpoints for recovery.</p>
    </section>
  )
}

function CloudSelection({ mode, disabled, create, job }) {
  const [form, setForm] = useState({ ...cloudDefaults })
  const [preview, setPreview] = useState(null)
  const [inspection, setInspection] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const titles = { all: 'ANALYZE ALL', player: 'ANALYZE PLAYER', loop: 'ANALYSIS LOOPS', games: 'COMPLETE PLAYER GAMES' }
  const change = (key, value) => {
    setForm((current) => ({ ...current, [key]: value }))
    setPreview(null)
    setInspection(null)
    setError('')
  }
  const field = (key, label, max, min = 1) => (
    <label><span className="field-label">{label}</span>
      <input className="text-input" type="number" min={min} max={max} required
        value={form[key]} onChange={(event) => change(key, event.target.value)} /></label>
  )
  const playerVisible = mode !== 'all' && (mode !== 'loop' || form.scope === 'player')
  const inspect = async () => {
    if (disabled || loading) return
    setLoading(true); setError('')
    try {
      if (mode === 'games') setPreview(await previewPlayerGameAnalysis(cloudGamePreview(form)))
      else setInspection(await fetchPlayerAnalysisCounts(form.player.trim()))
    } catch (err) { setError(err.message) }
    finally { setLoading(false) }
  }
  return (
    <form className="cloud-selection" onSubmit={(event) => {
      event.preventDefault()
      if (disabled || loading) return
      try { create(cloudPayload(mode, form, preview)) } catch (err) { setError(err.message) }
    }}>
      <h3>{titles[mode]} · CLOUD</h3>
      {job && <><p role="status">Current job: {job.status.replaceAll('_', ' ')}</p><CloudFenProgress job={job} /></>}
      <fieldset disabled={disabled || loading} aria-label={`${titles[mode]} cloud controls`}>
      {mode === 'loop' && <label>Scope
        <select className="text-input" value={form.scope} onChange={(event) => change('scope', event.target.value)}>
          <option value="all">All positions</option><option value="player">Player</option>
        </select></label>}
      {playerVisible && <label>Player
        <input className="text-input" value={form.player} required maxLength={200}
          onChange={(event) => change('player', event.target.value)} placeholder="chess.com nickname" />
      </label>}
      {playerVisible && mode !== 'games' && <button className="btn btn-secondary" type="button"
        disabled={loading || !form.player.trim()} onClick={inspect}>Inspect player</button>}
      {inspection && <p>{inspection.total_positions} positions; {inspection.analyzed_positions} analyzed.</p>}
      {mode === 'games' && <>
        <label>Game selection
          <select className="text-input" value={form.selection} onChange={(event) => change('selection', event.target.value)}>
            <option value="latest">Latest games</option><option value="oldest">Oldest games</option>
            <option value="range">Date range</option><option value="fair_range">Fair range</option>
          </select></label>
        {['range', 'fair_range'].includes(form.selection) && <>
          <label>From<input className="text-input" type="date" value={form.from}
            onChange={(event) => change('from', event.target.value)} /></label>
          <label>To<input className="text-input" type="date" value={form.to}
            onChange={(event) => change('to', event.target.value)} /></label>
        </>}
        {mode === 'games' && form.selection === 'range' && <>
          <label><input type="checkbox" checked={form.allGames}
            onChange={(event) => change('allGames', event.target.checked)} /> All games in range</label>
          <select className="text-input" aria-label="Range order" value={form.order}
            onChange={(event) => change('order', event.target.value)}>
            <option value="latest">Newest first</option><option value="oldest">Oldest first</option>
          </select>
        </>}
        {!(form.selection === 'range' && form.allGames) && field('games', 'Games', 50000)}
      </>}
      {mode === 'loop' ? <>{field('positions', 'Positions per group', 1000)}{field('runs', 'Groups (combined into one job)', MAX_CLOUD_RUNS)}</>
        : mode !== 'games' && field('total', 'Positions', MAX_CLOUD_FENS)}
      {mode === 'games' && <button className="btn btn-secondary" type="button"
        disabled={loading || !form.player.trim()} onClick={inspect}>Preview games</button>}
      {preview && <p>Frozen preview: {preview.selected_games} games, {preview.fens_to_analyze} missing FENs.
        The system sets the FEN count from this selection. Selections over {MAX_CLOUD_FENS.toLocaleString('en-US')} FENs cannot be submitted.</p>}
      <small>{cloudBound(mode, form, preview)}</small>
      {error && <p role="alert" className="status-banner warn">{error}</p>}
      <button className="btn" disabled={disabled || loading || (mode === 'games' && !!cloudGameSelectionError(preview))}>
        Create cloud job — billable
      </button>
      </fieldset>
    </form>
  )
}
