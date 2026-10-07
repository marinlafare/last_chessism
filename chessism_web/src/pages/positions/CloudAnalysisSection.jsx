import { useCallback, useEffect, useRef, useState } from 'react'
import {
  createCloudAnalysis, fetchCloudAnalysis, resumeCloudAnalysis, retryCloudAnalysis, cancelCloudAnalysis,
  previewPlayerGameAnalysis, fetchPlayerAnalysisCounts, reviseCloudVms,
} from './positionsApi'
import { cloudDefaults, cloudRunDefaults, cloudPayload, cloudGamePreview, cloudBound, cloudGameSelectionError,
  MAX_CLOUD_FENS, MAX_CLOUD_RUNS, MAX_BATCH_VMS } from './cloudAnalysisForm'
import { DEFAULT_ANALYSIS_NODES } from './positionPageSupport'
import { blockingCloudJob, cloudIsBusy } from './cloudJobState'
import CloudFenProgress from './CloudFenProgress'
import CloudJobSteps from './CloudJobSteps'
import { cloudJobProgress } from './cloudJobProgress'
import useCloudJobDisplay from './useCloudJobDisplay'
import { CloudPerformanceSummary } from './CloudPerformanceSummary'
import './cloudAnalysis.css'

export default function CloudAnalysisSection({ onProgress }) {
  const [data, setData] = useState({ controller_online: false, jobs: [] })
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [revisedVms, setRevisedVms] = useState(1)
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
  const displayedJobs = useCloudJobDisplay(data.jobs)
  const runDisplay = displayedJobs.find(({ job }) => job.selection.backend === 'cloud_run')
  const batchDisplay = displayedJobs.find(({ job }) => job.selection.backend !== 'cloud_run')
  return (
    <section className="cloud-analysis" aria-label="Google Cloud analysis">
      <h2>GOOGLE CLOUD ANALYSIS</h2>
      <p>Same FEN selection, same database results. Local analysis can run alongside cloud jobs;
        reserved FENs are skipped by the other workers.</p>
      <p role="status">Controller: {data.controller_online ? 'online' : 'offline — start cloud-controller with Docker Compose'}.</p>
      <p>Cloud Run uses independent one-vCPU workers; Batch Spot uses 16-vCPU/16-GiB VMs.
        Each FEN uses one thread, 256 MiB hash and {DEFAULT_ANALYSIS_NODES.toLocaleString('en-US')} nodes (fixed),
        with no time-based search cutoff. Results are uploaded in batches of 500 and imported into the local database.
        Successful jobs clean up automatically after the final database import.</p>
      {error && <div role="alert" className="status-banner warn">{error}</div>}
      {cloudBusy && <p role="status" className="cloud-busy-notice">
        A cloud job is {activeJob?.status || 'active'}. New cloud selections are disabled until it finishes
        importing and cleaning up. Paused/failed jobs must be recovered first. Local analysis remains available.
      </p>}
      <div className="cloud-analysis-grid">
        <CloudSelection backend="cloud_run" disabled={busy || !data.controller_online || cloudBusy}
          job={runDisplay?.job} fading={runDisplay?.state === 'fading'}
          create={(payload) => action(() => createCloudAnalysis(payload))} />
        <CloudSelection backend="batch_spot" disabled={busy || !data.controller_online || cloudBusy}
          job={batchDisplay?.job} fading={batchDisplay?.state === 'fading'}
          create={(payload) => action(() => createCloudAnalysis(payload))} />
      </div>
      <h3>CLOUD JOB PROGRESS</h3>
      <p>Done means analysis, database import and cleanup have finished. The completed display fades after 10 seconds.
        Older completed/cancelled entries are hidden; saved results and reports are retained.</p>
      {!displayedJobs.length && <p>No active cloud jobs.</p>}
      {displayedJobs.map(({ job, state }) => {
        const active = job.runs.find((run) => run.status !== 'complete')
        const progress = cloudJobProgress(job)
        return (
          <article className={`cloud-job${state === 'fading' ? ' cloud-job-fading' : ''}`} key={job.id}>
            <strong>{job.selection.backend === 'cloud_run' ? 'Cloud Run' : 'Batch Spot'} · {job.selection.mode} {job.selection.player_name} — {progress.label}</strong>
            <small>{job.id}</small>
            <CloudFenProgress job={job} />
            <CloudJobSteps job={job} />
            {active?.cloud_state && <small>Cloud execution: {active.cloud_state} (compute only, not the full workflow).</small>}
            {job.runs.map((run) => <CloudPerformanceSummary key={run.id} run={run} />)}
            {active?.batch_jobs.map((id) => <small key={id}>Batch: {id}</small>)}
            {active?.run_job && <small>Cloud Run: {active.run_job}</small>}
            {active?.run_execution && <small>Execution: {active.run_execution.name}</small>}
            {active?.recovery && <p>Google interruptions: {active.recovery.preemptions}.
              {' '}Application failures: {active.recovery.application_failures} / 2.
              {' '}Recovery: {active.recovery.status.toLowerCase()}.</p>}
            {!!active?.vm_statuses?.length && <ul aria-label="Batch VM progress">{active.vm_statuses.map((vm) => (
              <li key={vm.index}>VM {vm.index + 1}: {vm.state} · {vm.positions.toLocaleString('en-US')} FENs ·
                {' '}{vm.preemptions} Google interruptions · {vm.application_failures}/2 application failures</li>
            ))}</ul>}
            {job.error && <p className="status-banner warn">{job.error}</p>}
            {job.status === 'paused' && active?.status === 'preparing' && job.selection.execution_mode === 'batch_multi_vm_v1' && (
              <div>
                <label>VM count for retrying preflight
                  <input className="text-input" type="number" min="1" max={MAX_BATCH_VMS} step="1"
                    value={revisedVms} onChange={(event) => setRevisedVms(event.target.value)}
                    disabled={busy || !data.controller_online} />
                </label>
                <button className="btn" type="button"
                  disabled={busy || !data.controller_online || !Number.isInteger(Number(revisedVms)) || Number(revisedVms) < 1 || Number(revisedVms) > MAX_BATCH_VMS}
                  onClick={() => action(() => reviseCloudVms(job.id, Number(revisedVms)))}>Update VM count and retry checks</button>
                <small>The selected FENs remain reserved. VM count cannot change after preparation.</small>
              </div>
            )}
            {['paused', 'waiting'].includes(job.status) && (
              <button className="btn" disabled={busy || !data.controller_online || (activeJob && activeJob.id !== job.id)}
                onClick={() => action(() => resumeCloudAnalysis(job.id))}>Resume saved job</button>
            )}
            {(active?.recovery_session || active?.run_job) && ['running', 'run_discover', 'submitting'].includes(active?.status) && ['running', 'paused'].includes(job.status) && (
              <button className="btn btn-secondary" disabled={busy || !data.controller_online || job.selection.cancel_requested}
                onClick={() => action(() => cancelCloudAnalysis(job.id))}>
                {job.selection.cancel_requested ? 'Cancellation requested' : 'Stop cloud job and recovery'}
              </button>
            )}
            {job.status === 'failed' && (active?.run_job ? active.run_execution_count < 3 : active?.recovery_session ? active.recovery_session < 3 : active?.batch_jobs.length < 3) && (
              <button className="btn" disabled={busy || !data.controller_online || (activeJob && activeJob.id !== job.id)}
                onClick={() => action(() => retryCloudAnalysis(job.id))}>Retry cloud — billable</button>
            )}
          </article>
        )
      })}
      <p>Temporary job data and its image are deleted; the empty bucket/repository, recovery workflow and permissions remain.
        Eligible, job-owned VM or Cloud Run log streams are removed after a performance report is saved locally.
        Shared logs and Google audit/billing/monitoring/workflow history remain. Failed jobs retain checkpoints for recovery.</p>
    </section>
  )
}

function CloudSelection({ backend, disabled, create, job, fading }) {
  const isRun = backend === 'cloud_run'
  const title = isRun ? 'Cloud Run' : 'Batch Spot'
  const id = isRun ? 'cloud-run' : 'cloud-batch'
  const [mode, setMode] = useState('all')
  const [form, setForm] = useState({ ...(isRun ? cloudRunDefaults : cloudDefaults) })
  const [preview, setPreview] = useState(null)
  const [inspection, setInspection] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const change = (key, value) => {
    setForm((current) => ({ ...current, [key]: value }))
    // Concurrency does not change the frozen FEN selection.
    if (!['n_cpus', 'n_vms'].includes(key)) { setPreview(null); setInspection(null) }
    setError('')
  }
  const changeMode = (value) => {
    setMode(value)
    setPreview(null); setInspection(null)
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
    <form className="cloud-selection" aria-labelledby={`${id}-title`} onSubmit={(event) => {
      event.preventDefault()
      if (disabled || loading) return
      try { create(cloudPayload(mode, form, preview)) } catch (err) { setError(err.message) }
    }}>
      <h3 id={`${id}-title`}>{title.toUpperCase()}</h3>
      <p>Choose your FEN set and {isRun ? 'CPU concurrency' : 'number of Spot VMs'}. Only missing, unreserved FENs are selected.</p>
      {job && <div className={fading ? 'cloud-job-fading' : ''}>
        <p role="status">Current job: {cloudJobProgress(job).label}</p><CloudFenProgress job={job} />
      </div>}
      <fieldset disabled={disabled || loading} aria-label={`${title} controls`}>
      <label>FEN selection
        <select className="text-input" aria-label="FEN selection" value={mode}
          onChange={(event) => changeMode(event.target.value)}>
          <option value="all">All positions — unscored FENs</option>
          <option value="player">Player positions — unscored FENs</option>
          <option value="games">Player games — missing FENs</option>
          <option value="loop">Grouped positions — all or player</option>
        </select>
      </label>
      {isRun ? <><label>Simultaneous CPUs (n_cpus)
        <input className="text-input" type="number" min="1" max="64" step="1" required
          aria-label="Simultaneous CPUs (n_cpus)" aria-describedby="cloud-run-cpus-help"
          value={form.n_cpus} onChange={(event) => change('n_cpus', event.target.value)} />
      </label>
      <small id="cloud-run-cpus-help">Maximum concurrent one-vCPU workers, not threads per FEN.
        Up to 64, subject to your Google Cloud quota and the amount of selected work.</small></> : <>
        <label>Spot VMs (n_vms)
          <input className="text-input" type="number" min="1" max={MAX_BATCH_VMS} step="1" required
            aria-label="Spot VMs (n_vms)" aria-describedby="cloud-batch-vms-help"
            value={form.n_vms} onChange={(event) => change('n_vms', event.target.value)} />
        </label>
        <small id="cloud-batch-vms-help">Each VM: 16 vCPUs, 16 GiB RAM (n2d-highcpu-16, Spot only).
          {' '}{Number(form.n_vms) * 16} vCPUs requested. Up to {MAX_BATCH_VMS} VMs, subject to quota;
          insufficient quota pauses before launch. Fewer VMs are used only when there are fewer FENs than VMs.</small>
      </>}
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
      <button className="btn" type="submit" disabled={disabled || loading || (mode === 'games' && !!cloudGameSelectionError(preview))}>
        Create {title} job — billable
      </button>
      </fieldset>
    </form>
  )
}
