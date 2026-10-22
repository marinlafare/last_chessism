import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import {
  createCloudAnalysis, resumeCloudAnalysis, retryCloudAnalysis, stopAndCleanCloudAnalysis,
  previewPlayerGameAnalysis, fetchPlayerAnalysisCounts, reviseCloudVms, fetchCloudDraft,
} from './positionsApi'
import { cloudDefaults, cloudRunDefaults, cloudPayload, cloudGamePreview, cloudGameSelectionError,
  cloudLimit, MAX_BATCH_VMS } from './cloudAnalysisForm'
import { blockingCloudJob, cloudIsBusy } from './cloudJobState'
import CloudFenProgress from './CloudFenProgress'
import CloudJobOverview from './CloudJobOverview'
import CloudPerformanceReportModal from './CloudPerformanceReportModal'
import { cloudJobProgress, cloudActiveRun, cloudJobDone } from './cloudJobProgress'
import useCloudJobDisplay from './useCloudJobDisplay'
import useCloudStatus from './useCloudStatus'
import CloudGameDraft from './CloudGameDraft'
import CloudComputeField from './CloudComputeField'
import { hasSavedCloudDraft } from './cloudGameDraft'
import CloudJobIndicator from './CloudJobIndicator'
import AnalysisDisclosure, { AnalysisDisclosureGroup } from './AnalysisDisclosure'
import AnalysisHistory from './AnalysisHistory'
import './cloudAnalysis.css'

export default function CloudAnalysisSection({ onProgress, collapsible = false }) {
  const { data, error, setError, refresh, updateData } = useCloudStatus(onProgress)
  const [busy, setBusy] = useState(false)
  const [sendingStop, setSendingStop] = useState(null)
  const [revisedVms, setRevisedVms] = useState(1)
  const [savedDraft, setSavedDraft] = useState(null)
  const [restoredDraftId, setRestoredDraftId] = useState(null)
  const [draftError, setDraftError] = useState('')
  // Keep the open report available after its progress card is dismissed.
  const [performanceRun, setPerformanceRun] = useState(null)
  useEffect(() => {
    const controller = new AbortController()
    // Read the saved selection, not a new preview or reservation. Never submit
    // work as a side effect of visiting or refreshing the page.
    fetchCloudDraft({ signal: controller.signal }).then(draft => { if (!controller.signal.aborted) {
      setSavedDraft(draft)
      setRestoredDraftId(hasSavedCloudDraft(draft) ? draft.id : null)
    } })
      .catch(err => { if (!controller.signal.aborted) setDraftError(err.message) })
    return () => controller.abort()
  }, [])
  const action = async (fn) => {
    setBusy(true)
    try { await fn(); await refresh() } catch (err) { setError(err.message) }
    finally { setBusy(false) }
  }
  const activeJob = blockingCloudJob(data)
  const cloudBusy = cloudIsBusy(data)
  const activeBackend = activeJob ? (activeJob.selection?.backend || 'batch_spot') : null
  const { displayedJobs, dismiss } = useCloudJobDisplay(data.jobs)
  const runDisplay = displayedJobs.find(({ job }) => job.selection.backend === 'cloud_run')
  const batchDisplay = displayedJobs.find(({ job }) => job.selection.backend !== 'cloud_run')
  const statusJob = activeJob || displayedJobs[0]?.job
  const historyRevision = backend => data.jobs.filter(job => (job.selection.backend || 'batch_spot') === backend)
    .map(job => `${job.id}:${job.status}`).join('|')
  const content = (
    <section {...pageStyleTarget('cloud.content')} className="cloud-analysis" aria-label="Google Cloud analysis">
      {!data.controller_online && <p {...pageStyleTarget('text')} role="status">Controller offline — start cloud-controller with Docker Compose.</p>}
      {error && <div role="alert" className="status-banner warn">{error}</div>}
      {draftError && <p {...pageStyleTarget('text')} role="alert">Could not restore the saved FEN set: {draftError}. Open the combined builder and reload the saved draft.</p>}
      <div className="cloud-analysis-grid">
        <AnalysisDisclosure id="analysis-cloud-run" label="Cloud Run" level={3}
          hidden={cloudBusy && activeBackend !== 'cloud_run'}>
          <CloudSelection backend="cloud_run" disabled={busy || !data.controller_online || cloudBusy}
            job={runDisplay?.job}
            onSubmitted={refresh}
            savedDraft={savedDraft} onDraftChange={setSavedDraft}
            create={(payload) => action(() => createCloudAnalysis(payload))} />
          {!runDisplay && <AnalysisHistory backend="cloud_run" revision={historyRevision('cloud_run')} />}
        </AnalysisDisclosure>
        <AnalysisDisclosure id="analysis-batch-spot" label="Batch Spot" level={3}
          hidden={cloudBusy && activeBackend !== 'batch_spot'}
          revealKey={restoredDraftId}>
          <CloudSelection backend="batch_spot" disabled={busy || !data.controller_online || cloudBusy}
            job={batchDisplay?.job}
            onSubmitted={refresh}
            savedDraft={savedDraft} onDraftChange={setSavedDraft}
            revealDraftId={restoredDraftId}
            create={(payload) => action(() => createCloudAnalysis(payload))} />
          {!batchDisplay && <AnalysisHistory backend="batch_spot" revision={historyRevision('batch_spot')} />}
        </AnalysisDisclosure>
      </div>
      {!!displayedJobs.length && <h3 {...pageStyleTarget('title')}>CLOUD JOB PROGRESS</h3>}
      {displayedJobs.map(({ job }) => {
        const active = cloudActiveRun(job)
        const canRevise = !job.stop_cleanup && job.status === 'paused' && active?.status === 'preparing' && ['batch_multi_vm_v1', 'batch_work_units_v1'].includes(job.selection.execution_mode)
        const canResume = ['paused', 'waiting'].includes(job.status)
        const canCancel = !job.stop_cleanup && sendingStop !== job.id && ['queued', 'running', 'paused', 'waiting', 'failed'].includes(job.status)
        const canRetry = !job.stop_cleanup && job.status === 'failed' && (job.sequence?.automatic ? job.sequence.session < 3 : active?.run_job ? active.run_execution_count < 3 : active?.recovery_session ? active.recovery_session < 3 : active?.batch_jobs.length < 3)
        const canDismiss = cloudJobDone(job) || job.status === 'cancelled'
        const stopping = Boolean(job.stop_cleanup || sendingStop === job.id) && !canDismiss
        const stopLabel = sendingStop === job.id ? 'Sending stop request…' : cloudJobProgress(job).label
        const backend = job.selection.backend || 'batch_spot'
        const showHistory = (backend === 'cloud_run' ? runDisplay : batchDisplay)?.job.id === job.id
        return (
          <article {...pageStyleTarget(stopping ? 'cloud.job.stopping' : 'cloud.job')}
            className={`cloud-job${stopping ? ' cloud-job-stopping' : ''}`} key={job.id}>
            <CloudJobOverview job={job} active={active} onOpenReport={setPerformanceRun} />
            {(job.error || canRevise || canResume || canRetry) && <section {...pageStyleTarget('cloud.job.controls')}
              className="cloud-job-part cloud-job-controls" aria-label="Cloud job stop and recovery">
            {job.error && <p {...pageStyleTarget('text')} className="status-banner warn">{job.error}</p>}
            {canRevise && (
              <div>
                <label {...pageStyleTarget('label')}>VM count for retrying preflight
                  <input {...pageStyleTarget('field')} className="text-input" type="number" min="1" max={MAX_BATCH_VMS} step="1"
                    value={revisedVms} onChange={(event) => setRevisedVms(event.target.value)}
                    disabled={busy || !data.controller_online} />
                </label>
                <button {...pageStyleTarget('button')} className="btn" type="button"
                  disabled={busy || !data.controller_online || !Number.isInteger(Number(revisedVms)) || Number(revisedVms) < 1 || Number(revisedVms) > MAX_BATCH_VMS}
                  onClick={() => action(() => reviseCloudVms(job.id, Number(revisedVms)))}>Update VM count and retry checks</button>
                <small {...pageStyleTarget('text')}>The selected FENs remain reserved. VM count cannot change after preparation.</small>
              </div>
            )}
            {canResume && (
              <button {...pageStyleTarget('button')} className="btn" disabled={busy || !data.controller_online || (activeJob && activeJob.id !== job.id)}
                onClick={() => action(() => resumeCloudAnalysis(job.id))}>{job.stop_cleanup ? 'Resume cleanup' : 'Resume saved job'}</button>
            )}
            {canRetry && (
              <button {...pageStyleTarget('button')} className="btn" disabled={busy || !data.controller_online || (activeJob && activeJob.id !== job.id)}
                onClick={() => action(() => retryCloudAnalysis(job.id))}>Retry cloud — billable</button>
            )}
            </section>}
            {canCancel && <button {...pageStyleTarget('cloud.job.stop')} className="cloud-job-stop" type="button"
              disabled={busy || !data.controller_online} onClick={() => {
                if (window.confirm('Stop this cloud job and clean up? Completed result batches will be saved. Unfinished work will stop and its checkpoints will be removed; this job cannot restart.')) {
                  setSendingStop(job.id)
                  action(async () => {
                    try {
                      const reply = await stopAndCleanCloudAnalysis(job.id)
                      updateData(current => ({ ...current, jobs: current.jobs.map(item => item.id === job.id
                        ? { ...item, stop_cleanup: { phase: reply.phase, finished_at: null } } : item) }))
                    } finally { setSendingStop(null) }
                  })
                }
              }}>Stop and clean up</button>}
            {stopping && <div className="cloud-job-stop-footer">
              <p {...pageStyleTarget('text')} className="cloud-job-stop-status" role="status">{stopLabel}</p>
              <span {...pageStyleTarget('cloud.job.stop-indicator')}
                className={`cloud-stop-indicator${job.status === 'paused' ? ' is-paused' : ''}`}
                role="img" aria-label={stopLabel} title={stopLabel} />
            </div>}
            {canDismiss && <button {...pageStyleTarget('cloud.job.dismiss')} className="cloud-job-dismiss" type="button"
              title="Hide this progress card. Saved scores, reports and job history are kept."
              onClick={() => dismiss(job.id)}>Reviewed — dismiss progress</button>}
            {showHistory && <AnalysisDisclosureGroup>
              <AnalysisHistory backend={backend} revision={historyRevision(backend)} />
            </AnalysisDisclosureGroup>}
          </article>
        )
      })}
      {performanceRun && <CloudPerformanceReportModal run={performanceRun} onClose={() => setPerformanceRun(null)} />}
    </section>
  )
  return collapsible ? <AnalysisDisclosure id="analysis-gcloud" label="GCloud"
    revealKey={restoredDraftId}
    indicator={statusJob ? <CloudJobIndicator job={statusJob} /> : null}>
    {content}
  </AnalysisDisclosure> : content
}

function CloudSelection({ backend, disabled, create, job, onSubmitted, savedDraft, onDraftChange, revealDraftId }) {
  const isRun = backend === 'cloud_run'
  const maximum = cloudLimit(backend)
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
    if (!['n_cpus', 'n_vms', 'times'].includes(key)) { setPreview(null); setInspection(null) }
    setError('')
  }
  const changeMode = (value) => {
    setMode(value)
    setPreview(null); setInspection(null)
    setError('')
  }
  const field = (key, label, max, min = 1) => (
    <label {...pageStyleTarget('label')}><span {...pageStyleTarget('label')} className="field-label">{label}</span>
      <input {...pageStyleTarget('field')} className="text-input" type="number" min={min} max={max} required
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
  const singleSelection = (
    <form {...pageStyleTarget(isRun ? 'cloud.run.selection' : 'cloud.batch.selection')} id={`${id}-selection-form`}
      className="cloud-selection cloud-process-content" aria-label={`${title} selection`} onSubmit={(event) => {
      event.preventDefault()
      if (disabled || loading) return
      try { create(cloudPayload(mode, form, preview)) } catch (err) { setError(err.message) }
    }}>
      {job && <div>
        <p {...pageStyleTarget('text')} role="status">Current job: {cloudJobProgress(job).label}</p><CloudFenProgress job={job} />
      </div>}
      <fieldset disabled={disabled || loading} aria-label={`${title} controls`}>
      <label {...pageStyleTarget('label')}>FEN selection
        <select {...pageStyleTarget('field')} className="text-input" aria-label="FEN selection" value={mode}
          onChange={(event) => changeMode(event.target.value)}>
          <option value="all">All positions — unscored FENs</option>
          <option value="player">Player positions — unscored FENs</option>
          <option value="games">Player games — missing FENs</option>
          <option value="loop">Grouped positions — all or player</option>
        </select>
      </label>
      {mode === 'loop' && <label {...pageStyleTarget('label')}>Scope
        <select {...pageStyleTarget('field')} className="text-input" value={form.scope} onChange={(event) => change('scope', event.target.value)}>
          <option value="all">All positions</option><option value="player">Player</option>
        </select></label>}
      {playerVisible && <label {...pageStyleTarget('label')}>Player
        <input {...pageStyleTarget('field')} className="text-input" value={form.player} required maxLength={200}
          onChange={(event) => change('player', event.target.value)} placeholder="chess.com nickname" />
      </label>}
      {playerVisible && mode !== 'games' && <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button"
        disabled={loading || !form.player.trim()} onClick={inspect}>Inspect player</button>}
      {inspection && <p {...pageStyleTarget('text')}>{inspection.total_positions} positions; {inspection.analyzed_positions} analyzed.</p>}
      {mode === 'games' && <>
        <label {...pageStyleTarget('label')}>Game selection
          <select {...pageStyleTarget('field')} className="text-input" value={form.selection} onChange={(event) => change('selection', event.target.value)}>
            <option value="latest">Latest games</option><option value="oldest">Oldest games</option>
            <option value="range">Date range</option><option value="fair_range">Fair range</option>
          </select></label>
        {['range', 'fair_range'].includes(form.selection) && <>
          <label {...pageStyleTarget('label')}>From<input {...pageStyleTarget('field')} className="text-input" type="date" value={form.from}
            onChange={(event) => change('from', event.target.value)} /></label>
          <label {...pageStyleTarget('label')}>To<input {...pageStyleTarget('field')} className="text-input" type="date" value={form.to}
            onChange={(event) => change('to', event.target.value)} /></label>
        </>}
        {mode === 'games' && form.selection === 'range' && <>
          <label {...pageStyleTarget('label')}><input {...pageStyleTarget('field')} type="checkbox" checked={form.allGames}
            onChange={(event) => change('allGames', event.target.checked)} /> All games in range</label>
          <select {...pageStyleTarget('field')} className="text-input" aria-label="Range order" value={form.order}
            onChange={(event) => change('order', event.target.value)}>
            <option value="latest">Newest first</option><option value="oldest">Oldest first</option>
          </select>
        </>}
        {!(form.selection === 'range' && form.allGames) && field('games', 'Games', 50000)}
      </>}
      {mode === 'loop' ? <>{field('positions', 'Positions per group', 1000)}{field('runs', 'Groups (combined into one job)', maximum / 1000)}</>
        : mode !== 'games' && field('total', 'Total FEN positions', maximum)}
      {mode === 'games' && <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button"
        disabled={loading || !form.player.trim()} onClick={inspect}>Preview games</button>}
      {preview && <p {...pageStyleTarget('text')}>Frozen preview: {preview.selected_games} games, {preview.fens_to_analyze} missing FENs.
        The system sets the FEN count from this selection. Selections over {maximum.toLocaleString('en-US')} FENs cannot be submitted.</p>}
      {error && <p {...pageStyleTarget('text')} role="alert" className="status-banner warn">{error}</p>}
      <button {...pageStyleTarget('button')} className="btn" type="submit" title="Start billable Google Cloud analysis"
        disabled={disabled || loading || (mode === 'games' && !!cloudGameSelectionError(preview, backend))}>
        {isRun ? 'Run' : `Create ${title} job — billable`}
      </button>
      </fieldset>
    </form>
  )
  const selectionLayout = <div className="cloud-process-layout">
    <CloudComputeField resource={isRun ? 'cpu' : 'vm'}
      value={isRun ? form.n_cpus : form.n_vms} onChange={value => change(isRun ? 'n_cpus' : 'n_vms', value)}
      disabled={disabled || loading} label={isRun ? 'CPUs' : 'Spot VMs (n_vms)'} formId={`${id}-selection-form`} />
    {singleSelection}
  </div>
  return <div className="cloud-process-choices">
    <AnalysisDisclosure id={`${id}-single`} label="One process" level={4} hideWhenOpen={isRun ? null : `${id}-combined`}>
      {selectionLayout}
    </AnalysisDisclosure>
    <AnalysisDisclosure id={`${id}-combined`} label="Combined" level={4} lazy hideWhenOpen={isRun ? null : `${id}-single`}
      revealKey={revealDraftId}>
      <CloudGameDraft launchDisabled={disabled} onSubmitted={onSubmitted}
        backend={backend} draft={savedDraft} setDraft={onDraftChange}
        nVms={form.n_vms} onVmChange={value => change('n_vms', value)}
        nCpus={form.n_cpus} onCpuChange={value => change('n_cpus', value)} />
    </AnalysisDisclosure>
  </div>
}
