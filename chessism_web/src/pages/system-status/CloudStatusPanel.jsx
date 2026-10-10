import { cloudStatusSummary } from './cloudStatusSummary'
import CloudEtaBar from './CloudEtaBar'

const number = value => value == null ? '—' : value.toLocaleString('en-US')

export default function CloudStatusPanel({ loading, error, data, lastSuccessAt }) {
  const view = cloudStatusSummary(data, { loading, error })
  const { job, progress } = view

  return <section className="status cloud-status-panel" aria-labelledby="cloud-status-title">
    <div className="section-head"><h2 id="cloud-status-title">GCloud Status</h2></div>
    <div className="status-grid">
      <article className={`status-card cloud-status-activity ${view.tone}`} aria-live="polite">
        <h3>Cloud activity</h3>
        <p className="value" data-cloud-activity>{view.activity}</p>
        {job ? <div className="cloud-status-progress" title="Progress counts analyzed FENs already saved locally, updated as result batches arrive.">
          <p className="cloud-status-fen-count">{number(progress.imported)} / {number(progress.target)}</p>
          <progress value={progress.imported} max={progress.target || 1} aria-label="Cloud FEN progress"
            aria-valuetext={`${progress.imported} of ${progress.target} FENs analyzed and saved locally, ${progress.percent}%`} />
          <p className="meta">FENs analyzed &amp; saved</p>
          <CloudEtaBar key={job.id} job={job} jobs={data.jobs} available={!error && Boolean(data?.controller_online)} lastSuccessAt={lastSuccessAt} />
        </div> : null}
        {view.notice && <p className="meta cloud-status-notice">{view.notice}</p>}
      </article>

      <article className="status-card neutral" data-cloud-cpus>
        <h3>CPUs</h3>
        <p className="value">{number(view.cpus)}</p>
        <p className="meta">{job ? 'configured vCPUs' : view.cpus == null ? 'Awaiting status' : 'No current job'}</p>
      </article>

      <article className="status-card neutral" data-cloud-machines>
        <h3>{view.isRun ? 'Cloud Run' : 'Virtual machines'}</h3>
        <p className="value">{view.isRun ? 'Serverless' : `${number(view.vms)} VMs`}</p>
        <p className="meta">{job ? view.isRun ? '1 vCPU · 1 GiB per worker' : 'n2d-highcpu-16 · Spot' : view.vms == null ? 'Awaiting status' : 'No current job'}</p>
        {job && !view.isRun && <p className="meta cloud-status-vm-spec">16 vCPUs · 16 GiB per VM</p>}
      </article>

      <article className="status-card neutral">
        <h3>Controller</h3>
        <p className="value" data-cloud-controller>{view.controller}</p>
        <p className="meta">local cloud controller</p>
      </article>

      <article className="status-card neutral">
        <h3>Last updated</h3>
        <p className="value">{lastSuccessAt ? new Date(lastSuccessAt).toLocaleTimeString() : '—'}</p>
        <p className="meta">{error ? 'last known status' : 'refreshes every 5s'}</p>
      </article>
    </div>
  </section>
}
