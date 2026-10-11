import { pageStyleTarget } from '../../appearance/styleCore.js'
import { cloudStatusSummary } from './cloudStatusSummary'
import CloudEtaBar from './CloudEtaBar'

const number = value => value == null ? '—' : value.toLocaleString('en-US')

export default function CloudStatusPanel({ loading, error, data, lastSuccessAt }) {
  const view = cloudStatusSummary(data, { loading, error })
  const { job, progress } = view

  return <section {...pageStyleTarget('cloud.panel')} className="status cloud-status-panel" aria-labelledby="cloud-status-title">
    <div className="section-head"><h2 {...pageStyleTarget('title')} id="cloud-status-title">GCloud Status</h2></div>
    <div className="status-grid">
      <article className={`status-card cloud-status-activity ${view.tone}`} aria-live="polite">
        <h3 {...pageStyleTarget('title')}>Cloud activity</h3>
        <p {...pageStyleTarget('text')} className="value" data-cloud-activity>{view.activity}</p>
        {job ? <div className="cloud-status-progress" title="Progress counts analyzed FENs already saved locally, updated as result batches arrive.">
          <p {...pageStyleTarget('text')} className="cloud-status-fen-count">{number(progress.imported)} / {number(progress.target)}</p>
          <progress value={progress.imported} max={progress.target || 1} aria-label="Cloud FEN progress"
            aria-valuetext={`${progress.imported} of ${progress.target} FENs analyzed and saved locally, ${progress.percent}%`} />
          <p {...pageStyleTarget('text')} className="meta">FENs analyzed &amp; saved</p>
          <CloudEtaBar key={job.id} job={job} jobs={data.jobs} available={!error && Boolean(data?.controller_online)} lastSuccessAt={lastSuccessAt} />
        </div> : null}
        {view.notice && <p {...pageStyleTarget('text')} className="meta cloud-status-notice">{view.notice}</p>}
      </article>

      <article className="status-card neutral" data-cloud-cpus>
        <h3 {...pageStyleTarget('title')}>CPUs</h3>
        <p {...pageStyleTarget('text')} className="value">{number(view.cpus)}</p>
        <p {...pageStyleTarget('text')} className="meta">{job ? 'configured vCPUs' : view.cpus == null ? 'Awaiting status' : 'No current job'}</p>
      </article>

      <article className="status-card neutral" data-cloud-machines>
        <h3 {...pageStyleTarget('title')}>{view.isRun ? 'Cloud Run' : 'Virtual machines'}</h3>
        <p {...pageStyleTarget('text')} className="value">{view.isRun ? 'Serverless' : `${number(view.vms)} VMs`}</p>
        <p {...pageStyleTarget('text')} className="meta">{job ? view.isRun ? '1 vCPU · 1 GiB per worker' : 'n2d-highcpu-16 · Spot' : view.vms == null ? 'Awaiting status' : 'No current job'}</p>
        {job && !view.isRun && <p {...pageStyleTarget('text')} className="meta cloud-status-vm-spec">16 vCPUs · 16 GiB per VM</p>}
      </article>

      <article className="status-card neutral">
        <h3 {...pageStyleTarget('title')}>Controller</h3>
        <p {...pageStyleTarget('text')} className="value" data-cloud-controller>{view.controller}</p>
        <p {...pageStyleTarget('text')} className="meta">local cloud controller</p>
      </article>

      <article className="status-card neutral">
        <h3 {...pageStyleTarget('title')}>Last updated</h3>
        <p {...pageStyleTarget('text')} className="value">{lastSuccessAt ? new Date(lastSuccessAt).toLocaleTimeString() : '—'}</p>
        <p {...pageStyleTarget('text')} className="meta">{error ? 'last known status' : 'refreshes every 5s'}</p>
      </article>
    </div>
  </section>
}
