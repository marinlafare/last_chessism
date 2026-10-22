import { pageStyleTarget } from '../../appearance/styleCore.js'
import { cloudJobProgress, cloudStepProgress } from './cloudJobProgress'

export default function CloudJobSteps({ job, controllerOnline = true }) {
  const { steps } = cloudJobProgress(job)
  return <ol {...pageStyleTarget('cloud.steps')} className="cloud-job-steps" role="list" aria-label="Cloud job steps">
    {steps.map((step, index) => {
      const { id, title, state } = step
      const progress = cloudStepProgress(job, step)
      const active = state === 'current' && controllerOnline
      const status = state === 'complete' ? 'Complete' : state === 'current' ? controllerOnline ? 'In progress' : 'Waiting for controller'
        : state === 'attention' ? 'Needs attention' : 'Pending'
      const indeterminate = progress.value === null
      return <li {...pageStyleTarget(`cloud.step.${state}`)} key={id} className={`cloud-step cloud-step--${state}`}
      aria-current={state === 'current' || state === 'attention' ? 'step' : undefined}>
      <span {...pageStyleTarget(`cloud.step.${state}.number`)} className="cloud-job-badge cloud-step-number" aria-hidden="true">{index + 1}</span>
      <span {...pageStyleTarget(`cloud.step.${state}.name`)} className="cloud-job-badge cloud-step-name">{title}</span>
      <small {...pageStyleTarget(`cloud.step.${state}.status`)} className="cloud-step-status">{status}</small>
      <div className="cloud-step-meter">
        <div {...pageStyleTarget(`cloud.step.progress.${id}`)}
          className={`cloud-step-bar${indeterminate ? ' is-indeterminate' : ''}${active ? ' is-active' : ''}`}
          role="progressbar" aria-label={title} aria-valuemin={0} aria-valuemax={100}
          aria-valuenow={progress.value ?? undefined} aria-valuetext={`${status} — ${progress.detail}`}
          title={indeterminate ? `${status}. This step does not report a completion percentage.` : progress.detail}>
          <span className="cloud-step-bar-fill" aria-hidden="true" style={{ width: indeterminate ? '30%' : `${progress.value}%` }} />
        </div>
        {(state === 'current' || state === 'attention') && !indeterminate && <small
          {...pageStyleTarget(`cloud.step.${state}.status`)} className="cloud-step-detail">{progress.detail}</small>}
      </div>
    </li>})}
  </ol>
}
