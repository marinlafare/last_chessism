import { pageStyleTarget } from '../../appearance/styleCore.js'
import ProgressMeter from '../../components/progress/ProgressMeter.jsx'
import { cloudActiveRun, cloudJobProgress, cloudStepProgress } from './cloudJobProgress'
import ProcessTiming from './ProcessTiming.jsx'
import { cloudStepTiming } from './processTiming.js'

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
      const timing = cloudStepTiming(job, step, cloudActiveRun(job)?.id)
      return <li {...pageStyleTarget(`cloud.step.${state}`)} key={id} className={`cloud-step cloud-step--${state}`}
      aria-current={state === 'current' || state === 'attention' ? 'step' : undefined}>
      <span {...pageStyleTarget(`cloud.step.${state}.number`)} className="cloud-job-badge cloud-step-number" aria-hidden="true">{index + 1}</span>
      <span {...pageStyleTarget(`cloud.step.${state}.name`)} className="cloud-job-badge cloud-step-name">{title}</span>
      <small {...pageStyleTarget(`cloud.step.${state}.status`)} className="cloud-step-status">{status}</small>
      <div className="cloud-step-meter">
        <ProcessTiming {...pageStyleTarget(`cloud.step.${state}.status`)} {...timing}
          active={state === 'current' || state === 'attention'} pending={state === 'pending'} />
        <ProgressMeter {...pageStyleTarget(`cloud.step.progress.${id}`)}
          className={`cloud-step-bar${indeterminate ? ' is-indeterminate' : ''}${active ? ' is-active' : ''}`}
          value={progress.value} aria-label={title} aria-valuetext={`${status} — ${progress.detail}`}
          fillTag="span" fillClassName="cloud-step-bar-fill" indeterminateWidth={30}
          title={indeterminate ? `${status}. This step does not report a completion percentage.` : progress.detail} />
        {(state === 'current' || state === 'attention') && (!indeterminate || progress.showDetail) && <small
          {...pageStyleTarget(`cloud.step.${state}.detail`)} className="cloud-step-detail">{progress.detail}</small>}
      </div>
    </li>})}
  </ol>
}
