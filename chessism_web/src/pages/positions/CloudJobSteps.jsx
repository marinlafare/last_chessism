import { pageStyleTarget } from '../../appearance/styleCore.js'
import { cloudJobProgress } from './cloudJobProgress'

export default function CloudJobSteps({ job }) {
  const { steps } = cloudJobProgress(job)
  return <ol className="cloud-job-steps" role="list" aria-label="Cloud job steps">
    {steps.map(({ title, state }, index) => <li key={title} className={`cloud-step cloud-step--${state}`}
      aria-current={state === 'current' || state === 'attention' ? 'step' : undefined}>
      <span {...pageStyleTarget('cloud.step.number')} className="cloud-job-badge cloud-step-number" aria-hidden="true">{index + 1}</span>
      <span {...pageStyleTarget('cloud.step.name')} className="cloud-job-badge cloud-step-name">{title}</span>
      <small className="cloud-step-status">{state === 'complete' ? 'Complete' : state === 'current' ? 'In progress'
        : state === 'attention' ? 'Needs attention' : 'Pending'}</small>
    </li>)}
  </ol>
}
