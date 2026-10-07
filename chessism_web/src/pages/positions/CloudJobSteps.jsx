import { cloudJobProgress } from './cloudJobProgress'

export default function CloudJobSteps({ job }) {
  const { steps } = cloudJobProgress(job)
  return <ol className="cloud-job-steps" aria-label="Cloud job steps">
    {steps.map(({ title, state }) => <li key={title} className={`cloud-step cloud-step--${state}`}
      aria-current={state === 'current' || state === 'attention' ? 'step' : undefined}>
      <span>{title}</span>
      <small>{state === 'complete' ? 'Complete' : state === 'current' ? 'In progress'
        : state === 'attention' ? 'Needs attention' : 'Pending'}</small>
    </li>)}
  </ol>
}
