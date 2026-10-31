import { cloudJobIndicator } from './cloudJobProgress'

export default function CloudJobIndicator({ job }) {
  const { state, label } = cloudJobIndicator(job)
  return <span className={`cloud-job-indicator cloud-job-indicator--${state}`}
    role="img" aria-label={label} title={label} />
}
