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
      const graph = id === 'neo4j' ? job.graph_sync : null
      const heartbeat = graph?.heartbeat_at ? Date.parse(graph.heartbeat_at) : NaN
      const online = graph ? Number.isFinite(heartbeat) && Date.now() - heartbeat < 60000 : controllerOnline
      const active = state === 'current' && online && (!graph || graph.status === 'working')
      const status = graph ? ({ complete:'Complete', working:online?'In progress':'Waiting for Neo4j worker',
        waiting:'Waiting for imports', pending:'Pending', retrying:'Retrying', unavailable:'Needs attention' }[graph.status] || 'Pending')
        : state === 'complete' ? 'Complete' : state === 'current' ? controllerOnline ? 'In progress' : 'Waiting for controller'
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
        {(graph || state === 'current' || state === 'attention') && (!indeterminate || progress.showDetail) && <small
          {...pageStyleTarget(`cloud.step.${state}.detail`)} className="cloud-step-detail">{progress.detail}</small>}
      </div>
    </li>})}
  </ol>
}
