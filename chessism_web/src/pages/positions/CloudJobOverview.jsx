import { pageStyleTarget } from '../../appearance/styleCore'
import CloudFenProgress from './CloudFenProgress'
import CloudJobSteps from './CloudJobSteps'
import CloudVmTiles from './CloudVmTiles'
import { CloudPerformanceSummary } from './CloudPerformanceSummary'

const number = value => Number.isFinite(value) ? value.toLocaleString('en-US') : '—'
const playerCountNote = 'Saved selection-time FEN count; multiple selections for the same player are added together. Shared or subsequently reserved positions can make these counts exceed the deduplicated job total.'

export default function CloudJobOverview({ job, active, onOpenReport }) {
  const combined = job.combined_selection
  const backend = job.selection.backend === 'cloud_run' ? 'Cloud Run' : 'Batch Spot'
  const cycles = job.runs.filter(run => run.positions > 0)
  const cycleLabel = run => `Cycle ${cycles.findIndex(item => item.id === run.id) + 1} of ${cycles.length}`
  const progressRun = active?.positions > 0 ? active : cycles[cycles.length - 1]
  const selectionLabel = combined ? 'Combined games' : ({ games: 'Games', player: 'Player', loop: 'Repeated selection' }[job.selection.mode] || 'Positions')
  // Keep completed VM details accessible during shared verification/cleanup.
  const vmRun = active ? (active.vm_statuses?.length ? active : null)
    : [...job.runs].reverse().find(run => run.vm_statuses?.length)
  return <>
    <header {...pageStyleTarget('cloud.job.summary')} className="cloud-job-part cloud-job-heading" aria-label="Cloud job selection">
      <div className="cloud-job-summary-row">
        <div {...pageStyleTarget('cloud.job.kind')} className="cloud-job-kind">
          <span className="cloud-job-kind-items">
            <strong {...pageStyleTarget('cloud.job.backend')} className="cloud-job-badge">{backend}</strong>
            <span {...pageStyleTarget('cloud.job.arrow')} aria-hidden="true">→</span>
            <strong {...pageStyleTarget('cloud.job.mode')} className="cloud-job-badge">{selectionLabel}</strong>
          </span>
        </div>
        <div {...pageStyleTarget('cloud.job.total-row')} className="cloud-job-total">
          <strong {...pageStyleTarget('cloud.job.total')} className="cloud-job-badge cloud-job-count"
            title="Total FENs" aria-label={`Total FENs: ${number(job.target)}`}>{number(job.target)}</strong>
        </div>
      </div>
      {combined ? <ul className="cloud-job-players">{combined.players.map(player => <li key={player}>
        <span {...pageStyleTarget('cloud.job.player')} className="cloud-job-badge">{player}</span>
        <strong {...pageStyleTarget('cloud.job.count')} className="cloud-job-badge cloud-job-count" title={playerCountNote}>
          {number(combined.player_fen_previews?.[player])}
        </strong>
      </li>)}</ul> : job.selection.player_name && <span {...pageStyleTarget('cloud.job.player')}
        className="cloud-job-badge">{job.selection.player_name}</span>}
    </header>
    <section {...pageStyleTarget('cloud.job.progress')} className="cloud-job-part cloud-job-progress" aria-label="Cloud progress and process steps">
      <CloudFenProgress job={job} />
      {job.sequence?.frozen && progressRun && <div className="cloud-sequence-progress">
        <CloudFenProgress job={{ ...job, target: progressRun.positions, imported: progressRun.imported, runs: [progressRun] }}
          label={cycleLabel(progressRun)} variant="cycle" showCompletionStatus={false} />
      </div>}
      <CloudJobSteps job={job} />
    </section>
    {vmRun && <CloudVmTiles key={vmRun.id} run={vmRun} cycleLabel={job.sequence ? cycleLabel(vmRun) : null} />}
    {job.runs.map(run => <CloudPerformanceSummary key={run.id} run={run} onOpen={onOpenReport} />)}
    {active?.run_job && <small {...pageStyleTarget('text')}>Cloud Run: {active.run_job}</small>}
    {active?.run_execution && <small {...pageStyleTarget('text')}>Execution: {active.run_execution.name}</small>}
    {active?.recovery && !vmRun && <p {...pageStyleTarget('text')}>Google interruptions: {active.recovery.preemptions}.
      {' '}Application failures: {active.recovery.application_failures} / 2.
      {' '}Recovery: {active.recovery.status.toLowerCase()}.</p>}
  </>
}
