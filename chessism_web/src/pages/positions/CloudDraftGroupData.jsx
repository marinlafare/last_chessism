import { pageStyleTarget } from '../../appearance/styleCore.js'
import { draftFenLimit, draftTaskSummary, draftVmPlan } from './cloudGameDraft'

export default function CloudDraftGroupData({ draft, backend = 'batch_spot', nVms, loading, phase, onRefresh }) {
  const preview = draft?.preview?.revision === draft?.revision ? draft?.preview : null
  const isRun = backend === 'cloud_run'
  const vmPlan = isRun ? null : draftVmPlan(preview?.fens_to_analyze, nVms)
  const count = value => value == null ? '—' : value.toLocaleString('en-US')
  return <section {...pageStyleTarget(isRun ? 'cloud.run.draft.group' : 'cloud.batch.draft.group')} className="cloud-draft-preview" aria-label="Group data">
    <div className="cloud-draft-group-heading">
      <h5 {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.title`)}>Group data</h5>
      {!!draft?.entries.length && <button {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.refresh`)} className="cloud-draft-refresh" type="button"
        aria-label="Recalculate group data" title="Recalculate group data" disabled={loading} onClick={onRefresh}>↻</button>}
    </div>
    <dl className="cloud-draft-totals">
      <div><dt {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.fens.label`)}>Available FENs</dt><dd {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.fens.value`)}>{count(preview?.fens_to_analyze)}</dd></div>
      <div><dt {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.games.label`)}>Unique games</dt><dd {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.games.value`)}>{count(preview?.unique_games)}</dd></div>
      <div><dt {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.reserved.label`)}>Reserved elsewhere</dt><dd {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.reserved.value`)}>{count(preview?.claimed_fens)}</dd></div>
    </dl>
    {loading && <p {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.loading`)} role="status">{phase}</p>}
    {preview && <>
      {!isRun && <p {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.plan`)}>{draftTaskSummary(preview.fens_to_analyze, nVms)}</p>}
      {vmPlan && <>
        <p {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.cpus`)}>Up to {Math.min(Number(nVms), preview.fens_to_analyze) * 16} vCPUs concurrently.</p>
        <table {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.table`)} className="cloud-draft-vm-plan" aria-label="Planned Batch VM allocation">
          <thead><tr><th {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.cycle.header`)} scope="col">Cycle</th><th {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.total.header`)} scope="col">Total FENs</th><th {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.per-vm.header`)} scope="col">FENs per VM</th></tr></thead>
          <tbody>{vmPlan.map(row => <tr key={row.cycle}>
            <th {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.cycle.cell`)} scope="row">{row.cycle}</th>
            <td {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.total.cell`)}>{row.total.toLocaleString('en-US')}</td>
            <td {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.per-vm.cell`)}>{row.vmFens.map((total, index) => <span key={index}>VM {index + 1}: {total.toLocaleString('en-US')}</span>)}</td>
          </tr>)}</tbody>
        </table>
      </>}
      <small {...pageStyleTarget(`cloud.${isRun ? 'run' : 'batch'}.combined.group.note`)}>Shared positions count once. Counts are rechecked at launch; maximum {count(draftFenLimit(draft, backend))} FENs.</small>
    </>}
  </section>
}
