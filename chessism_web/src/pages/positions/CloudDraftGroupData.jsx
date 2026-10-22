import { pageStyleTarget } from '../../appearance/styleCore.js'
import { draftFenLimit, draftTaskSummary, draftVmPlan } from './cloudGameDraft'

export default function CloudDraftGroupData({ draft, backend = 'batch_spot', nVms, loading, phase, onRefresh }) {
  const preview = draft?.preview?.revision === draft?.revision ? draft?.preview : null
  const isRun = backend === 'cloud_run'
  const vmPlan = isRun ? null : draftVmPlan(preview?.fens_to_analyze, nVms)
  const count = value => value == null ? '—' : value.toLocaleString('en-US')
  return <section {...pageStyleTarget(isRun ? 'cloud.run.draft.group' : 'cloud.batch.draft.group')} className="cloud-draft-preview" aria-label="Group data">
    <div className="cloud-draft-group-heading">
      <h5 {...pageStyleTarget('title')}>Group data</h5>
      {!!draft?.entries.length && <button {...pageStyleTarget('button')} className="cloud-draft-refresh" type="button"
        aria-label="Recalculate group data" title="Recalculate group data" disabled={loading} onClick={onRefresh}>↻</button>}
    </div>
    <dl className="cloud-draft-totals">
      <div><dt {...pageStyleTarget('label')}>Available FENs</dt><dd {...pageStyleTarget('value')}>{count(preview?.fens_to_analyze)}</dd></div>
      <div><dt {...pageStyleTarget('label')}>Unique games</dt><dd {...pageStyleTarget('value')}>{count(preview?.unique_games)}</dd></div>
      <div><dt {...pageStyleTarget('label')}>Reserved elsewhere</dt><dd {...pageStyleTarget('value')}>{count(preview?.claimed_fens)}</dd></div>
    </dl>
    {loading && <p {...pageStyleTarget('text')} role="status">{phase}</p>}
    {preview && <>
      {!isRun && <p {...pageStyleTarget('text')}>{draftTaskSummary(preview.fens_to_analyze, nVms)}</p>}
      {vmPlan && <>
        <p {...pageStyleTarget('text')}>Up to {Math.min(Number(nVms), preview.fens_to_analyze) * 16} vCPUs concurrently.</p>
        <table {...pageStyleTarget('table')} className="cloud-draft-vm-plan" aria-label="Planned Batch VM allocation">
          <thead><tr><th {...pageStyleTarget('table.header')} scope="col">Cycle</th><th {...pageStyleTarget('table.header')} scope="col">Total FENs</th><th {...pageStyleTarget('table.header')} scope="col">FENs per VM</th></tr></thead>
          <tbody>{vmPlan.map(row => <tr key={row.cycle}>
            <th {...pageStyleTarget('table.header')} scope="row">{row.cycle}</th>
            <td {...pageStyleTarget('table.cell')}>{row.total.toLocaleString('en-US')}</td>
            <td {...pageStyleTarget('table.cell')}>{row.vmFens.map((total, index) => <span key={index}>VM {index + 1}: {total.toLocaleString('en-US')}</span>)}</td>
          </tr>)}</tbody>
        </table>
      </>}
      <small {...pageStyleTarget('text')}>Shared positions count once. Counts are rechecked at launch; maximum {count(draftFenLimit(draft, backend))} FENs.</small>
    </>}
  </section>
}
