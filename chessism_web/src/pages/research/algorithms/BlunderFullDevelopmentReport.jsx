import { pageStyleTarget } from '../../../appearance/styleCore.js'
import BlunderDevelopmentDiagnostics from './BlunderDevelopmentDiagnostics.jsx'

const n = value => Number.isFinite(value) ? value.toLocaleString(undefined, { maximumFractionDigits: 4 }) : '—'

export default function BlunderFullDevelopmentReport({ report }) {
  const complete = report.format === 'chessism_blunder_full_development_v1'
  return <section aria-label="Full CUDA development report">
    <h3 {...pageStyleTarget('title')}>Full development · RTX 4060</h3>
    <p {...pageStyleTarget('text')}>{n(report.source.selected_games)} selected development games · {n(report.source.source_positions)} source positions</p>
    <p {...pageStyleTarget('text')}>Training and validation only. Final-test outcomes remain untouched; live summaries are unchanged. Reserved identities remain protected if the run is interrupted.</p>
    {report.source.balancing_order && <p {...pageStyleTarget('text')}>CP-first balancing: {n(report.source.reserved_games)} inspected games · {n(report.source.no_cp_games)} without usable CP · {n(report.source.removed_by_player_cap)} excluded by the unchanged player cap.</p>}
    <div className="algorithm-table-wrap"><table><caption>Development input</caption>
      <thead><tr><th>Partition</th><th>Mode</th><th>Available games</th><th>Selected games</th><th>Largest player share</th></tr></thead>
      <tbody>{report.source.selection.map(g => <tr key={`${g.split}-${g.mode}`}>
        <th>{g.split === 0 ? 'Training' : 'Validation'}</th><td>{g.mode}</td><td>{n(g.available_games)}</td><td>{n(g.selected_games)}</td><td>{g.largest_player_share == null ? '—' : n(g.largest_player_share*100)+'%'}</td>
      </tr>)}</tbody>
    </table></div>
    {!complete && <p {...pageStyleTarget('text')}>Input identities reserved; no completed model report yet.</p>}
    {complete && <>
      <p {...pageStyleTarget('text')}>{report.hardware.name} · GPU pool {n(report.gpu_memory.pool_limit_bytes/1024**3)} GiB · coefficient 0.00368208 · threshold 0.15</p>
      <div className="algorithm-table-wrap"><table><caption>Validation results — not independent test evidence</caption>
        <thead><tr><th>Mode</th><th>Selection</th><th>Validation MSE</th><th>Calibration error (pp)</th><th>Frozen reference error (pp)</th><th>Training batch rows</th></tr></thead>
        <tbody>{report.fits.map(f => <tr key={f.mode}><th>{f.mode}</th><td>{f.selection.status.replaceAll('_', ' ')}</td><td>{n(f.mse)}</td><td>{n(f.ece == null ? null : f.ece*100)}</td><td>{n(f.frozen_comparison?.ece == null ? null : f.frozen_comparison.ece*100)}</td><td>{n(f.training_batch_rows)}</td></tr>)}</tbody>
      </table></div>
      {report.development_diagnostics && <BlunderDevelopmentDiagnostics report={report.development_diagnostics} />}
      <details><summary {...pageStyleTarget('disclosure')}>Quality checks</summary><table><tbody>{Object.entries(report.gates).map(([k,v]) => <tr key={k}><th>{k.replaceAll('_',' ')}</th><td>{v.replaceAll('_',' ')}</td></tr>)}</tbody></table></details>
      <details><summary {...pageStyleTarget('disclosure')}>Original model-quality checks</summary><table><tbody>{Object.entries(report.original_quality_gates).map(([k,v]) => <tr key={k}><th>{k.replaceAll('_',' ')}</th><td>{v.replaceAll('_',' ')}</td></tr>)}</tbody></table></details>
      {report.notes.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
    </>}
  </section>
}
