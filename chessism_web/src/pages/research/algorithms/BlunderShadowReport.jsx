import { pageStyleTarget } from '../../../appearance/styleCore.js'

const number = value => Number.isFinite(value) ? value.toLocaleString(undefined, { maximumFractionDigits: 4 }) : '—'
const label = value => String(value).replaceAll('_', ' ')

export default function BlunderShadowReport({ report }) {
  const complete = report.format === 'chessism_blunder_shadow_pilot_v1'
  return <section className="algorithm-panel" aria-label="Blunder shadow pilot report">
    <h3 {...pageStyleTarget('title')}>Sampled shadow summaries — live results unchanged</h3>
    <p {...pageStyleTarget('text')}>{complete
      ? `${number(report.source.games)} unique games · ${number(report.checked_moves)} player moves · ${report.hardware.name} · Smoke: ${report.smoke.status}`
      : `Last complete shadow generation ${report.generation}: ${number(report.records.length)} games. The full pilot has not completed.`}</p>
    <p {...pageStyleTarget('text')}>These are sampled player histories, not complete player summaries. Parameters are frozen; this is not an independent calibration test or an approved canonical model.</p>
    <div className="algorithm-table-wrap"><table>
      <caption>Blunder counts — threshold effects separated from rating effects</caption>
      <thead><tr>{['Player', 'Mode', 'Games', 'Assessed games', 'Old 0.15', 'Baseline 0.20', 'Rating-aware 0.20', 'Unassessed moves'].map(h => <th key={h}>{h}</th>)}</tr></thead>
      <tbody>{report.groups.map(row => <tr {...pageStyleTarget('row')} key={`${row.player}-${row.mode}`}>
        <th>{row.player === '*' ? 'All sampled players' : row.player}</th><td>{row.mode}</td><td>{number(row.games)}</td>
        <td>{number(row.assessed_games)}</td>
        <td>{number(row.old15)}</td><td>{number(row.baseline20)}</td><td>{number(row.candidate20)}</td><td>{number(row.unassessed)}</td>
      </tr>)}</tbody>
    </table></div>
    {complete && <>
      <p {...pageStyleTarget('text')}>Agreement: {label(report.status)} · Maximum prediction difference: {report.max_prediction_error} · Incremental/rebuild agreement: {report.operational_checks.incremental_equals_rebuild ? 'passed' : 'failed'} · Duplicate retry changed {report.operational_checks.duplicate_retry.changed_games} games.</p>
      <div className="algorithm-table-wrap"><table><caption>Reference checks — mismatches</caption><tbody>
        {Object.entries(report.checks).map(([key, value]) => <tr {...pageStyleTarget('row')} key={key}><th>{label(key)}</th><td>{number(value)}</td></tr>)}
      </tbody></table></div>
      <p {...pageStyleTarget('text')}>Same-formula warm medians: CPU {number(report.benchmark.median_cpu_seconds)} s · GPU including transfers {number(report.benchmark.median_gpu_seconds)} s. Three repeats; database reads and writes excluded from these two numbers.</p>
      <details><summary>Measured pipeline timings</summary><table><tbody>{Object.entries(report.timings).map(([key, value]) => <tr key={key}><th>{label(key)}</th><td>{number(value)} s</td></tr>)}</tbody></table></details>
      <details><summary>Sampled blunders-per-game distributions</summary><p>Bins: 0, 1, 2, 3, 4, 5, 6+ blunders. {report.summary_version === 2 ? 'Games without assessed player moves are excluded from every histogram.' : 'Legacy pilot report: zero-move games were included; use the corrected summary-v2 run.'}</p><table><tbody>
        {report.groups.map(row => <tr key={`${row.player}-${row.mode}`}><th>{row.player} · {row.mode}</th><td>{row.histograms.old15.join(' / ')}</td><td>{row.histograms.baseline20.join(' / ')}</td><td>{row.histograms.candidate20.join(' / ')}</td></tr>)}
      </tbody></table></details>
      <details><summary>Changed moves, forced mates and threshold examples</summary>
        <p>Board transitions and arithmetic are verified; these are not human ground-truth quality labels.</p>
        <div className="algorithm-table-wrap"><table><thead><tr>{['Change', 'Player / mode', 'Game / ply', 'Move', 'Ratings', 'Before → after', 'Old / baseline / new', 'Candidate loss'].map(h => <th key={h}>{h}</th>)}</tr></thead><tbody>
          {report.examples.map((row, i) => <tr {...pageStyleTarget('row')} key={i}><td>{label(row.category)}</td><td>{row.player} · {row.mode}</td>
            <td>{row.game} / {row.ply}</td><td title={row.after_fen}>{row.san || '—'}</td><td>{row.player_rating} / {row.opponent_rating}</td>
            <td>{row.before} ({row.before_kind === 1 ? 'mate encoding' : 'cp'}) → {row.after} ({row.after_kind === 1 ? 'mate encoding' : 'cp'})</td>
            <td>{row.old15} / {row.baseline20} / {row.candidate20}</td><td>{number(row.candidate_loss)}</td></tr>)}
        </tbody></table></div>
      </details>
      <details><summary>Original model quality gates — unchanged</summary><table><tbody>{Object.entries(report.original_quality_gates).map(([key, value]) => <tr key={key}><th>{label(key)}</th><td>{label(value)}</td></tr>)}</tbody></table></details>
      {report.notes.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
    </>}
  </section>
}
