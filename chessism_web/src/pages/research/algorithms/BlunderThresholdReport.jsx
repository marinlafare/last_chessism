import { pageStyleTarget } from '../../../appearance/styleCore.js'

const n = value => Number.isFinite(value) ? value.toLocaleString(undefined, { maximumFractionDigits: 4 }) : '—'
const label = value => String(value).replaceAll('_', ' ')

function Counts({ rows, rating = false }) {
  return <div className="algorithm-table-wrap"><table>
    <caption>{rating ? 'Historical player rating and game mode' : 'Sampled player and game mode'}</caption>
    <thead><tr>{[rating ? 'Rating band' : 'Player', 'Mode', 'Assessed games', 'Assessed moves', 'Current 0.15', 'Baseline 0.20', 'Rating-aware 0.20', 'Rating-aware 0.15', 'Restored old flags', 'New flags from lowering', 'Still removed'].map(h => <th key={h}>{h}</th>)}</tr></thead>
    <tbody>{rows.map(r => <tr {...pageStyleTarget('row')} key={`${r.player || r.rating_band}-${r.mode}`}>
      <th>{rating ? r.rating_band : r.player === '*' ? 'All sampled players' : r.player}</th><td>{r.mode}</td>
      {[r.assessed_games, r.moves-r.unassessed, r.old15, r.baseline20, r.candidate20, r.candidate15,
        r.changes.restored_old, r.changes.new_from_lowering, r.changes.still_removed].map((v, i) => <td key={i}>{n(v)}</td>)}
    </tr>)}</tbody>
  </table></div>
}

export default function BlunderThresholdReport({ report }) {
  return <section className="algorithm-panel" aria-label="Blunder threshold comparison report">
    <h3 {...pageStyleTarget('title')}>Threshold comparison — live results unchanged</h3>
    <p {...pageStyleTarget('text')}>{n(report.source.games)} identical sampled games · {n(report.checked_moves)} player moves · {report.hardware.name}. Frozen model; no refitting.</p>
    <p {...pageStyleTarget('text')}>Lowering the threshold changes labels, not predictions or calibration. These are sampled histories, not complete player summaries or an independent validation.</p>
    <Counts rows={report.groups} />
    <Counts rows={report.rating_groups} rating />
    <details><summary>Reference checks — mismatches</summary><table><tbody>
      {Object.entries({ ...report.parent_reference_checks, ...report.checks }).map(([k, v]) => <tr key={k}><th>{label(k)}</th><td>{n(v)}</td></tr>)}
    </tbody></table></details>
    <details><summary>Changed-move examples and controls</summary><p>Illustrative selection, not human ground-truth labels. Mate encodings are not centipawns.</p>
      <div className="algorithm-table-wrap"><table><thead><tr>{['Category', 'Player / mode', 'Game / ply', 'Move', 'Ratings', 'Before → after', 'Current / rating 0.20 / rating 0.15', 'Expected-score loss'].map(h => <th key={h}>{h}</th>)}</tr></thead><tbody>
        {report.examples.map(r => <tr {...pageStyleTarget('row')} key={`${r.game}-${r.ply}`}><td>{label(r.category)}</td><td>{r.player} · {r.mode}</td><td>{r.game} / {r.ply}</td><td title={r.after_fen}>{r.san}</td><td>{r.player_rating} / {r.opponent_rating}</td><td>{r.before} ({r.before_kind === 1 ? 'mate' : 'cp'}) → {r.after} ({r.after_kind === 1 ? 'mate' : 'cp'})</td><td>{r.old15} / {r.candidate20} / {r.candidate15}</td><td>{n(r.candidate_loss)}</td></tr>)}
      </tbody></table></div>
    </details>
    <details><summary>Original model quality gates — unchanged</summary><table><tbody>{Object.entries(report.original_quality_gates).map(([k, v]) => <tr key={k}><th>{label(k)}</th><td>{label(v)}</td></tr>)}</tbody></table></details>
    <details><summary>Measured comparison timings</summary><table><tbody>{Object.entries(report.timings).map(([k, v]) => <tr key={k}><th>{label(k)}</th><td>{n(v)} s</td></tr>)}</tbody></table></details>
    {report.notes.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
  </section>
}
