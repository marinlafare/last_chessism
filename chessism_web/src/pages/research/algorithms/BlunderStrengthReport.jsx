import { useState } from 'react'
import { pageStyleTarget } from '../../../appearance/styleCore.js'

const n = v => Number.isFinite(v) ? v.toLocaleString(undefined, { maximumFractionDigits: 3 }) : '—'

export default function BlunderStrengthReport({ report }) {
  const [kind, setKind] = useState('mode')
  if (report.format === 'chessism_blunder_strength_reservation_v1') return <section aria-label="Rating-strength comparison report">
    <h3 {...pageStyleTarget('title')}>Rating-strength comparison — in progress</h3>
    <p {...pageStyleTarget('text')}>Sample identities reserved. No completed comparison yet. These games remain excluded from future fresh tests even if this run is interrupted. Live summaries are unchanged.</p>
  </section>
  const labels = Object.fromEntries(report.candidates.map(c => [c.id, c.label]))
  return <section aria-label="Rating-strength comparison report">
    <h3 {...pageStyleTarget('title')}>Weaker rating adjustments — exploratory comparison</h3>
    <p {...pageStyleTarget('text')}>{n(report.source.selected_games)} unique games · {n(report.source.selected_players.length)} players · {report.hardware.name} · {n(report.included_rows)} moves</p>
    <p {...pageStyleTarget('text')}>Fixed coefficient: 0.00368208 · Fixed threshold: 0.15 · Eight candidates share the same input. No model selected; live summaries unchanged.</p>
    <label {...pageStyleTarget('label')}>Compare groups <select {...pageStyleTarget('input')} value={kind} onChange={e => setKind(e.target.value)}>
      <option value="mode">Game mode</option><option value="rating_mode">Historical rating and mode</option>
      <option value="gap_mode">Rating gap and mode</option><option value="all">Entire diagnostic sample</option>
    </select></label>
    <div className="algorithm-table-wrap"><table><caption>Paired candidates — same games and thresholds</caption>
      <thead><tr>{['Group', 'Candidate', 'Games', 'Current CPU blunders', 'Candidate blunders', 'MSE', 'Calibration error (pp)', 'Players', 'Largest owner share'].map(h => <th key={h}>{h}</th>)}</tr></thead>
      <tbody>{report.groups.filter(g => g.kind === kind).map(g => <tr {...pageStyleTarget('row')} key={`${g.group}-${g.candidate}`}>
        <th>{g.group}</th><td>{labels[g.candidate]}</td><td>{n(g.games)}</td><td>{n(g.old15)}</td><td>{n(g.blunders)}</td>
        <td>{n(g.mse)}</td><td>{n(g.ece_pp)}</td><td>{n(g.players)}</td><td>{n(g.largest_owner_share*100)}%</td>
      </tr>)}</tbody>
    </table></div>
    <p {...pageStyleTarget('text')}>Calibration is descriptive, game-weighted expected game points after the selected player moves. This stratified sample is not a population estimate or a fresh independent test.</p>
    <details><summary>Changed-move examples — not human quality labels</summary>
      {report.examples.map(e => <article {...pageStyleTarget('card')} key={`${e.game}-${e.ply}`}><h4 {...pageStyleTarget('title')}>{e.player} · {e.mode} · {e.san || `ply ${e.ply}`}</h4>
        <p>Ratings {e.player_rating} / {e.opponent_rating} · {e.before} → {e.after} cp · Current CPU: {e.old15 ? 'blunder' : 'not a blunder'}</p>
        <table><thead><tr><th>Candidate</th><th>Expected-result loss (pp)</th><th>Blunder?</th></tr></thead><tbody>
          {Object.entries(e.candidates).map(([k, v]) => <tr key={k}><th>{labels[k]}</th><td>{n(v.loss*100)}</td><td>{v.blunder === 1 ? 'Yes' : 'No'}</td></tr>)}
        </tbody></table>
      </article>)}
    </details>
    <details><summary>Coverage, safeguards and timings</summary>
      <p>{n(report.gpu_batches.length)} GPU batches · {n(report.gpu_batches.filter(b => b.players.length > 1).length)} multi-player batches · prior research-game overlap: {report.source.prior_game_overlap}</p>
      <table><tbody>{Object.entries(report.checks).map(([k, v]) => <tr key={k}><th>{k.replaceAll('_', ' ')}</th><td>{n(v)}</td></tr>)}</tbody></table>
      <p>Maximum prediction error: {report.max_prediction_error} · maximum metric error: {report.max_metric_error}</p>
      <table><tbody>{Object.entries(report.timings).map(([k, v]) => <tr key={k}><th>{k.replaceAll('_', ' ')}</th><td>{n(v)} s</td></tr>)}</tbody></table>
      <pre>{JSON.stringify(report.source.selection, null, 2)}</pre>
    </details>
    <details><summary>Original model-quality gates — unchanged</summary><table><tbody>{Object.entries(report.original_quality_gates).map(([k, v]) => <tr key={k}><th>{k.replaceAll('_', ' ')}</th><td>{v.replaceAll('_', ' ')}</td></tr>)}</tbody></table></details>
    {report.notes.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
  </section>
}
