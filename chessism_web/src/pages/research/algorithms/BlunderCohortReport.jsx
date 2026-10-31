import { pageStyleTarget } from '../../../appearance/styleCore.js'

const n = v => Number.isFinite(v) ? v.toLocaleString(undefined, { maximumFractionDigits: 3 }) : '—'
const label = v => String(v).replaceAll('_', ' ')

function Counts({ rows, caption }) {
  return <div className="algorithm-table-wrap"><table><caption>{caption}</caption>
    <thead><tr>{['Player / rating', 'Mode', 'Assessed games', 'Current 0.15', 'GPU rating-aware 0.15', 'Rating-aware 0.20', 'Change', 'Unassessed moves'].map(h => <th key={h}>{h}</th>)}</tr></thead>
    <tbody>{rows.map((r, i) => <tr {...pageStyleTarget('row')} key={i}><th>{r.player || r.rating_band || 'All players'}</th><td>{r.mode}</td>
      {[r.assessed_games, r.old15, r.candidate15, r.candidate20, r.candidate15-r.old15, r.unassessed].map((v, j) => <td key={j}>{n(v)}</td>)}
    </tr>)}</tbody>
  </table></div>
}

export default function BlunderCohortReport({ report }) {
  const complete = report.format === 'chessism_blunder_cohort_report_v1'
  const rows = report.players.flatMap(p => Object.entries(p.by_mode).map(([mode, g]) => ({ player: p.player, mode, ...g })))
  return <section className="algorithm-panel" aria-label="All-player blunder shadow report">
    <h3 {...pageStyleTarget('title')}>All-player GPU summaries — not canonical</h3>
    <p {...pageStyleTarget('text')}>{report.players.length} / {report.inventory.length} player summaries saved · {complete ? report.status : 'Incomplete checkpoint'} · Live summaries unchanged.</p>
    <p {...pageStyleTarget('text')}>One combined GPU stream with bounded batches; full eligible histories, not a sample. Counts represent player perspectives, so a shared game can appear for both players.</p>
    {report.by_mode && <Counts caption="Paired cohort totals — identical eligible games" rows={Object.entries(report.by_mode).map(([mode, g]) => ({ mode, ...g }))} />}
    <Counts caption="Completed player summaries" rows={rows} />
    {report.rating_groups && <Counts caption="Historical player rating and mode" rows={report.rating_groups} />}
    <details><summary>Coverage and exclusions</summary><table><thead><tr>{['Player', 'Mode', 'Recorded games', 'Fully analyzed', 'Unsupported ratings', 'Nonstandard', 'Compared games'].map(h => <th key={h}>{h}</th>)}</tr></thead><tbody>
      {report.inventory.flatMap(p => p.scope.map(s => <tr key={`${p.player_name}-${s.mode}`}><th>{p.player_name}</th><td>{s.mode}</td>{[s.all_games, s.analyzed_games, s.unsupported_rating_games, s.nonstandard_games, s.eligible_games].map((v, i) => <td key={i}>{n(v)}</td>)}</tr>))}
    </tbody></table></details>
    <details><summary>Numerical checks and shared GPU batches</summary><p>{n(report.gpu_batches?.length || 0)} batches · {n(report.gpu_batches?.filter(b => b.players.length > 1).length || 0)} multi-player batches.</p><table><tbody>
      {Object.entries(report.checks || {}).map(([k, v]) => <tr key={k}><th>{label(k)}</th><td>{n(v)}</td></tr>)}
    </tbody></table></details>
    <details><summary>Saved research artifacts and current UI histograms</summary>
      {report.players.map(p => <div key={p.player}><h4>{p.player}</h4><p>{p.artifact.relative_path} · {n(p.artifact.records)} per-game records · SHA-256: {p.artifact.sha256}</p><table><tbody>
        {Object.entries(p.current_ui_by_mode).map(([mode, g]) => <tr key={mode}><th>{mode}</th><td>Current UI: {n(g.games)} games</td><td>{g.counts.join(' / ')}</td><td>GPU paired histogram: {p.by_mode[mode].histograms.candidate15.join(' / ')}</td></tr>)}
      </tbody></table></div>)}
    </details>
    {report.timings && <details><summary>Measured timings — includes validation work</summary><table><tbody>{Object.entries(report.timings).map(([k, v]) => <tr key={k}><th>{label(k)}</th><td>{n(v)} s</td></tr>)}</tbody></table></details>}
    <details><summary>Original model quality gates — unchanged</summary><table><tbody>{Object.entries(report.original_quality_gates).map(([k, v]) => <tr key={k}><th>{label(k)}</th><td>{label(v)}</td></tr>)}</tbody></table></details>
    {report.stale_players?.length > 0 && <p role="alert">Source changed for: {report.stale_players.join(', ')}. These are historical snapshots, not current canonical summaries.</p>}
    {report.notes.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
  </section>
}
