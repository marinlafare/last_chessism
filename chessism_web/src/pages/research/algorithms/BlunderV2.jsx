import { useState } from 'react'
import { pageStyleTarget } from '../../../appearance/styleCore.js'
import BlunderRefinementReport from './BlunderRefinementReport.jsx'

export default function BlunderV2({ onRun, disabled = false }) {
  const [games, setGames] = useState('1000')
  const [balanced, setBalanced] = useState(true)
  const [refinement, setRefinement] = useState(true)
  const [reuse, setReuse] = useState(true)
  return <section {...pageStyleTarget('blunder.v2.panel')} className="algorithm-panel" aria-label="Smooth rating-aware blunders">
    <h2 {...pageStyleTarget('title')}>Rating-aware blunders v2</h2>
    <p {...pageStyleTarget('text')}>Fixed baseline 0.00368208 · Smooth game-time rating adjustment · 0.20 expected-point loss. Forced-mate exceptions remain active, including +105 CP → mate against.</p>
    <p {...pageStyleTarget('text')}>Fit and validate a candidate on CUDA. Chronological and held-out-player checks; no clock or position-complexity inputs. This does not change live summaries.</p>
    <form className="algorithm-actions" onSubmit={event => { event.preventDefault(); if (!disabled) onRun(Number(games), refinement && reuse ? 'development_reuse_v1' : balanced ? 'balanced_v1' : 'legacy', refinement ? 'calibration_v1' : 'none') }}>
      <label {...pageStyleTarget('label')}>Maximum games per rating/mode group
        <input {...pageStyleTarget('field')} type="number" min="100" max="3000" step="1" required value={games} onChange={event => setGames(event.target.value)} disabled={disabled} />
      </label>
      <label {...pageStyleTarget('label')}><input type="checkbox" checked={refinement} disabled={disabled} onChange={event => { setRefinement(event.target.checked); if (event.target.checked) setBalanced(true) }} />Refine calibration</label>
      <label {...pageStyleTarget('label')}><input type="checkbox" checked={balanced} disabled={disabled || refinement} onChange={event => setBalanced(event.target.checked)} />Balanced sampling v1</label>
      {refinement && <label {...pageStyleTarget('label')}><input type="checkbox" checked={reuse} disabled={disabled} onChange={event => setReuse(event.target.checked)} />Reuse development games safely</label>}
      <button {...pageStyleTarget('button')} type="submit" disabled={disabled}>{refinement ? 'Refine calibration on GPU' : 'Fit and validate v2 on GPU'}</button>
    </form>
    {balanced && <p {...pageStyleTarget('text')}>10% player cap within each mode and partition · Rating/gap quotas · {refinement && reuse ? 'Training and validation games can be reused only in their original roles. Historical test reservations and unknown roles stay excluded; final tests use fresh games.' : 'Previously evaluated v2 games excluded.'} Sparse groups stay unfilled. This requires an earlier completed v2 cohort.</p>}
    {refinement && <p {...pageStyleTarget('text')}>28 predefined candidates per mode. Validation must meet the 3-pp calibration target, improve prediction error by at least 1%, and retain half the best candidate gain. Final test runs only if all modes qualify; otherwise it stays unevaluated. Live summaries remain unchanged.</p>}
    <p {...pageStyleTarget('text')}>At most 3 million source positions. Sparse groups remain unvalidated. CPU/GPU agreement alone does not approve this model.</p>
  </section>
}

const number = value => Number.isFinite(value) ? value.toLocaleString(undefined, { maximumFractionDigits: 6 }) : '—'
const label = value => String(value).replaceAll('_', ' ')
const points = value => Number.isFinite(value) ? `${(value * 100).toFixed(2)} pp` : '—'
const interval = values => values?.length === 2 ? `${points(values[0])} to ${points(values[1])}` : '—'

export function BlunderV2Report({ report }) {
  return <section {...pageStyleTarget('blunder.v2.report')} className="algorithm-panel" aria-label="Rating-aware v2 report">
    <h3 {...pageStyleTarget('title')}>Candidate report — not canonical</h3>
    <p {...pageStyleTarget('text')}>Live summaries unchanged · {number(report.source.selected_games)} games · {number(report.included_rows)} positions · {report.hardware.name}</p>
    <p {...pageStyleTarget('text')}>CPU/CUDA check: {report.cpu_cuda_passed ? 'passed' : 'not passed'} · Move mismatches: {number(report.decision_mismatches)} · Per-game mismatches: {number(report.per_game_count_mismatches)}</p>
    <p {...pageStyleTarget('text')}>Assessed moves: {number(report.assessed_moves)} · Unassessed transitions: {number(report.unassessed_moves)} (not zero-blunder observations)</p>
    {report.refinement && <BlunderRefinementReport report={report} />}
    {report.source.sampling_audit && <details open><summary>{report.source.sampling_audit.reuse_policy ? 'Role-locked development reuse — coverage and player concentration' : 'Balanced sampling v1 — coverage and player concentration'}</summary>
      {report.source.sampling_audit.reuse_policy ? <p {...pageStyleTarget('text')}>Reused training games: {number(report.source.sampling_audit.reused_training_games)} · Reused validation games: {number(report.source.sampling_audit.reused_validation_games)} · Protected games excluded: {number(report.source.sampling_audit.excluded_protected_games)} · Final-test overlap: {number(report.source.sampling_audit.test_prior_overlap)} · Role violations: {number(report.source.sampling_audit.role_violations)}</p>
        : <p {...pageStyleTarget('text')}>Previously evaluated games excluded: {number(report.source.sampling_audit.excluded_prior_games)} · Overlap: {number(report.source.sampling_audit.prior_game_overlap)} · Both players count toward the cap.</p>}
      <div className="algorithm-table-wrap"><table><thead><tr>{['Partition', 'Mode', 'CP games', 'Largest player share', 'Balance'].map(title => <th key={title}>{title}</th>)}</tr></thead>
        <tbody>{report.source.sampling_audit.cp_concentration.map(row => <tr {...pageStyleTarget('row')} key={`${row.split}-${row.mode}`}><th>{['Training', 'Validation', 'Test', 'Held-out players'][row.split]}</th><td>{row.mode}</td><td>{number(row.games)}</td><td>{row.largest_player_share == null ? '—' : `${(row.largest_player_share * 100).toFixed(2)}%`}</td><td>{label(row.status)}</td></tr>)}</tbody>
      </table></div>
      <p {...pageStyleTarget('text')}>Balanced sample, not population-weighted results. Candidate pools are bounded; missing cells and games without CP observations can still limit validation.</p>
    </details>}
    <div className="algorithm-table-wrap"><table>
      <caption>Validation gates</caption><thead><tr><th>Check</th><th>Result</th></tr></thead>
      <tbody>{Object.entries(report.gates).map(([key, value]) => <tr {...pageStyleTarget('row')} key={key}><th>{label(key)}</th><td>{label(value)}</td></tr>)}</tbody>
    </table></div>
    <div className="algorithm-table-wrap"><table>
      <caption>Fitted parameters — historical ratings, fixed baseline</caption><thead><tr>{['Mode', 'Status', 'a', 'b', 'Regularization', 'Train / validation / test / held-out games'].map(title => <th key={title}>{title}</th>)}</tr></thead>
      <tbody>{report.fits.map(fit => <tr {...pageStyleTarget('row')} key={fit.mode}><th>{fit.mode}</th><td>{label(fit.status)}</td><td>{number(fit.a)}</td><td>{number(fit.b)}</td><td>{number(fit.regularization)}</td><td>{fit.games_by_split.map(number).join(' / ')}</td></tr>)}</tbody>
    </table></div>
    {report.refinement?.test_evaluated !== false && <><div className="algorithm-table-wrap"><table>
      <caption>Test blunders — separates threshold and rating effects</caption><thead><tr>{['Mode', 'Old 0.15', 'Baseline 0.20', 'Rating-aware 0.20'].map(title => <th key={title}>{title}</th>)}</tr></thead>
      <tbody>{report.blunder_comparison.map(row => <tr {...pageStyleTarget('row')} key={row.mode}><th>{row.mode}</th><td>{number(row.old15)}</td><td>{number(row.baseline20)}</td><td>{number(row.candidate20)}</td></tr>)}</tbody>
    </table></div>
    <p {...pageStyleTarget('text')}>Test MSE: {number(report.test.mse)} · Baseline: {number(report.test.baseline_mse)} · Calibration error: {number(report.test.ece)}</p>
    {report.calibration_diagnostics_version && <>
      <div className="algorithm-table-wrap"><table>
        <caption>Calibration comparison — lower is better; percentage points (pp)</caption>
        <thead><tr>{['Sample', 'Games', 'Baseline', 'Rating-aware', 'Change (new − old)', '95% interval for change'].map(title => <th key={title}>{title}</th>)}</tr></thead>
        <tbody>{[['Test', report.test], ['Held-out players', report.held_out_players]].map(([name, row]) => <tr {...pageStyleTarget('row')} key={name}>
          <th>{name}</th><td>{number(row.games)}</td><td>{points(row.baseline_ece)}</td><td>{points(row.ece)}</td><td>{points(row.ece_change)}</td><td>{interval(row.calibration_uncertainty?.ece_change_ci95)}</td>
        </tr>)}</tbody>
      </table></div>
      <p {...pageStyleTarget('text')}>Same held-out games for both models. Intervals resample whole games, not individual moves; shared players and sparse groups limit certainty. Calibration is separate from the blunder threshold.</p>
      <details><summary>Which prediction ranges contribute to calibration error?</summary><div className="algorithm-table-wrap"><table>
        <thead><tr>{['Model', 'Range', 'Predicted points', 'Observed points', 'Error contribution'].map(title => <th key={title}>{title}</th>)}</tr></thead>
        <tbody>{[['Rating-aware', report.test.calibration], ['Baseline', report.test.baseline_calibration]].flatMap(([name, bins]) => (bins || []).map(row => <tr {...pageStyleTarget('row')} key={`${name}-${row.bin}`}>
          <th>{name}</th><td>{row.bin / 10}–{(row.bin + 1) / 10}</td><td>{number(row.predicted)}</td><td>{number(row.observed)}</td><td>{points(row.ece_contribution)}</td>
        </tr>))}</tbody>
      </table></div></details>
    </>}</>}
    <details><summary>Subgroup validation and coverage</summary><div className="algorithm-table-wrap"><table>
      <thead><tr>{['Group', 'Status', 'Test games', 'MSE', 'Baseline MSE', 'Calibration error', 'Baseline calibration'].map(title => <th key={title}>{title}</th>)}</tr></thead>
      <tbody>{report.strata.map(row => <tr {...pageStyleTarget('row')} key={row.name}><th>{row.name}</th><td>{label(row.status)}</td><td>{number(row.games)}</td><td>{number(row.mse)}</td><td>{number(row.baseline_mse)}</td><td>{points(row.ece)}</td><td>{points(row.baseline_ece)}</td></tr>)}</tbody>
    </table></div></details>
    <p {...pageStyleTarget('text')}>Read/prepare: {number(report.timings.extraction_seconds)} s · Fit: {number(report.timings.fitting_seconds)} s · Total: {number(report.timings.total_compute_seconds)} s</p>
    {report.notes.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
  </section>
}
