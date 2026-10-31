import { pageStyleTarget } from '../../../appearance/styleCore.js'

const number = value => Number.isFinite(value) ? value.toLocaleString(undefined, { maximumFractionDigits: 6 }) : '—'
const points = value => Number.isFinite(value) ? `${(value * 100).toFixed(2)} pp` : '—'
const label = value => String(value).replaceAll('_', ' ')

export default function BlunderRefinementReport({ report }) {
  const development = report.refinement.development
  return <section aria-label="Calibration refinement report">
    <h4 {...pageStyleTarget('title')}>Calibration-aware refinement — development results</h4>
    <p {...pageStyleTarget('text')}>{report.final_evaluation
      ? 'Frozen final evaluation: rapid’s development calibration and sparse coverage were provisionally acknowledged before testing. Parameters were not refitted; failed gates remain visible. Live summaries are unchanged.'
      : report.refinement.test_evaluated
      ? 'Validation selection passed in all modes. A separate untouched test was evaluated once; see its gates below.'
      : 'Final test not evaluated. No candidate qualified in every mode. Parameters below are diagnostic only; no acceptance claim.'}</p>
    <p {...pageStyleTarget('text')}>Validation data selected these parameters; its results are not independent test evidence. Fixed coefficient 0.00368208 · Blunder threshold 0.20 · Explicit mate rules unchanged.</p>
    <div className="algorithm-table-wrap"><table>
      <caption>Selection per mode</caption><thead><tr>{['Mode', 'Selection', 'Rating strength', 'Validation MSE', 'Baseline MSE', 'Validation calibration', 'Baseline calibration', 'Eligible candidates'].map(title => <th key={title}>{title}</th>)}</tr></thead>
      <tbody>{report.fits.map(fit => <tr {...pageStyleTarget('row')} key={fit.mode}>
        <th>{fit.mode}</th><td>{label(fit.selection?.status)}</td><td>{number(fit.rating_strength)}</td>
        <td>{number(fit.validation_mse)}</td><td>{number(fit.selection?.baseline?.mse)}</td><td>{points(fit.validation_ece)}</td>
        <td>{points(fit.selection?.baseline?.ece)}</td><td>{number(fit.selection?.eligible_candidates)}</td>
      </tr>)}</tbody>
    </table></div>
    <details><summary>All predefined candidates — validation only</summary>
      <div className="algorithm-table-wrap"><table><thead><tr>{['Mode', 'Regularization', 'Strength', 'MSE', 'Calibration', 'Predictive requirement', 'Calibration requirement', 'Search valid'].map(title => <th key={title}>{title}</th>)}</tr></thead>
        <tbody>{report.fits.flatMap(fit => (fit.refinement_trials || []).map((trial, index) => <tr {...pageStyleTarget('row')} key={`${fit.mode}-${index}`}>
          <th>{fit.mode}</th><td>{number(trial.regularization)}</td><td>{number(trial.rating_strength)}</td><td>{number(trial.mse)}</td><td>{points(trial.ece)}</td>
          <td>{trial.predictive_eligible ? 'passed' : 'not passed'}</td><td>{trial.calibration_eligible ? 'passed' : 'not passed'}</td><td>{trial.converged && !trial.search_boundary ? 'yes' : 'no'}</td>
        </tr>))}</tbody></table></div>
    </details>
    {development && <details><summary>Development diagnostics by mode, rating, rating gap and phase</summary>
      <div className="algorithm-table-wrap"><table><thead><tr>{['Group', 'Games', 'MSE', 'Baseline MSE', 'Calibration', 'Baseline calibration'].map(title => <th key={title}>{title}</th>)}</tr></thead>
        <tbody>{[...development.groups, ...Object.entries(development.phases).map(([name, row]) => ({ name, ...row }))].map(row => <tr {...pageStyleTarget('row')} key={row.name}>
          <th>{row.name}</th><td>{number(row.games)}</td><td>{number(row.mse)}</td><td>{number(row.baseline_mse)}</td><td>{points(row.ece)}</td><td>{points(row.baseline_ece)}</td>
        </tr>)}</tbody></table></div>
    </details>}
    {report.player_cluster_uncertainty && <details open><summary>Repeated-player uncertainty — both colors kept together</summary>
      {Object.entries(report.player_cluster_uncertainty).map(([name, row]) => <p {...pageStyleTarget('text')} key={name}>
        {label(name)}: {label(row.status)} · {number(row.components)} independent player components
        {row.ece_ci95 ? ` · Calibration interval: ${points(row.ece_ci95[0])} to ${points(row.ece_ci95[1])}` : ` · ${row.reason}`}
      </p>)}
    </details>}
    <p {...pageStyleTarget('text')}>A representative-population audit and production recovery/performance tests are still required. Automatic promotion is disabled.</p>
  </section>
}
