import { useState } from 'react'
import { pageStyleTarget } from '../../../appearance/styleCore.js'

export default function BlunderTraining({ onRun, disabled = false }) {
  const [games, setGames] = useState('1000')
  return <section {...pageStyleTarget('blunder.training.panel')} className="algorithm-panel" aria-label="Rating-aware blunder model">
    <h2 {...pageStyleTarget('title')}>Rating-aware blunders</h2>
    <p {...pageStyleTarget('text')}>Fit a candidate expected-score curve on the GPU for each rating band and game mode. Whole games are split into training, validation and test sets. Live calculations stay unchanged.</p>
    <form className="algorithm-actions" onSubmit={event => { event.preventDefault(); if (!disabled) onRun(Number(games)) }}>
      <label {...pageStyleTarget('label')}>Maximum games per rating/mode group
        <input {...pageStyleTarget('field')} type="number" min="100" max="3000" step="1" required value={games} onChange={event => setGames(event.target.value)} disabled={disabled} />
      </label>
      <button {...pageStyleTarget('button')} type="submit" disabled={disabled}>Fit candidate on GPU</button>
    </form>
    <p {...pageStyleTarget('text')}>At most 3 million positions. Requires at least 500 training, 50 validation and 50 test games per group. Unsupported groups remain unavailable; this does not activate a new blunder algorithm.</p>
  </section>
}

const number = value => Number.isFinite(value) ? value.toLocaleString(undefined, { maximumFractionDigits: 4 }) : '—'

export function BlunderTrainingReport({ report }) {
  return <section {...pageStyleTarget('blunder.training.report')} className="algorithm-panel" aria-label="Rating-aware blunder report">
    <p {...pageStyleTarget('text')}>Candidate · {report.fitted_groups} / 27 groups fitted · CPU/CUDA check: {report.cpu_cuda_passed ? 'passed' : 'not passed'} · Not canonical</p>
    <p {...pageStyleTarget('text')}>{number(report.source.selected_games)} selected games · {number(report.included_rows)} fitting positions · {report.hardware.name}</p>
    <div className="algorithm-table-wrap"><table>
      <thead><tr>{['Rating','Mode','Status','Train / validation / test games','Coefficient','Test MSE: model / baseline','Old / new test blunders'].map(title => <th {...pageStyleTarget('label')} key={title}>{title}</th>)}</tr></thead>
      <tbody>{report.fits.map(fit => <tr {...pageStyleTarget('row')} key={fit.group}>
        <td>{fit.rating_band}</td><td>{fit.mode}</td><td>{fit.status.replaceAll('_',' ')}{fit.at_search_boundary ? ' · boundary fit' : ''}</td>
        <td>{['training','validation','test'].map(key => number(fit.samples[key].games)).join(' / ')}</td>
        <td>{fit.coefficient?.toFixed(7) || '—'}</td>
        <td>{fit.metrics ? `${number(fit.metrics.test.expected_points_mse)} / ${number(fit.metrics.test.baseline_mse)}` : '—'}</td>
        <td>{number(fit.blunder_comparison.old_blunders)} / {number(fit.blunder_comparison.new_blunders)}</td>
      </tr>)}</tbody>
    </table></div>
    <p {...pageStyleTarget('text')}>Read/prepare: {number(report.timings.extraction_seconds)} s · Fit and CPU reference: {number(report.timings.fitting_and_reference_seconds)} s · Held-out comparison: {number(report.timings.held_out_comparison_seconds)} s</p>
    {report.notes.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
  </section>
}
