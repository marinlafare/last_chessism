import { pageStyleTarget } from '../../../appearance/styleCore.js'

const number = value => Number.isFinite(value) ? value.toLocaleString(undefined, { maximumFractionDigits: 4 }) : '—'

export function BlunderValidationReport({ report }) {
  return <section {...pageStyleTarget('blunder.fixed.report')} className="algorithm-panel" aria-label="Fixed-coefficient blunder report">
    <p {...pageStyleTarget('text')}>{report.status === 'passed' ? 'Validation passed' : 'Validation failed'} · Not canonical · Live summaries unchanged</p>
    <p {...pageStyleTarget('text')}>{report.source.player} · {number(report.source.games)} games · {number(report.rows)} own moves · {report.hardware.name}</p>
    <p {...pageStyleTarget('text')}>Coefficient: {report.rules.coefficient} · Drop threshold: {report.rules.probability_drop} · Rating-aware: no</p>
    <div className="algorithm-table-wrap"><table {...pageStyleTarget('table')}>
      <thead><tr><th {...pageStyleTarget('table.header')}>Check</th><th {...pageStyleTarget('table.header')}>Mismatches</th></tr></thead>
      <tbody>{Object.entries(report.checks).map(([key, count]) => <tr {...pageStyleTarget('row')} key={key}>
        <td {...pageStyleTarget('table.cell')}>{({ cpu_moves: 'CPU / CUDA moves', sql_moves: 'SQL / CUDA moves', cpu_games: 'CPU / CUDA game totals', sql_games: 'SQL / CUDA game totals', saved_games: 'Saved CPU game totals', saved_move_counts: 'Saved move counts' })[key] || key}</td>
        <td {...pageStyleTarget('table.cell')}>{number(count)}</td>
      </tr>)}</tbody>
    </table></div>
    <div className="algorithm-table-wrap"><table {...pageStyleTarget('table')}>
      <thead><tr>{['Mode', 'Games', 'CPU blunders', 'GPU blunders', 'SQL blunders', 'Saved blunders'].map(label => <th {...pageStyleTarget('table.header')} key={label}>{label}</th>)}</tr></thead>
      <tbody>{report.groups.map(group => <tr {...pageStyleTarget('row')} key={group.mode}>
        <td {...pageStyleTarget('table.cell')}>{group.mode}</td>{['games', 'cpu', 'cuda', 'sql', 'saved'].map(key => <td {...pageStyleTarget('table.cell')} key={key}>{number(group[key])}</td>)}
      </tr>)}</tbody>
    </table></div>
    <p {...pageStyleTarget('text')}>Unassessed tablebase transitions: {number(report.unassessed_moves)}</p>
    <p {...pageStyleTarget('text')}>Metadata: {number(report.timings.metadata_seconds)} s · Extract/prepare: {number(report.timings.extraction_seconds)} s · SQL reference: {number(report.timings.sql_reference_seconds)} s · CPU reference: {number(report.timings.cpu_reference_seconds)} s</p>
    <p {...pageStyleTarget('text')}>GPU setup: {number(report.timings.gpu_preflight_seconds)} s · Warm-up: {number(report.timings.cuda_warmup_seconds)} s · Transfer to GPU: {number(report.timings.host_to_device_seconds)} s · GPU calculation: {number(report.timings.gpu_calculation_seconds)} s · Transfer back: {number(report.timings.device_to_host_seconds)} s · Validation total: {number(report.timings.validation_wall_seconds)} s</p>
    {report.notes.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
    <details><summary {...pageStyleTarget('disclosure')}>Input identity and mismatch examples</summary>
      <p {...pageStyleTarget('text')}>Input SHA-256: {report.input_sha256}</p>
      <pre {...pageStyleTarget('text')}>{JSON.stringify({ moves: report.mismatch_examples, games: report.game_mismatch_examples }, null, 2)}</pre>
    </details>
  </section>
}
