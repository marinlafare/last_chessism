import BuilderChart from './BuilderChart'
import RelationshipScatter from './RelationshipScatter'
import { duration, number } from './algorithmForm'

function ValueTable({ data }) {
  return <div className="algorithm-table-scroll"><table>
    <thead><tr>{data.columns.map((key) => <th key={key}>{key}</th>)}</tr></thead>
    <tbody>{data.rows.map((row, index) => <tr key={index} title={data.row_keys?.[index]}>
      {row.map((value, column) => <td key={column}>{value == null ? 'N/A' : typeof value === 'number' ? number(value) : String(value)}</td>)}
    </tr>)}</tbody>
  </table>{data.truncated && <p>Showing {data.rows.length} of {number(data.total_rows)} rows.</p>}</div>
}

function Heatmap({ output }) {
  return <div className="algorithm-table-scroll"><table className="algorithm-heatmap">
    <thead><tr><th>Column</th>{output.columns.map((key) => <th key={key}>{key}</th>)}</tr></thead>
    <tbody>{output.columns.map((key, row) => <tr key={key}><th>{key}</th>
      {output.correlations[row].map((value, column) => <td key={column}
        style={{ background: value == null ? '#182129' : `rgba(${value < 0 ? '201,109,65' : '69,153,206'},${.1 + Math.abs(value) * .65})` }}>
        {value == null ? 'N/A' : value.toFixed(3)}
      </td>)}
    </tr>)}</tbody>
  </table><p>−1 inverse · 0 no linear relationship · +1 direct. N/A means insufficient or constant data.</p></div>
}

function Output({ output }) {
  return <article className="algorithm-card">
    <h3>{output.name}</h3><p>{output.type} · input: {output.input} · {number(output.total_rows)} rows</p>
    {output.type === 'table' && <ValueTable data={output} />}
    {output.type === 'scalar' && <strong className="algorithm-scalar">{number(output.value)}</strong>}
    {output.type === 'heatmap' && <Heatmap output={output} />}
    {output.type === 'scatter' && <RelationshipScatter scatter={output} />}
    {['line', 'bar'].includes(output.type) && <BuilderChart output={output} />}
    {output.truncated && output.type !== 'table' && <p className="algorithm-warning">Display limited to {output.points.length} of {number(output.valid_points)} finite points. This is a display prefix, not a full-data aggregation.</p>}
  </article>
}

export default function BuilderResults({ run, onClose }) {
  const result = run.result
  return <section className="algorithm-panel algorithm-results" aria-label="Algorithm results">
    <div className="algorithm-heading">
      <div><p className="eyebrow">{result.sample_run ? 'SAMPLE RESULTS' : 'RESULTS'} · COMPLETE</p><h2>{run.name}</h2></div>
      <button type="button" onClick={onClose}>Close results</button>
    </div>
    <div className="algorithm-metrics">
      {[
        ['Source rows', result.input_rows], ['Prepared rows', result.included_rows],
        ['Excluded at preparation', result.excluded_rows], ['Imputed rows', result.imputed_rows],
      ].map(([label, value]) => <div key={label}><span>{label}</span><strong>{number(value)}</strong></div>)}
    </div>
    <p>Extraction {duration(result.timings.extraction_seconds)} · calculation {duration(result.timings.calculation_seconds)} · temporary working inputs discarded.</p>
    {result.sample_run && <p className="algorithm-warning">This tests only the first up to 500 source rows, not the full matrix. Sampling can omit groups and change aggregate values.</p>}
    {result.source.row_limit_reached && <p className="algorithm-warning">The configured source row cap was reached.</p>}
    {result.outputs.map((output, index) => <Output key={index} output={output} />)}
    <h3>Intermediate steps</h3>
    {result.steps.map((step) => <details className="algorithm-card" key={step.id}>
      <summary>{step.name} · {number(step.input_rows)} → {number(step.output_rows)} rows · {number(step.seconds)} s</summary>
      <p>Operation: {step.op}. First up to eight rows shown; row identifiers are available on hover.</p>
      <ValueTable data={step.preview} />
      <p>Missing values: {Object.entries(step.missing).map(([key, count]) => `${key}: ${count}`).join(' · ')}</p>
    </details>)}
    {result.warnings.map((warning) => <p className="algorithm-warning" key={warning}>{warning}</p>)}
    <details><summary>Source and saved instructions</summary>
      <p>Snapshot: {result.source.taken_at} · input SHA-256: <code>{result.source.input_sha256}</code></p>
      <pre>{JSON.stringify(run.config, null, 2)}</pre>
    </details>
  </section>
}
