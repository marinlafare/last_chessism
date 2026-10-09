import RelationshipScatter from './RelationshipScatter'
import BuilderResults from './BuilderResults'
import { duration, number } from './algorithmForm'

function color(value) {
  if (value == null) return '#171e24'
  const opacity = 0.1 + Math.abs(value) * 0.65
  return value < 0 ? 'rgba(201,109,65,' + opacity + ')' : 'rgba(69,153,206,' + opacity + ')'
}

function SummaryTable({ result }) {
  return (
    <div className="algorithm-table-scroll">
      <table>
        <caption>Prepared column summaries — original units</caption>
        <thead>
          <tr><th>Column</th><th>Missing in source</th><th>Mean</th><th>Std. deviation</th><th>Min</th><th>Max</th></tr>
        </thead>
        <tbody>
          {result.summaries.map((item) => (
            <tr key={item.column}>
              <th scope="row">{item.column}{item.constant ? ' (constant)' : ''}</th>
              <td>{item.column in result.missing_by_column ? number(result.missing_by_column[item.column]) : 'derived'}</td>
              <td>{number(item.mean)}</td><td>{number(item.std)}</td>
              <td>{number(item.min)}</td><td>{number(item.max)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function CorrelationTable({ result }) {
  return (
    <div className="algorithm-table-scroll">
      <table className="algorithm-heatmap">
        <caption>Pearson correlations · −1 inverse · 0 no linear relationship · +1 direct</caption>
        <thead>
          <tr>
            <th scope="col">Column</th>
            {result.columns.map((column) => <th scope="col" key={column}>{column}</th>)}
          </tr>
        </thead>
        <tbody>
          {result.columns.map((column, row) => (
            <tr key={column}>
              <th scope="row">{column}</th>
              {result.correlations[row].map((value, col) => (
                <td key={col} style={{ backgroundColor: color(value) }} title={column + ' / ' + result.columns[col]}>
                  {value == null ? 'N/A' : value.toFixed(3)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default function AlgorithmResults({ run, onClose }) {
  const result = run.result
  if (result?.format === 'algorithm_builder_v2') return <BuilderResults run={run} onClose={onClose} />
  return (
    <section className="algorithm-panel algorithm-results" aria-label="Algorithm results">
      <div className="algorithm-heading">
        <div><p className="eyebrow">RESULTS · {run.status}</p><h2>{run.name}</h2></div>
        <button type="button" onClick={onClose}>Close results</button>
      </div>
      {run.error && <p role="alert">{run.error}</p>}
      {!result ? <p>No completed result is available for this run.</p> : (
        <>
          <div className="algorithm-metrics">
            {[
              ['Source rows', result.input_rows], ['Included rows', result.included_rows],
              ['Excluded rows', result.excluded_rows], ['Imputed rows', result.imputed_rows],
            ].map(([label, count]) => (
              <div key={label}><span>{label}</span><strong>{number(count)}</strong></div>
            ))}
          </div>
          <p>
            Extraction {duration(result.timings?.extraction_seconds)} · calculation
            {' '}{duration(result.timings?.calculation_seconds)} · {result.backend} / {result.precision}
          </p>
          {result.source.row_limit_reached && (
            <p className="algorithm-warning">
              The configured row limit was reached. This is an ordered prefix of the matching data,
              not a random sample of the full database.
            </p>
          )}
          <SummaryTable result={result} />
          <CorrelationTable result={result} />
          <RelationshipScatter scatter={result.scatter} />
          {result.warnings.map((warning) => <p key={warning} className="algorithm-warning">{warning}</p>)}
          <details>
            <summary>Source and calculation record</summary>
            <p>Snapshot taken: {result.source.taken_at}</p>
            <p>Input SHA-256: <code>{result.source.input_sha256}</code></p>
            <p>
              Sampling seed: {result.seed} · calculation version: {result.implementation_version} ·
              temporary inputs removed, not backed up.
            </p>
          </details>
        </>
      )}
    </section>
  )
}
