export function CloudPerformanceSummary({ run }) {
  const report = run.performance
  if (!report) return null
  const number = (value) => Number.isFinite(value) ? value.toFixed(2) : 'not measured'
  return <details>
    <summary>Performance report — saved locally</summary>
    <p>{report.analyzed_this_attempt.toLocaleString('en-US')} FENs analyzed in the reporting attempt;
      {' '}{report.resumed.toLocaleString('en-US')} restored from checkpoints.
      {' '}{number(report.metrics.worker_seconds)} seconds; {number(report.metrics.fen_per_second)} FEN/s.</p>
    <p>VM CPU busy: {number(report.metrics.vm_cpu_busy_percent)}% averaged over worker startup,
      analysis and uploads. This is not an individual-core utilization measurement.</p>
    <table><thead><tr><th>Worker</th><th>FENs</th><th>Analysis seconds</th><th>Upload queue wait seconds</th></tr></thead>
      <tbody>{report.workers.map((worker) => <tr key={worker.worker}>
        <td>{worker.worker + 1}</td><td>{worker.positions}</td>
        <td>{number(worker.analysis_seconds)}</td><td>{number(worker.upload_wait_seconds)}</td>
      </tr>)}</tbody></table>
    {run.log_cleanup && <p>Eligible log streams removed: {run.log_cleanup.deleted.length}.
      {' '}Shared/unattributed streams retained: {run.log_cleanup.retained_shared.join(', ') || 'none'}.
      Google audit, billing and monitoring history remain.</p>}
  </details>
}
