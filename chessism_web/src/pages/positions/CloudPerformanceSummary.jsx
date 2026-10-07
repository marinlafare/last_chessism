export function CloudPerformanceSummary({ run }) {
  const report = run.performance
  if (!report) return null
  const number = (value) => Number.isFinite(value) ? value.toFixed(2) : 'not measured'
  if (report.backend === 'batch_spot') return <details>
    <summary>Batch Spot performance report — saved locally</summary>
    <p>{report.position_count.toLocaleString('en-US')} FENs across {report.vm_count} Spot VMs;
      {' '}{report.cpus_per_vm} vCPUs and {report.memory_gib_per_vm} GiB RAM per VM ({report.machine_type}).</p>
    <p>{number(report.worker_seconds_sum)} summed VM worker seconds. This is not elapsed wall time or billed duration.</p>
    <table><thead><tr><th>VM</th><th>FENs</th><th>Restored</th><th>Attempt seconds</th><th>FEN/s</th></tr></thead>
      <tbody>{report.vms.map((vm) => <tr key={vm.index}>
        <td>{vm.index + 1}</td><td>{vm.positions}</td><td>{vm.resumed}</td>
        <td>{number(vm.metrics.worker_seconds)}</td><td>{number(vm.metrics.fen_per_second)}</td>
      </tr>)}</tbody></table>
    <p>Detailed per-engine reports are retained locally. Interrupted attempts can add runtime and charges.</p>
  </details>
  if (report.backend === 'cloud_run') return <details>
    <summary>Cloud Run performance report — saved locally</summary>
    <p>{report.position_count.toLocaleString('en-US')} FENs across {report.task_count} tasks;
      up to {report.parallelism} simultaneous CPUs.</p>
    <p>{number(report.worker_seconds_sum)} summed task seconds. This is not elapsed wall time or billed duration.</p>
    <p>Detailed task reports and execution timestamps are retained locally.</p>
  </details>
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
