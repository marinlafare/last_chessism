import { pageStyleTarget } from '../../appearance/styleCore.js'

export function cloudPerformanceTitle(report) {
  return report.backend === 'batch_spot' ? 'Batch Spot performance report'
    : report.backend === 'cloud_run' ? 'Cloud Run performance report' : 'Performance report'
}

export function CloudPerformanceSummary({ run, onOpen }) {
  if (!run.performance) return null
  return <button {...pageStyleTarget('cloud.performance.toggle')} className="cloud-performance-button" type="button"
    aria-haspopup="dialog" onClick={() => onOpen(run)}>
    {cloudPerformanceTitle(run.performance)} — saved locally
  </button>
}

export function CloudPerformanceDetails({ run }) {
  const report = run.performance
  if (!report) return null
  const number = (value) => Number.isFinite(value) ? value.toFixed(2) : 'not measured'
  if (report.backend === 'batch_spot') return <>
    <p {...pageStyleTarget('cloud.performance.batch.summary')}>{report.position_count.toLocaleString('en-US')} FENs across {report.vm_count} Spot VMs;
      {' '}{report.cpus_per_vm} vCPUs and {report.memory_gib_per_vm} GiB RAM per VM ({report.machine_type}).</p>
    <p {...pageStyleTarget('cloud.performance.batch.duration')}>{number(report.worker_seconds_sum)} summed VM worker seconds. This is not elapsed wall time or billed duration.</p>
    <table {...pageStyleTarget('cloud.performance.batch.table')}><thead><tr><th {...pageStyleTarget('cloud.performance.batch.vm.header')}>VM</th><th {...pageStyleTarget('cloud.performance.batch.fens.header')}>FENs</th><th {...pageStyleTarget('cloud.performance.batch.restored.header')}>Restored</th><th {...pageStyleTarget('cloud.performance.batch.seconds.header')}>Attempt seconds</th><th {...pageStyleTarget('cloud.performance.batch.rate.header')}>FEN/s</th></tr></thead>
      <tbody>{report.vms.map((vm) => <tr key={vm.index}>
        <td {...pageStyleTarget('cloud.performance.batch.vm.cell')}>{vm.index + 1}</td><td {...pageStyleTarget('cloud.performance.batch.fens.cell')}>{vm.positions}</td><td {...pageStyleTarget('cloud.performance.batch.restored.cell')}>{vm.resumed}</td>
        <td {...pageStyleTarget('cloud.performance.batch.seconds.cell')}>{number(vm.metrics.worker_seconds)}</td><td {...pageStyleTarget('cloud.performance.batch.rate.cell')}>{number(vm.metrics.fen_per_second)}</td>
      </tr>)}</tbody></table>
    <p {...pageStyleTarget('cloud.performance.batch.note')}>Detailed per-engine reports are retained locally. Interrupted attempts can add runtime and charges.</p>
  </>
  if (report.backend === 'cloud_run') return <>
    <p {...pageStyleTarget('cloud.performance.run.summary')}>{report.position_count.toLocaleString('en-US')} FENs across {report.task_count} tasks;
      up to {report.parallelism} simultaneous CPUs.</p>
    <p {...pageStyleTarget('cloud.performance.run.duration')}>{number(report.worker_seconds_sum)} summed task seconds. This is not elapsed wall time or billed duration.</p>
    <p {...pageStyleTarget('cloud.performance.run.note')}>Detailed task reports and execution timestamps are retained locally.</p>
  </>
  return <>
    <p {...pageStyleTarget('cloud.performance.worker.summary')}>{report.analyzed_this_attempt.toLocaleString('en-US')} FENs analyzed in the reporting attempt;
      {' '}{report.resumed.toLocaleString('en-US')} restored from checkpoints.
      {' '}{number(report.metrics.worker_seconds)} seconds; {number(report.metrics.fen_per_second)} FEN/s.</p>
    <p {...pageStyleTarget('cloud.performance.worker.cpu')}>VM CPU busy: {number(report.metrics.vm_cpu_busy_percent)}% averaged over worker startup,
      analysis and uploads. This is not an individual-core utilization measurement.</p>
    <table {...pageStyleTarget('cloud.performance.worker.table')}><thead><tr><th {...pageStyleTarget('cloud.performance.worker.worker.header')}>Worker</th><th {...pageStyleTarget('cloud.performance.worker.fens.header')}>FENs</th><th {...pageStyleTarget('cloud.performance.worker.analysis.header')}>Analysis seconds</th><th {...pageStyleTarget('cloud.performance.worker.upload.header')}>Upload queue wait seconds</th></tr></thead>
      <tbody>{report.workers.map((worker) => <tr key={worker.worker}>
        <td {...pageStyleTarget('cloud.performance.worker.worker.cell')}>{worker.worker + 1}</td><td {...pageStyleTarget('cloud.performance.worker.fens.cell')}>{worker.positions}</td>
        <td {...pageStyleTarget('cloud.performance.worker.analysis.cell')}>{number(worker.analysis_seconds)}</td><td {...pageStyleTarget('cloud.performance.worker.upload.cell')}>{number(worker.upload_wait_seconds)}</td>
      </tr>)}</tbody></table>
    {run.log_cleanup && <p {...pageStyleTarget('cloud.performance.worker.cleanup')}>Eligible log streams removed: {run.log_cleanup.deleted.length}.
      {' '}Shared/unattributed streams retained: {run.log_cleanup.retained_shared.join(', ') || 'none'}.
      Google audit, billing and monitoring history remain.</p>}
  </>
}
