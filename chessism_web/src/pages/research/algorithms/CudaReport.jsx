import { pageStyleTarget } from '../../../appearance/styleCore.js'
import CudaBenchmark from './CudaBenchmark'

export default function CudaReport({ report }) {
  const ms = value => `${(value * 1000).toFixed(2)} ms`
  const live = report.validation_scope === 'live_player'
  const timings = [
    ...(live ? [['GPU device initialization', 'gpu_preflight_seconds'], ['Database extraction', 'extraction_seconds'], ['Existing SQL calculation', 'sql_reference_seconds']] : []),
    ['CPU reference', 'cpu_reference_seconds'], ['CUDA setup / warm-up', 'cuda_setup_seconds'],
    ['Transfer to GPU', 'host_to_device_seconds'], ['GPU calculation', 'calculation_seconds'],
    ['Transfer back', 'device_to_host_seconds'], ['Warm GPU path', 'total_warm_seconds'],
    ...(report.streaming_version === 1 ? [['Stream comparison (includes CPU checks)', 'stream_comparison_seconds']] : []),
    ...(live ? [['Whole validation', 'validation_wall_seconds']] : []),
  ]
  return <div {...pageStyleTarget('cuda.validation.result')} role="status">
    <h3 {...pageStyleTarget('title')}>Score distribution · {report.status}</h3>
    <p {...pageStyleTarget('text')}>{report.scored_only ? 'Scored-only input · version 2' : 'Historical version 1 report — not validation of the scored-only matrix'}</p>
    <p {...pageStyleTarget('text')}>{report.rows.toLocaleString()} rows · {report.input_source === 'synthetic' ? 'Synthetic fixtures — not live-data validation' : live ? `${report.source.player} · entire scored history · all three modes` : 'Frozen exported sample'} · {report.hardware.name}</p>
    <p {...pageStyleTarget('text')}>Row mismatches: {report.row_mismatches} · Counter mismatches: {report.counter_mismatches}</p>
    {live && <>
      <p {...pageStyleTarget('text')}>Existing SQL: {report.reference_checks.existing_sql.status} · Saved CPU summary: {report.reference_checks.saved_cpu_summary.status}</p>
      <p {...pageStyleTarget('text')}>SQL mismatches: {report.reference_checks.existing_sql.mismatches} · Saved-summary mismatches: {report.reference_checks.saved_cpu_summary.mismatches}</p>
      <p {...pageStyleTarget('text')}>Validation only. Live chart summaries and the canonical algorithm are unchanged.</p>
      {report.streaming_version === 1 && <p {...pageStyleTarget('text')}>Bounded streaming · at most {report.peak_batch_rows.toLocaleString()} rows per batch · {(report.source.temporary_input_bytes / 1024 / 1024).toFixed(2)} MiB temporary input (removed after the run).</p>}
    </>}
    <dl className="algorithm-validation-timings">
      {timings.map(([name, key]) => <div key={key}><dt>{name}</dt><dd>{ms(report.timings[key])}</dd></div>)}
    </dl>
    <p {...pageStyleTarget('text')}>{live ? 'Whole validation includes CPU/SQL checks and GPU initialization; it is not the GPU-only production cost. SQL and extraction timings are order/cache-sensitive.' : 'Excludes database extraction and publication.'} The CPU reference is a correctness oracle, not an optimized performance baseline.</p>
    {report.batch_benchmark && <CudaBenchmark benchmark={report.batch_benchmark} extractionSeconds={report.timings.extraction_seconds} />}
    <details><summary {...pageStyleTarget('disclosure')}>Input fingerprint</summary><code className="algorithm-validation-digest">{report.input_sha256}</code></details>
  </div>
}
