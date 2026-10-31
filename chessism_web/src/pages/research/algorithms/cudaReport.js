export function parseCudaReport(text) {
  if (text.length > 2 * 1024 * 1024) throw Error('Report exceeds 2 MiB.')
  const report = JSON.parse(text)
  const version = report?.format === 'chessism_cuda_comparison_v2' ? 2 : report?.format === 'chessism_cuda_comparison_v1' ? 1 : 0
  const streamed = report?.streaming_version === 1
  if (!version || report.algorithm !== 'score-distribution' ||
      report.backend !== 'cuda_cupy' || !['passed', 'failed'].includes(report.status) || report.canonical !== false ||
      !Number.isInteger(report.rows) || report.rows < 1 || report.rows > (streamed ? 2000000 : 100000) ||
      !/^[a-f0-9]{64}$/.test(report.input_sha256) ||
      !Number.isInteger(report.row_mismatches) || report.row_mismatches < 0 || report.row_mismatches > report.rows ||
      !Number.isInteger(report.counter_mismatches) || report.counter_mismatches < 0 ||
      !Array.isArray(report.groups) || report.groups.length > 512 ||
      !['synthetic', 'frozen_export'].includes(report.input_source)) throw Error('Unsupported CUDA validation report.')
  if (version === 2 && (report.scored_only !== true || report.input_contract !== 'score_distribution_v2' || report.algorithm_version !== 2)) throw Error('Invalid scored-only input contract.')
  if (streamed && (version !== 2 || report.validation_scope !== 'live_player' ||
      report.input_encoding !== 'score_distribution_le64_v1' || report.source?.extraction_version !== 2 ||
      report.source.rows !== report.rows || report.source.expected_rows !== report.rows ||
      report.source.temporary_input_bytes !== report.rows * 32 ||
      !Number.isInteger(report.peak_batch_rows) || report.peak_batch_rows < 1 || report.peak_batch_rows > 8192 ||
      !Number.isFinite(report.timings?.stream_comparison_seconds) || report.timings.stream_comparison_seconds < 0)) throw Error('Invalid streamed input contract.')
  if (report.status === 'passed' && (report.row_mismatches || report.counter_mismatches)) throw Error('Report status contradicts its mismatches.')
  if (report.validation_scope === 'live_player') {
    if (version !== 2 || report.input_source !== 'frozen_export' || typeof report.source?.player !== 'string' || report.source.truncated !== false) throw Error('Invalid live-player source record.')
    for (const key of ['existing_sql', 'saved_cpu_summary']) {
      const check = report.reference_checks?.[key]
      if (!check || !Number.isSafeInteger(check.mismatches) || check.mismatches < 0 || check.status !== (check.mismatches ? 'failed' : 'passed') || (report.status === 'passed' && check.status !== 'passed')) throw Error('Invalid SQL/summary comparison.')
    }
    for (const key of ['gpu_preflight_seconds', 'extraction_seconds', 'sql_reference_seconds', 'validation_wall_seconds']) {
      if (!Number.isFinite(report.timings?.[key]) || report.timings[key] < 0) throw Error('Invalid live-player timing.')
    }
  }
  for (const group of report.groups) {
    if (typeof group.player !== 'string' || typeof group.mode !== 'string' ||
        !['cpu', 'cuda'].every(key => Array.isArray(group[key]) && group[key].length === (version === 2 ? 15 : 16) && group[key].every(n => Number.isSafeInteger(n) && n >= 0))) {
      throw Error('Invalid group counters.')
    }
  }
  const keys = ['cpu_reference_seconds', 'cuda_setup_seconds', 'host_to_device_seconds', 'calculation_seconds', 'device_to_host_seconds', 'total_warm_seconds']
  if (!keys.every(key => Number.isFinite(report.timings?.[key]) && report.timings[key] >= 0) || typeof report.hardware?.name !== 'string') throw Error('Invalid timing/hardware metadata.')
  if (report.batch_benchmark !== undefined) validateBenchmark(report)
  return report
}

function validateBenchmark(report) {
  const value = report.batch_benchmark
  const sizes = [8192, 65536, 262144]
  if (report.validation_scope !== 'live_player' || !value || value.version !== 1 || value.status !== 'passed' ||
      value.canonical !== false || value.input_reuse !== 'within_run_only' || value.database_extractions !== 1 ||
      value.repeats !== 3 || !Array.isArray(value.variants) || value.variants.length !== sizes.length ||
      !sizes.includes(value.fastest_measured_gpu_batch_rows) || !Number.isFinite(value.wall_seconds) || value.wall_seconds < 0) throw Error('Invalid batch benchmark.')
  for (const [index, row] of value.variants.entries()) {
    const check = row.validation
    if (row.batch_rows !== sizes[index] || check?.status !== 'passed' || check.rows !== report.rows ||
        check.input_sha256 !== report.input_sha256 || check.row_mismatches !== 0 || check.counter_mismatches !== 0 ||
        !Number.isInteger(check.peak_batch_rows) || check.peak_batch_rows < 1 || check.peak_batch_rows > row.batch_rows) throw Error('Invalid batch verification.')
    for (const backend of ['cuda_cupy', 'numpy_cpu']) {
      const times = row[`${backend}_seconds`]
      const middle = row[`${backend}_median_seconds`]
      if (!Array.isArray(times) || times.length !== 3 || !times.every(n => Number.isFinite(n) && n >= 0) ||
          middle !== [...times].sort((a, b) => a - b)[1]) throw Error('Invalid benchmark timings.')
    }
  }
  const best = value.variants.reduce((a, b) => a.cuda_cupy_median_seconds <= b.cuda_cupy_median_seconds ? a : b)
  if (best.batch_rows !== value.fastest_measured_gpu_batch_rows) throw Error('Benchmark selection contradicts timings.')
}
