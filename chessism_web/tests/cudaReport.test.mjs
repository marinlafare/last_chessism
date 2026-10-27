import assert from 'node:assert/strict'
import test from 'node:test'
import { parseCudaReport } from '../src/pages/research/algorithms/cudaReport.js'

const report = () => ({ format: 'chessism_cuda_comparison_v2', algorithm: 'score-distribution', backend: 'cuda_cupy',
  algorithm_version:2, input_contract:'score_distribution_v2', scored_only:true,
  status: 'passed', canonical: false, rows: 10, input_sha256: 'a'.repeat(64), row_mismatches: 0, counter_mismatches: 0,
  groups: [], input_source: 'synthetic', hardware: { name: 'test GPU' }, timings: {
    cpu_reference_seconds: .001, cuda_setup_seconds: .1, host_to_device_seconds: .001,
    calculation_seconds: .001, device_to_host_seconds: .001, total_warm_seconds: .003,
  } })
test('opens a bounded report without accepting it as canonical', () => {
  const parsed = parseCudaReport(JSON.stringify(report()))
  assert.equal(parsed.status, 'passed')
  assert.equal(parsed.canonical, false)
  assert.equal(parsed.input_source, 'synthetic')
})
test('rejects contradictory or malformed reports', () => {
  for (const changes of [{ canonical: true }, { rows: 100001 }, { row_mismatches: 1 },
    { backend: 'numpy_cpu' }, { input_sha256: 'test' }, { timings: {} },
    { scored_only:false }, { input_contract:'score_distribution_v1' },
    { groups: [{ player: 'a', mode: 'blitz', cpu: [], cuda: [] }] }]) {
    assert.throws(() => parseCudaReport(JSON.stringify({ ...report(), ...changes })))
  }
  assert.throws(() => parseCudaReport(' '.repeat(2 * 1024 * 1024 + 1)))
})
test('reads historical v1 reports without treating them as the new contract', () => {
  const old = {...report(),format:'chessism_cuda_comparison_v1',algorithm_version:1,input_contract:'score_distribution_v1'}
  delete old.scored_only
  assert.equal(parseCudaReport(JSON.stringify(old)).scored_only, undefined)
})
test('live validation cannot claim a pass if either production reference disagrees', () => {
  const live = {...report(),input_source:'frozen_export',validation_scope:'live_player',source:{player:'alice',truncated:false},
    reference_checks:{existing_sql:{status:'passed',mismatches:0},saved_cpu_summary:{status:'passed',mismatches:0}},
    timings:{...report().timings,gpu_preflight_seconds:.1,extraction_seconds:.1,sql_reference_seconds:.1,validation_wall_seconds:1}}
  assert.equal(parseCudaReport(JSON.stringify(live)).status,'passed')
  live.reference_checks.saved_cpu_summary={status:'failed',mismatches:1}
  assert.throws(()=>parseCudaReport(JSON.stringify(live)))
  live.status='failed'
  assert.equal(parseCudaReport(JSON.stringify(live)).status,'failed')
})
test('larger reports require the bounded live streaming contract', () => {
  const live = {...report(), rows:1000000, streaming_version:1, input_encoding:'score_distribution_le64_v1', peak_batch_rows:8192,
    input_source:'frozen_export',validation_scope:'live_player',
    source:{player:'alice',truncated:false,extraction_version:2,rows:1000000,expected_rows:1000000,temporary_input_bytes:32000000},
    reference_checks:{existing_sql:{status:'passed',mismatches:0},saved_cpu_summary:{status:'passed',mismatches:0}},
    timings:{...report().timings,gpu_preflight_seconds:.1,extraction_seconds:.1,sql_reference_seconds:.1,validation_wall_seconds:1,stream_comparison_seconds:.1}}
  assert.equal(parseCudaReport(JSON.stringify(live)).rows,1000000)
  for (const changes of [{rows:2000001},{peak_batch_rows:1000000},{streaming_version:undefined},{validation_scope:'sample'},
    {source:{...live.source,expected_rows:999999}},{input_encoding:'other'}]) {
    assert.throws(()=>parseCudaReport(JSON.stringify({...live,...changes})))
  }
})
test('benchmark timings and comparisons cannot contradict their claims', () => {
  const live={...report(),input_source:'frozen_export',validation_scope:'live_player',source:{player:'alice',truncated:false},
    reference_checks:{existing_sql:{status:'passed',mismatches:0},saved_cpu_summary:{status:'passed',mismatches:0}},
    timings:{...report().timings,gpu_preflight_seconds:.1,extraction_seconds:.1,sql_reference_seconds:.1,validation_wall_seconds:1},
    batch_benchmark:{version:1,status:'passed',canonical:false,repeats:3,input_reuse:'within_run_only',database_extractions:1,
      fastest_measured_gpu_batch_rows:8192,wall_seconds:1,
      variants:[8192,65536,262144].map(batch_rows=>({batch_rows,
        validation:{status:'passed',rows:10,row_mismatches:0,counter_mismatches:0,input_sha256:'a'.repeat(64),peak_batch_rows:10},
        cuda_cupy_seconds:[.1,.2,.3],numpy_cpu_seconds:[.1,.3,.2],cuda_cupy_median_seconds:.2,numpy_cpu_median_seconds:.2}))}}
  assert.equal(parseCudaReport(JSON.stringify(live)).batch_benchmark.status,'passed')
  for(const mutate of [v=>v.input_reuse='persistent',v=>v.canonical=true,v=>v.database_extractions=2,
    v=>v.fastest_measured_gpu_batch_rows=262144,v=>v.variants[0].cuda_cupy_median_seconds=0,
    v=>v.variants[0].validation.input_sha256='b'.repeat(64),v=>v.variants[1].batch_rows=2000000,
    v=>v.variants[0].validation.row_mismatches=1]) {
    const broken=structuredClone(live);mutate(broken.batch_benchmark)
    assert.throws(()=>parseCudaReport(JSON.stringify(broken)))
  }
})
