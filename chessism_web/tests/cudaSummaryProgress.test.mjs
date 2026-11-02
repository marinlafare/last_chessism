import test from 'node:test'
import assert from 'node:assert/strict'
import { cudaSummaryProgress } from '../src/pages/positions/cudaSummaryProgress.js'

const run = { status: 'running', total_games: 100, processed_games: 100, timings: {
  metadata: { complete: true, rows: 999 }, salience: { complete: true, rows: 888 },
} }

test('only the current stage supplies counts, not earlier completed game work', () => {
  const progress = cudaSummaryProgress({ ...run, phase: 'Preparing position frequencies — 23,000 occurrences',
    timings: { ...run.timings, position_frequency: { rows: 23000, prepared: false } } })
  assert.equal(progress.phase, 'Preparing position frequencies')
  assert.equal(progress.detail, '23,000 occurrences prepared')
  assert.equal(progress.style, 'cuda.progress.position_frequency')
  assert.equal(progress.value, undefined)
  assert.equal(progress.moving, true)
})

test('frequency calculation and publication have separate truthful denominators', () => {
  const state = { prepared: true, shards: 4, shard_total: 32, published: 1, players: [{}, {}, {}] }
  const compute = cudaSummaryProgress({ ...run, phase: 'Counting repeated positions on RTX 4060', timings: { position_frequency: state } })
  assert.equal(compute.value, 4); assert.equal(compute.max, 32)
  const publish = cudaSummaryProgress({ ...run, phase: 'Publishing position frequencies', timings: { position_frequency: state } })
  assert.equal(publish.value, 1); assert.equal(publish.max, 3)
  assert.equal(publish.detail, '1 / 3 players')
})

test('metadata shows during a mixed run as well as a metadata-only run', () => {
  const progress = cudaSummaryProgress({ ...run, phase: 'Preparing behavioral and ending inputs',
    timings: { metadata: { ready: false, rows: 120 } } })
  assert.equal(progress.detail, '120 player-games prepared')
  assert.equal(progress.value, undefined)
  const calculated = cudaSummaryProgress({ ...run, phase: 'Calculating metadata on RTX 4060',
    timings: { metadata: { ready: true, computed: 1, chunks: [{}, {}] } } })
  assert.equal(calculated.value, 1); assert.equal(calculated.max, 2)
})

test('preparation reports saved inputs rather than zero games calculated', () => {
  const progress = cudaSummaryProgress({ ...run, processed_games: 0, phase: 'Preparing CUDA inputs',
    timings: { input_snapshot: { games: 25 } } })
  assert.equal(progress.value, 25); assert.equal(progress.max, 100)
  assert.equal(progress.detail, '25 / 100 games prepared')
})

test('salience and map details disappear when another stage starts', () => {
  const salience = { prepared: false, games: 10, rows: 300 }
  const map = { prepared: false, captured_games: 45 }
  assert.equal(cudaSummaryProgress({ ...run, phase: 'Preparing salience inputs', timings: { salience } }).detail,
    '10 games · 300 early positions')
  assert.equal(cudaSummaryProgress({ ...run, phase: 'Preparing collapsed ply map', timings: { salience, ply_map: map } }).detail,
    '45 games captured')
  assert.equal(cudaSummaryProgress({ ...run, phase: 'Publishing score distributions', timings: { salience, ply_map: map } }).detail, '')
})

test('publishing without a known denominator never displays overall completion', () => {
  for (const phase of ['Publishing score distributions', 'Publishing behavioral and ending summaries', 'Publishing salience and refreshing hero daily summaries']) {
    const progress = cudaSummaryProgress({ ...run, phase })
    assert.equal(progress.value, undefined); assert.equal(progress.detail, '')
  }
})

test('missing stage totals cannot become a default percentage or a zero denominator', () => {
  const progress = cudaSummaryProgress({ ...run, phase: 'Calculating salience on RTX 4060',
    timings: { salience: { prepared: true, shards: 2 } } })
  assert.equal(progress.value, undefined); assert.equal(progress.max, null)
  assert.equal(progress.detail, '2 groups calculated')
})

test('terminal and waiting states do not animate or retain old stage legends', () => {
  for (const status of ['failed', 'queued', 'retrying']) {
    const progress = cudaSummaryProgress({ ...run, status, phase: 'Retry required' })
    assert.equal(progress.moving, false); assert.equal(progress.detail, '')
  }
  assert.equal(cudaSummaryProgress({ ...run, phase: 'Waiting for active cloud analysis' }).moving, false)
  const complete = cudaSummaryProgress({ ...run, status: 'complete', phase: 'Complete' })
  assert.equal(complete.value, complete.max); assert.equal(complete.detail, '')
  assert.equal(cudaSummaryProgress(null).moving, false)
})

test('salience reduction has its own step instead of a finished 32-of-32 meter', () => {
  const salience = { prepared: true, shards: 32, shard_total: 32, games: 500, reduced_games: 100 }
  const progress = cudaSummaryProgress({ ...run, phase: 'Salience CUDA groups — 32 / 32', timings: { salience } })
  assert.equal(progress.phase, 'Finalizing salience')
  assert.equal(progress.value, 100); assert.equal(progress.max, 500)
  assert.equal(progress.detail, '100 / 500 games finalized')
  const published = cudaSummaryProgress({ ...run, phase: 'Reducing game salience on RTX 4060',
    timings: { salience: { ...salience, reduced: true, reduced_games: 500 } } })
  assert.equal(published.phase, 'Publishing salience'); assert.equal(published.value, undefined)
})

test('counter-only polls preserve the title while retaining current detail', () => {
  const first = cudaSummaryProgress({ ...run, phase: 'Grouping ply map memberships on RTX 4060 — 100 pairs' })
  const next = cudaSummaryProgress({ ...run, phase: 'Grouping ply map memberships on RTX 4060 — 200 pairs' })
  assert.equal(first.phase, next.phase)
  assert.equal(next.detail, '200 pairs')
})
