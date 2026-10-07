import test from 'node:test'
import assert from 'node:assert/strict'
import { cloudDefaults, cloudRunDefaults, cloudPayload, cloudGamePreview, cloudBound, cloudGameSelectionError,
  CLOUD_STALL_TIMEOUT_SECONDS, MAX_CLOUD_FENS, MAX_CLOUD_RUNS } from '../src/pages/positions/cloudAnalysisForm.js'
import { DEFAULT_ANALYSIS_NODES } from '../src/pages/positions/positionPageSupport.js'

test('Batch VM count and Cloud Run CPU count are independent and bounded', () => {
  assert.equal(cloudDefaults.backend, 'batch_spot')
  assert.equal(cloudPayload('all', cloudDefaults).backend, 'batch_spot')
  assert.equal(Object.hasOwn(cloudPayload('all', cloudDefaults), 'n_cpus'), false)
  assert.equal(cloudPayload('all', cloudDefaults).n_vms, 1)
  for (const n_vms of [1, 2, 10]) {
    const form = { ...cloudDefaults, n_vms: String(n_vms) }
    assert.equal(cloudPayload('all', form).n_vms, n_vms)
    assert.match(cloudBound('all', form), new RegExp(`up to ${n_vms} Spot VMs`))
    assert.match(cloudBound('all', form), /16 vCPUs and 16 GiB RAM/)
  }
  for (const n_vms of [0, 11, 1.5, '', 'no']) {
    assert.throws(() => cloudPayload('all', { ...cloudDefaults, n_vms }))
  }
  for (const cpus of [1, 4, 64]) {
    const form = { ...cloudDefaults, backend: 'cloud_run', n_cpus: String(cpus) }
    assert.equal(cloudPayload('all', form).n_cpus, cpus)
    assert.equal(Object.hasOwn(cloudPayload('all', form), 'n_vms'), false)
    assert.match(cloudBound('all', form), /Cloud Run/)
  }
  for (const n_cpus of [0, 65, 1.5, 'no', '']) {
    assert.throws(() => cloudPayload('all', { ...cloudDefaults, backend: 'cloud_run', n_cpus }))
  }
  assert.throws(() => cloudPayload('all', { ...cloudDefaults, backend: 'unknown' }))
})

test('dedicated Cloud Run card supports every FEN selection and always submits CPU concurrency', () => {
  assert.equal(cloudRunDefaults.backend, 'cloud_run')
  assert.equal(cloudRunDefaults.n_cpus, 4)
  const preview = { plan_id: 'b'.repeat(32), fens_to_analyze: 2082 }
  for (const mode of ['all', 'player', 'loop', 'games']) {
    const form = { ...cloudRunDefaults, player: ' magnuscarlsen ', n_cpus: '8', total: 1500,
      scope: 'player', positions: 500, runs: 3 }
    const payload = cloudPayload(mode, form, preview)
    assert.equal(payload.backend, 'cloud_run')
    assert.equal(payload.n_cpus, 8)
    assert.equal(payload.mode, mode)
    assert.equal(payload.player_name, mode === 'all' ? '' : 'magnuscarlsen')
    assert.equal(payload.plan_id, mode === 'games' ? preview.plan_id : null)
    assert.equal(payload.total_fens, mode === 'games' ? undefined : 1500)
    assert.equal(payload.positions_per_run, 500)
    assert.equal(payload.runs, 3)
    assert.equal(Object.hasOwn(payload, 'nodes'), false)
    assert.equal(Object.hasOwn(payload, 'stall_timeout_seconds'), false)
    assert.match(cloudBound(mode, form, preview), /up to 8 simultaneous/)
  }
})

test('node limit is fixed at 100,000 and never sent as a cloud user parameter', () => {
  assert.equal(DEFAULT_ANALYSIS_NODES, 100_000)
  assert.equal(Object.hasOwn(cloudDefaults, 'nodes'), false)
  for (const mode of ['all', 'player', 'loop', 'games']) {
    const payload = cloudPayload(mode, { ...cloudDefaults, player: 'magnuscarlsen', nodes: 999999 }, { plan_id: 'a'.repeat(32), fens_to_analyze: 1 })
    assert.equal(Object.hasOwn(payload, 'nodes'), false)
  }
})

test('game jobs use the preview count, not an editable FEN bound', () => {
  const form = { ...cloudDefaults, player: 'magnuscarlsen', games: 100, total: 1 }
  const preview = { plan_id: 'a'.repeat(32), fens_to_analyze: 6967 }
  assert.equal(cloudGamePreview(form).game_limit, 100)
  assert.equal(cloudGamePreview(form).selection_mode, 'latest')
  const payload = cloudPayload('games', form, preview)
  assert.equal(Object.hasOwn(payload, 'total_fens'), false)
  assert.equal(Object.hasOwn(payload, 'stall_timeout_seconds'), false)
  assert.equal(Object.hasOwn(payload, 'max_task_seconds'), false)
  assert.match(cloudBound('games', form, preview), /One cloud request for 6,967 FENs.*300 seconds without a new completed FEN/)
  assert.match(cloudBound('games', { ...form, seconds: 900 }, preview), /300 seconds without a new completed FEN/)
})

test('stall timeout is system-owned for every cloud mode, including stale form data', () => {
  assert.equal(CLOUD_STALL_TIMEOUT_SECONDS, 300)
  assert.equal(Object.hasOwn(cloudDefaults, 'seconds'), false)
  for (const mode of ['all', 'player', 'loop', 'games']) {
    const payload = cloudPayload(mode, { ...cloudDefaults, player: 'magnuscarlsen',
      seconds: 900, stall_timeout_seconds: 900 }, { plan_id: 'a'.repeat(32), fens_to_analyze: 1 })
    assert.equal(Object.hasOwn(payload, 'stall_timeout_seconds'), false)
    assert.equal(Object.hasOwn(payload, 'max_task_seconds'), false)
  }
})

test('invalid or oversized game previews cannot submit and are never silently capped', () => {
  assert.match(cloudBound('games', cloudDefaults), /Preview games/)
  for (const count of [undefined, null, '6967', -1, 0, 200001, 2.5]) {
    const preview = { plan_id: 'a'.repeat(32), fens_to_analyze: count }
    assert.ok(cloudGameSelectionError(preview))
    assert.throws(() => cloudPayload('games', cloudDefaults, preview))
  }
  const preview = { plan_id: 'a'.repeat(32), fens_to_analyze: 200000 }
  assert.equal(cloudGameSelectionError(preview), '')
  assert.match(cloudBound('games', cloudDefaults, preview), /One cloud request for 200,000 FENs/)
})

test('cloud jobs keep the total selection bound across a VM fleet', () => {
  assert.equal(MAX_CLOUD_FENS, 200000)
  assert.equal(MAX_CLOUD_RUNS, 200)
  for (const count of [10001, 199999, 200000]) {
    const preview = { plan_id: 'a'.repeat(32), fens_to_analyze: count }
    assert.equal(cloudGameSelectionError(preview), '')
    assert.equal(cloudPayload('games', cloudDefaults, preview).plan_id, preview.plan_id)
    for (const mode of ['all', 'player']) {
      assert.equal(cloudPayload(mode, { ...cloudDefaults, player: 'magnuscarlsen', total: count }).total_fens, count)
    }
  }
  const loop = { ...cloudDefaults, positions: 1000, runs: 200 }
  assert.equal(cloudPayload('loop', loop).runs, 200)
  assert.match(cloudBound('loop', loop), /One cloud request for 200,000 FENs/)
  assert.match(cloudGameSelectionError({ plan_id: 'a'.repeat(32), fens_to_analyze: 200001 }), /200,000-FEN/)
  assert.match(cloudBound('games', cloudDefaults, { plan_id: 'a'.repeat(32), fens_to_analyze: 2082 }), /One cloud request for 2,082 FENs/)
})

test('global scope cannot accidentally inherit a player filter', () => {
  assert.equal(cloudPayload('all', { ...cloudDefaults, player: 'someone' }).player_name, '')
  assert.equal(cloudPayload('loop', { ...cloudDefaults, player: 'someone' }).player_name, '')
})
test('player and game requests require their selections', () => {
  assert.throws(() => cloudPayload('player', cloudDefaults), /player/)
  assert.throws(() => cloudPayload('games', cloudDefaults), /Preview/)
  const payload = cloudPayload('games', cloudDefaults, { plan_id: 'a'.repeat(32), fens_to_analyze: 1 })
  assert.equal(payload.plan_id, 'a'.repeat(32))
})
test('preview mirrors range ordering and all games choice', () => {
  const request = cloudGamePreview({ ...cloudDefaults, player: ' example ', selection: 'range', allGames: true, order: 'oldest' })
  assert.equal(request.player_name, 'example')
  assert.equal(request.use_all, true)
  assert.equal(request.range_order, 'oldest')
})
test('workload disclosure includes bounded retries but no false runtime or spending cap', () => {
  assert.match(cloudBound('all', { ...cloudDefaults, total: 1500 }), /One cloud request for 1,500 FENs.*preemptions retry automatically.*two attempts total/)
  assert.match(cloudBound('loop', { ...cloudDefaults, runs: 3 }), /One cloud request for 3,000 FENs/)
  assert.match(cloudBound('all', cloudDefaults), /No fixed task-hour or monetary cap/)
})
