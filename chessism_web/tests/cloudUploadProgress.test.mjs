import test from 'node:test'
import assert from 'node:assert/strict'
import { attachUploadProgress, uploadStepProgress } from '../src/pages/positions/cloudUploadProgress.js'
import { cloudJobProgress, cloudStepProgress } from '../src/pages/positions/cloudJobProgress.js'

const job = () => ({ id: 'job', status: 'running', target: 3760444,
  runs: [{ id: 'run', status: 'uploading', positions: 3760444 }] })

test('upload rows show exact FEN counts including zero and partial completion', () => {
  const selected = job(), step = cloudJobProgress(selected).steps[3]
  for (const count of [0, 5000, 1800000, 3760444]) {
    selected.upload_progress = { state: 'ready', uploaded_fens: count, total_fens: 3760444 }
    const progress = cloudStepProgress(selected, step)
    assert.equal(progress.value, count / 3760444 * 100)
    assert.ok(progress.detail.includes(`${count.toLocaleString()} / ${3760444..toLocaleString()} FENs uploaded`))
  }
  selected.upload_progress.uploaded_fens = 3760443
  assert.match(cloudStepProgress(selected, step).detail, /99%$/)
})

test('unavailable or malformed observations stay indeterminate', () => {
  const selected = job(), step = cloudJobProgress(selected).steps[3]
  for (const observation of [undefined, { state: 'pending' }, { state: 'unavailable' },
    ...[-1, 3760445, null, '1000', NaN].map(uploaded_fens => ({ state: 'ready', uploaded_fens, total_fens: 3760444 }))]) {
    selected.upload_progress = observation
    assert.equal(cloudStepProgress(selected, step).value, null)
  }
  assert.equal(uploadStepProgress(selected, { ...step, state: 'pending' }), null)
})

test('a completed upload retains its confirmed total in the bar tooltip', () => {
  const selected = job()
  selected.runs[0].status = 'run_discover'
  const progress = cloudStepProgress(selected, cloudJobProgress(selected).steps[3])
  assert.equal(progress.value, 100)
  assert.match(progress.detail, /FENs uploaded · 100%$/)
})

test('telemetry requests follow only active uploads and preserve cancellation signals', async () => {
  const selected = job(), calls = [], options = { signal: new AbortController().signal }
  const snapshot = { jobs: [selected, { ...job(), id: 'old', status: 'complete' },
    { ...job(), id: 'stopping', stop_cleanup: { phase: 'stopping' } }] }
  const enriched = await attachUploadProgress(snapshot, async (id, supplied) => {
    calls.push(id); assert.equal(supplied, options)
    return { state: 'ready', uploaded_fens: 5000, total_fens: 3760444 }
  }, options)
  assert.deepEqual(calls, ['job'])
  assert.equal(enriched.jobs[0].upload_progress.uploaded_fens, 5000)
  assert.equal(selected.upload_progress, undefined)
  const unavailable = await attachUploadProgress(snapshot, async () => { throw Error('reader unavailable') })
  assert.equal(unavailable.jobs[0].status, 'running')
  assert.equal(unavailable.jobs[0].upload_progress.state, 'unavailable')
})

test('Batch uploads use the same counter contract', async () => {
  const selected = { ...job(), sequence: { automatic: true, phase: 'uploading' } }
  const data = await attachUploadProgress({ jobs: [selected] }, async () => ({
    state: 'ready', total_fens: 1000000, uploaded_fens: 50000 }))
  assert.equal(cloudStepProgress(data.jobs[0], cloudJobProgress(selected).steps[3]).value, 5)
})
