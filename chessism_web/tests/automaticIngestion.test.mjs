import assert from 'node:assert/strict'
import { test } from 'node:test'
import { readAutomaticIngestion } from '../src/pages/scored-positions/automaticIngestion.js'

const job = (id, kind = 'fen_extraction', phase = 'saving_fens', updated_at = 10) => ({
  job_id: id, status: 'in_progress', progress: { kind, phase, updated_at, processed: 3, total: 10 },
})
const read = (previous, jobs, fetchJob = async () => { throw new Error('Unexpected status read') }) => (
  readAutomaticIngestion(previous, { fetchActive: async () => ({ jobs }), fetchJob })
)

test('discovers automatic work only and chooses the latest job for a stage', async () => {
  const result = await read({}, [job('old'), job('new', 'fen_extraction', 'saving_fens', 20),
    job('analysis', 'most_repeated'), job('tb', 'tablebase_analysis'), job('parse', 'game_update')])
  assert.deepEqual(Object.keys(result).sort(), ['fen', 'gameParsing', 'tablebase'])
  assert.equal(result.fen.jobId, 'new')
  assert.equal(result.fen.status.progress.processed, 3)
})

test('recognizes queued automatic work by function without a progress kind', async () => {
  const result = await read({}, [{ job_id: 'tb', status: 'queued', info: { function: 'run_tablebase_analysis_job' } }])
  assert.equal(result.tablebase.status.status, 'queued')
})

test('reads disappearing jobs to distinguish completion from failure', async () => {
  const initial = await read({}, [job('fen')])
  const result = await read(initial, [], async id => {
    assert.equal(id, 'fen')
    return { status: 'complete', progress: { phase: 'failed', detail: 'Tablebase failed' }, result: { success: false } }
  })
  assert.equal(result.fen.status.progress.phase, 'failed')
  assert.equal(result.fen.error, 'Tablebase failed')
  assert.equal((await read(result, [])).fen.error, 'Tablebase failed')
  const completed = await read(initial, [], async () => ({ status: 'complete', progress: { phase: 'complete' } }))
  assert.equal(completed.fen.status.progress.phase, 'complete')
})

test('clears expired jobs but surfaces network failures without changing previous state', async () => {
  const initial = await read({}, [job('fen')])
  assert.deepEqual(await read(initial, [], async () => { throw Object.assign(new Error('Expired'), { status: 404 }) }), {})
  assert.deepEqual(await read(initial, [], async () => ({ status: 'not_found' })), {})
  await assert.rejects(read(initial, [], async () => { throw new Error('Offline') }), /Offline/)
  assert.equal(initial.fen.status.progress.phase, 'saving_fens')
})

test('moves a job to its new stage without duplicate tracking', async () => {
  const initial = await read({}, [job('same')])
  const result = await read(initial, [job('same', 'tablebase_analysis')])
  assert.deepEqual(Object.keys(result), ['tablebase'])
})

test('passes cancellation to all status reads and rejects invalid active-job responses', async () => {
  const initial = await read({}, [job('fen')])
  const signal = new AbortController().signal
  await readAutomaticIngestion(initial, {
    signal,
    fetchActive: async options => { assert.equal(options.signal, signal); return { jobs: [] } },
    fetchJob: async (id, options) => { assert.equal(options.signal, signal); return { status: 'complete' } },
  })
  await assert.rejects(readAutomaticIngestion({}, { fetchActive: async () => ({}) }), /unavailable/)
})
