import test from 'node:test'
import assert from 'node:assert/strict'
import { blockingCloudJob, cloudIsBusy, fenProgress } from '../src/pages/positions/cloudJobState.js'

test('queued, running and recoverable jobs all block another cloud submission', () => {
  for (const status of ['queued', 'running', 'paused', 'waiting', 'failed']) {
    const job = { id: 'one', status }
    assert.equal(blockingCloudJob({ jobs: [job] }), job)
    assert.equal(cloudIsBusy({ jobs: [job] }), true)
  }
  for (const status of ['complete', 'cancelled', 'limit_reached']) {
    assert.equal(cloudIsBusy({ jobs: [{ status }] }), false)
  }
})

test('server blocker wins and busy flag protects against missing history rows', () => {
  const jobs = [{ id: 'new', status: 'queued' }, { id: 'old', status: 'paused' }]
  assert.equal(blockingCloudJob({ jobs, blocking_job_id: 'old' }), jobs[1])
  assert.equal(cloudIsBusy({ jobs: [], cloud_busy: true }), true)
})

test('progress measures committed FENs, not estimated engine progress', () => {
  assert.deepEqual(fenProgress({ target: 2082, imported: 500 }), { target: 2082, imported: 500, percent: 24 })
  assert.equal(fenProgress({ target: 2082, imported: 2082 }).percent, 100)
  assert.deepEqual(fenProgress({ target: 0, imported: 12 }), { target: 0, imported: 0, percent: 0 })
  assert.equal(fenProgress({ target: 2082, imported: 3000 }).imported, 2082)
  assert.equal(fenProgress({ target: 2082, imported: -1 }).imported, 0)
  assert.equal(fenProgress({}).percent, 0)
})
