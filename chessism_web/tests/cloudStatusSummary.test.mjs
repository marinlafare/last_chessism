import test from 'node:test'
import assert from 'node:assert/strict'
import { cloudStatusSummary } from '../src/pages/system-status/cloudStatusSummary.js'

const job = { id: 'job', status: 'running', selection: { backend: 'batch_spot', n_vms: 2 },
  target: 2000000, imported: 500000, runs: [{ status: 'running' }],
  sequence: { automatic: true, phase: 'running' } }
const data = { controller_online: true, cloud_busy: true, jobs: [job] }

test('Batch cards report configured CPUs, VMs and imported FEN progress', () => {
  const view = cloudStatusSummary(data)
  assert.equal(view.cpus, 32)
  assert.equal(view.vms, 2)
  assert.equal(view.activity, 'Active')
  assert.equal(view.tone, 'queue-active')
  assert.deepEqual(view.progress, { target: 2000000, imported: 500000, percent: 25 })
})
test('100% imported stays active through cleanup and finalization', () => {
  for (const [phase, label] of [['finalizing', 'Verifying'], ['cleaning', 'Cleaning'], ['refreshing', 'Finalizing']]) {
    const view = cloudStatusSummary({ ...data, jobs: [{ ...job, imported: job.target, sequence: { automatic: true, phase } }] })
    assert.equal(view.activity, label)
    assert.equal(view.tone, 'queue-active')
  }
})
test('completed history does not keep displaying CPUs, VMs or an active job', () => {
  const view = cloudStatusSummary({ ...data, cloud_busy: false, jobs: [{ ...job, status: 'complete', runs: [{ status: 'complete' }] }] })
  assert.equal(view.job, null)
  assert.equal(view.cpus, 0)
  assert.equal(view.vms, 0)
  assert.equal(view.activity, 'Idle')
  assert.equal(view.tone, 'queue-idle')
})
test('Cloud Run displays configured CPU concurrency without inventing VMs', () => {
  const view = cloudStatusSummary({ ...data, jobs: [{ ...job, selection: { backend: 'cloud_run', n_cpus: 16 } }] })
  assert.equal(view.cpus, 16)
  assert.equal(view.vms, 0)
  assert.equal(view.isRun, true)
})
test('offline and failed refreshes preserve but mark the last known counts', () => {
  const offline = cloudStatusSummary({ ...data, controller_online: false })
  assert.equal(offline.activity, 'Controller offline')
  assert.match(offline.notice, /Google work may continue/)
  const stale = cloudStatusSummary(data, { error: new Error('network') })
  assert.equal(stale.controller, 'Unknown')
  assert.equal(stale.activity, 'Stale')
  assert.equal(stale.progress.imported, 500000)
  assert.equal(stale.tone, 'queue-stale')
})
test('initial loading and errors never claim there are no resources or an idle job', () => {
  const loading = cloudStatusSummary(null, { loading: true })
  assert.equal(loading.cpus, null)
  assert.equal(loading.activity, 'Checking…')
  assert.equal(cloudStatusSummary(null, { error: new Error('offline') }).activity, 'Unavailable')
})
test('failed and paused jobs retain progress and show attention states', () => {
  assert.equal(cloudStatusSummary({ ...data, jobs: [{ ...job, status: 'failed' }] }).tone, 'queue-stale')
  assert.equal(cloudStatusSummary({ ...data, jobs: [{ ...job, status: 'paused' }] }).activity, 'Paused')
})
test('a busy cloud status with missing job details never claims zero configured CPUs', () => {
  const view = cloudStatusSummary({ controller_online: true, cloud_busy: true, jobs: [] })
  assert.equal(view.activity, 'Active')
  assert.equal(view.cpus, null)
  assert.equal(view.vms, null)
})
