import test from 'node:test'
import assert from 'node:assert/strict'
import { recordCloudEta, estimateCloudEta, formatCloudEta } from '../src/pages/system-status/cloudEta.js'

const start = 1000000
const job = { id: 'job-a', status: 'running', imported: 1000, target: 2000,
  selection: { n_vms: 2 }, sequence: { automatic: true, phase: 'running' } }
const observe = (history, seconds, count, current = job) => recordCloudEta(history, { ...current, imported: count }, start + seconds * 1000)
const ready = () => observe(observe(observe(null, 0, 1000), 30, 1100), 60, 1200)
const estimate = (history, seconds = 60, current = { ...job, imported: 1200 }, extra = {}) => estimateCloudEta(history, current,
  { now: start + seconds * 1000, lastSuccessAt: start + seconds * 1000, available: true, ...extra })

test('joining a running job does not infer speed from previous FENs', () => {
  const history = observe(null, 0, 1000)
  assert.equal(estimate(history, 0).label, 'Calculating…')
  assert.equal(estimate(observe(history, 30, 1500), 30).seconds, null)
})
test('at least one minute and two increases are required to smooth batched imports', () => {
  const oneBatch = observe(observe(null, 0, 1000), 60, 1200)
  assert.equal(estimate(oneBatch).seconds, null)
  const eta = estimate(ready())
  assert.equal(eta.seconds, 240)
  assert.equal(eta.label, '~4 min')
  assert.equal(eta.remainingPercent, 80)
  assert.equal(estimate(ready(), 65).seconds, 235)
})
test('silent polls are included in elapsed time instead of inflating the rate', () => {
  const history = observe(ready(), 90, 1200)
  assert.equal(estimate(history, 90).seconds, 360)
})
test('stale and offline status hide any previous numeric estimate', () => {
  assert.equal(estimate(ready(), 100, job, { lastSuccessAt: start + 60000 }).label, 'Unavailable')
  assert.equal(estimate(ready(), 60, job, { available: false }).seconds, null)
})
test('long pauses hide the ETA and fresh progress rebuilds the baseline', () => {
  let history = ready()
  for (let seconds = 90; seconds <= 180; seconds += 30) history = observe(history, seconds, 1200)
  assert.equal(estimate(history, 180).label, 'Recalculating…')
  history = observe(history, 185, 1250)
  assert.equal(history.samples.length, 1)
  assert.equal(estimate(history, 185).label, 'Calculating…')
})
test('new job, changed CPUs, decreased progress and polling gaps reset the estimate', () => {
  for (const changed of [{ ...job, id: 'job-b' }, { ...job, selection: { n_vms: 1 } }, { ...job, target: 3000 }]) {
    assert.equal(observe(ready(), 65, 1250, changed).samples.length, 1)
  }
  assert.equal(observe(ready(), 65, 1100).samples.length, 1)
  assert.equal(observe(ready(), 300, 1300).samples.length, 1)
})
test('paused work and finalization never show a misleading zero ETA', () => {
  for (const phase of ['finalizing', 'cleaning', 'refreshing']) {
    const current = { ...job, sequence: { automatic: true, phase } }
    assert.equal(estimate(ready(), 60, current).label, 'Finalizing…')
    assert.equal(recordCloudEta(ready(), current, start + 60000), null)
  }
  assert.equal(estimate(ready(), 60, { ...job, imported: job.target }).seconds, null)
  assert.equal(estimate(ready(), 60, { ...job, status: 'paused' }).label, 'Paused')
})
test('Cloud Run uses the same estimator and bounded five-minute sample window', () => {
  const run = { ...job, sequence: null, selection: { backend: 'cloud_run', n_cpus: 16 }, runs: [{ status: 'running' }] }
  let history = null
  for (let seconds = 0; seconds <= 1200; seconds += 5) history = observe(history, seconds, seconds, run)
  assert.ok(history.samples.length <= 80)
  assert.ok(history.samples.at(-1).at - history.samples[0].at <= 300000)
  assert.equal(estimate(history, 1200, { ...run, imported: 1200 }).seconds, 800)
})
test('durations remain approximate and never print zero while work remains', () => {
  assert.equal(formatCloudEta(1), '~<1 min')
  assert.equal(formatCloudEta(90), '~2 min')
  assert.equal(formatCloudEta(7200), '~2h 0m')
})
