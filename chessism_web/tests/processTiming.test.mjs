import test from 'node:test'
import assert from 'node:assert/strict'
import { cloudStepTiming, elapsedLabel, timestamp } from '../src/pages/positions/processTiming.js'

test('elapsed clocks use the recorded start, freeze at completion, and reject missing times', () => {
  assert.equal(elapsedLabel(100, null, 165000), '1m 5s')
  assert.equal(elapsedLabel(100, 160, 999000), '1m 0s')
  assert.equal(elapsedLabel(null, null), null)
  assert.equal(timestamp('bad'), null)
})
test('cloud timings select the current cycle and never invent pending start times', () => {
  const job = { created_at: '2026-10-29T10:00:00Z', phase_timings: [
    {run_id:'old',phase:'running',started_at:'2026-10-29T10:00:00Z',finished_at:'2026-10-29T11:00:00Z'},
    {run_id:'new',phase:'running',started_at:'2026-10-29T12:00:00Z',finished_at:null},
  ]}
  assert.equal(cloudStepTiming(job,{id:'analyze',state:'current'},'new').started,'2026-10-29T12:00:00.000Z')
  assert.deepEqual(cloudStepTiming(job,{id:'analyze',state:'pending'},'new'),{})
  assert.equal(cloudStepTiming(job,{id:'analyze',state:'complete'},'old').finished,'2026-10-29T11:00:00.000Z')
  assert.equal(cloudStepTiming(job,{id:'publish',state:'complete'}).started,null)
})
