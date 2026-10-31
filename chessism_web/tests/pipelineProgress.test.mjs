import test from 'node:test'
import assert from 'node:assert/strict'
import { pipelineSteps, stepEstimate } from '../src/pages/positions/pipelineProgress.js'

test('completed stages keep their own counts, not the next stage totals', () => {
  const steps = pipelineSteps({ run_id: 'r', stages: [
    { stage: 'fen_extraction', status: 'complete', processed: 50, total: 50 },
    { stage: 'saving_positions', status: 'running', processed: 0, total: 0 },
  ] }, { fen: { status: { progress: {
    ingestion_run_id: 'r', phase: 'saving_fens', processed: 3, total: 20,
  } } } })
  assert.equal(steps[0].total, 50)
  assert.equal(steps[0].status, 'complete')
  assert.equal(steps[1].processed, 3)
})
test('late polling from another run cannot replace progress', () => {
  const rows = pipelineSteps({ run_id: 'new', stages: [
    { stage: 'saving_positions', status: 'running', processed: 2, total: 8 },
  ] }, { fen: { status: { progress: {
    ingestion_run_id: 'old', phase: 'saving_fens', processed: 900, total: 1000,
  } } } })
  assert.equal(rows[0].processed, 2)
})
test('detailed backend steps are authoritative and unknown totals have no ETA', () => {
  const steps = [{ step: 'probe_endgames', status: 'working', total: null, processed: 20, rate: 10 }]
  assert.deepEqual(pipelineSteps({ steps }), steps)
  assert.equal(stepEstimate(steps[0]), null)
  assert.equal(stepEstimate({ ...steps[0], total: 100, fixed_total: true }), 8)
})

test('queued extraction is explicit, belongs to this run, and cannot override working steps', () => {
  const run = {run_id:'new',status:'running',steps:[{step:'save_games',status:'complete'}],
    step_catalog:[{step:'save_games'},{step:'extract_positions'}]}
  const queued = {fen:{status:{status:'queued',progress:{ingestion_run_id:'new',total:19047}}}}
  const extraction = pipelineSteps(run,queued)[1]
  assert.equal(extraction.status,'queued')
  assert.equal(extraction.total,19047)
  assert.equal(extraction.processed,0)
  assert.match(extraction.detail,/positions are not ready/)
  const working = {...run,steps:[...run.steps,{step:'extract_positions',status:'working',processed:100,total:200}]}
  assert.equal(pipelineSteps(working,queued)[1].processed,100)
  queued.fen.status.progress.ingestion_run_id='old'
  assert.equal(pipelineSteps(run,queued)[1].status,'pending')
  assert.equal(pipelineSteps({...run,status:'complete'},queued)[1].status,'skipped')
})
