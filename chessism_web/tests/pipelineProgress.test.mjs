import test from 'node:test'
import assert from 'node:assert/strict'
import { pipelineSteps, compactPipelineSteps, stepEstimate, graphIngestionActive } from '../src/pages/positions/pipelineProgress.js'

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

test('verified shared endgame completion remains visible without inventing detailed timings', () => {
  const run = {status:'complete',steps:[{step:'link_positions',status:'complete',processed:10,total:10}],
    step_catalog:[{step:'link_positions'},{step:'probe_endgames'}],
    stages:[{stage:'tablebase_marking',status:'complete',processed:1,total:1,elapsed_ms:150000}]}
  const rows=pipelineSteps(run)
  assert.equal(rows.find(row=>row.step==='tablebase_marking').status,'complete')
  assert.equal(compactPipelineSteps(run)[1].status,'complete')
  run.stages[0].status='running'
  assert.equal(pipelineSteps(run).some(row=>row.step==='tablebase_marking'),false)
})

test('Neo4j is appended without changing completed ingestion stages or using global counts', () => {
  const graph={step:'neo4j_ingestion',status:'working',processed:25,total:50,unit:'updates'}
  for(const run of [{steps:[{step:'save_games',status:'complete'}]}, {stages:[]}]) {
    const original=structuredClone(run)
    const rows=pipelineSteps({...run,status:'complete',graph_sync:graph})
    assert.deepEqual(rows.at(-1),graph)
    assert.deepEqual(run,original)
  }
  assert.equal(graphIngestionActive({status:'complete',graph_sync:graph}),true)
  assert.equal(graphIngestionActive({graph_sync:{...graph,status:'retrying'}}),true)
  assert.equal(graphIngestionActive({graph_sync:{...graph,status:'complete'}}),false)
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

test('compact ingestion has three stages, using actual current-step counters', () => {
  const run = {status:'running',steps:[
    {step:'save_games',label:'Save games',status:'complete',processed:500,total:500,unit:'games'},
    {step:'extract_positions',status:'waiting',processed:200,total:500,unit:'games'},
    {step:'save_positions',label:'Save unique positions',status:'working',processed:800,total:1000,unit:'positions'},
    {step:'probe_endgames',status:'pending'},
  ],graph_sync:{step:'neo4j_ingestion',status:'working',processed:25,total:100,unit:'updates'}}
  const before=structuredClone(run)
  const rows=compactPipelineSteps(run)
  assert.deepEqual(rows.map(row=>row.label),['Games','Positions','Neo4j'])
  assert.equal(rows[0].status,'complete')
  assert.equal(rows[1].currentStep,'save_positions')
  assert.equal(rows[1].processed,800)
  assert.equal(rows[1].total,1000)
  assert.equal(rows[1].completedSteps,0)
  assert.equal(rows[2].processed,25)
  assert.deepEqual(run,before)
})

test('endgames/finalization share Positions and do not inherit a completed extraction', () => {
  const run={status:'running',steps:[
    {step:'extract_positions',status:'complete',processed:100,total:100},
    {step:'probe_endgames',label:'Solve endgames',status:'working',processed:50,total:200},
    {step:'rating_projection',status:'pending'},
  ]}
  assert.equal(compactPipelineSteps(run)[1].currentStep,'probe_endgames')
  run.steps[1].status='complete';run.steps[2].status='working';run.steps[2].total=null
  assert.equal(compactPipelineSteps(run)[1].total,null)
  assert.equal(compactPipelineSteps(run)[1].status,'working')
})

test('compact statuses retain errors, unknown totals and graph completion independently', () => {
  const run={status:'failed',steps:[{step:'save_positions',status:'working',processed:10,total:null}],
    graph_sync:{step:'neo4j_ingestion',status:'unavailable',processed:0,total:null}}
  const rows=compactPipelineSteps(run)
  assert.equal(rows[1].status,'failed')
  assert.equal(rows[1].total,null)
  assert.equal(rows[2].status,'unavailable')
  run.status='complete';run.steps[0].status='complete';run.graph_sync.status='working'
  assert.equal(compactPipelineSteps(run)[2].status,'working')
  assert.deepEqual(compactPipelineSteps(null),[])
})
