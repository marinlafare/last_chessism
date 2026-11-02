import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9363, async ({send,context,evaluate,assert,waitFor}) => {
  await send('browsingContext.navigate',{context,wait:'complete',url:'http://localhost:6789/tests/fixtures/ingestion.html'})
  await waitFor(`window.ingestionTest && document.querySelector('.fen-pipeline-stage')`)
  await evaluate(`ingestionTest.run.status='complete';ingestionTest.run.finished_at='2026-11-01T12:00:00Z';
    ingestionTest.run.steps.forEach(step=>step.status='complete');ingestionTest.render()`)
  await waitFor(`document.querySelector('.fen-ingestion-dismiss:not(:disabled)')`)
  await evaluate(`ingestionTest.run={...ingestionTest.run,run_id:'new-parhamov',player_name:'parhamov',
    status:'running',finished_at:null,steps:ingestionTest.run.steps.map(step=>({...step,status:'working'}))};ingestionTest.render()`)
  await waitFor(`document.querySelector('.fen-pipeline-stage.is-working') && document.querySelector('.fen-ingestion-previous')`)
  await assert(`!document.querySelector('.fen-ingestion-previous[open]') && !document.querySelector('.fen-ingestion-dismiss')`)
  await evaluate(`document.querySelector('.fen-ingestion-previous > summary').click()`)
  await waitFor(`document.querySelector('.fen-ingestion-dismiss:disabled')`)
  await assert(`document.querySelector('.fen-ingestion-dismiss').title.includes('sample')`)
  await assert(`Number(getComputedStyle(document.querySelector('.fen-ingestion-dismiss')).opacity)<1`)
  await evaluate(`document.querySelector('.fen-ingestion-dismiss').click()`)
  await assert(`document.querySelectorAll('.fen-ingestion-result').length===1`)
  await evaluate(`ingestionTest.run.status='complete';ingestionTest.run.finished_at='2026-11-01T13:00:00Z';
    ingestionTest.run.steps.forEach(step=>step.status='complete');
    ingestionTest.run.graph_sync={step:'neo4j_ingestion',label:'Neo4j ingestion',group:'Graph',status:'working',
      processed:2,total:4,unit:'updates',elapsed_seconds:3};ingestionTest.render()`)
  await waitFor(`document.querySelector('[data-style-id="ingestion.neo4j.card"].is-working')`)
  await assert(`!document.querySelector('.fen-ingestion-dismiss:not(:disabled)') && document.querySelectorAll('.fen-ingestion-result').length===1`)
  await evaluate(`ingestionTest.run.graph_sync.status='retrying';ingestionTest.render()`)
  await waitFor(`document.querySelector('[data-style-id="ingestion.neo4j.card"].is-retrying')`)
  await assert(`!document.querySelector('.fen-ingestion-dismiss:not(:disabled)')`)
  await evaluate(`ingestionTest.run.graph_sync.status='complete';ingestionTest.run.graph_sync.processed=4;ingestionTest.render()`)
  await waitFor(`document.querySelectorAll('.fen-ingestion-dismiss:not(:disabled)').length===2`)
  // Job discovery can lead timing polling when a new extraction starts.
  await evaluate(`ingestionTest.jobs={fen:{jobId:'queued-next',status:{status:'queued'}}};ingestionTest.render()`)
  await waitFor(`document.querySelectorAll('.fen-ingestion-dismiss:disabled').length===2`)
  await evaluate(`ingestionTest.jobs={};ingestionTest.timingError='Status unavailable';ingestionTest.render()`)
  await assert(`!document.querySelector('.fen-ingestion-dismiss:not(:disabled)')`)
  await evaluate(`ingestionTest.timingError='';ingestionTest.jobError='Job status unavailable';ingestionTest.render()`)
  await assert(`!document.querySelector('.fen-ingestion-dismiss:not(:disabled)')`)
  await evaluate(`ingestionTest.jobError='';ingestionTest.render()`)
  await waitFor(`document.querySelectorAll('.fen-ingestion-dismiss:not(:disabled)').length===2`)
  await evaluate(`[...document.querySelectorAll('.fen-ingestion-result')].find(row=>row.textContent.includes('parhamov')).querySelector('button').click()`)
  await waitFor(`document.querySelectorAll('.fen-ingestion-result').length===1`)
  await assert(`document.querySelector('.fen-ingestion-result').textContent.includes('sample') && ingestionTest.errors.length===0`)
  console.log('PASS: old results cannot be dismissed while a new ingestion, queued extraction, graph sync/retry, or uncertain status is active; review re-enables after completion without dropping retained history. No API access.')
})
