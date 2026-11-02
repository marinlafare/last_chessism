import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9352,async({send,context,evaluate,assert,waitFor})=>{
  await send('browsingContext.navigate',{context,wait:'complete',url:'http://localhost:6789/tests/fixtures/ingestion.html'})
  await waitFor(`window.ingestionTest && document.querySelector('.fen-pipeline-stage')`)
  await evaluate(`ingestionTest.run.status='complete';ingestionTest.run.finished_at='2026-10-31T02:00:00Z';
    ingestionTest.run.steps=ingestionTest.run.steps.map(step=>({...step,status:'complete'}));
    ingestionTest.run.graph_sync={step:'neo4j_ingestion',label:'Neo4j ingestion',group:'Graph',status:'working',
      total:4,processed:2,unit:'updates',elapsed_seconds:3,heartbeat_at:Date.now()/1000,detail:'2 / 4 games'};
    ingestionTest.render()`)
  await waitFor(`document.querySelector('[data-style-id="ingestion.neo4j.card"]')`)
  await assert(`document.querySelector('[data-style-id="ingestion.neo4j.card"]').classList.contains('is-working')`)
  await assert(`!document.querySelector('.fen-ingestion-dismiss')`)
  await assert(`document.querySelector('[data-style-id="ingestion.neo4j.track"]').getAttribute('aria-valuenow')==='2'`)
  await assert(`document.querySelector('[data-style-id="ingestion.neo4j.fill"]').style.width==='50%'`)
  await evaluate(`ingestionTest.run.graph_sync.status='unavailable';ingestionTest.run.graph_sync.total=null;
    ingestionTest.run.graph_sync.detail='Neo4j progress is temporarily unavailable; checking again.';ingestionTest.render()`)
  await waitFor(`document.querySelector('[data-style-id="ingestion.neo4j.card"]').classList.contains('is-unavailable')`)
  await assert(`!document.querySelector('.fen-ingestion-dismiss')`)
  await evaluate(`Object.assign(ingestionTest.run.graph_sync,{status:'working',processed:3,total:4,
    detail:'3 / 4 games',heartbeat_at:Date.now()/1000});ingestionTest.render()`)
  await waitFor(`document.querySelector('[data-style-id="ingestion.neo4j.fill"]')?.style.width==='75%'`)
  await assert(`document.querySelector('[data-style-id="ingestion.neo4j.card"]').textContent.includes('Worker active')`)
  await evaluate(`ingestionTest.run.graph_sync.status='retrying';ingestionTest.run.graph_sync.detail='Retrying temporary failure';ingestionTest.render()`)
  await waitFor(`document.querySelector('[data-style-id="ingestion.neo4j.card"]').classList.contains('is-retrying')`)
  await assert(`!document.querySelector('.fen-ingestion-dismiss')`)
  await evaluate(`ingestionTest.run.graph_sync.status='complete';ingestionTest.run.graph_sync.processed=4;ingestionTest.run.graph_sync.detail='4 / 4 games';ingestionTest.render()`)
  await waitFor(`document.querySelector('.fen-ingestion-dismiss')`)
  await assert(`document.querySelectorAll('[data-style-id="ingestion.neo4j.card"]').length===1`)
  await assert(`document.querySelector('[data-style-id="ingestion.neo4j.fill"]').style.width==='100%'`)
  await send('browsingContext.reload',{context,wait:'complete'})
  await waitFor(`document.querySelector('.fen-ingestion-dismiss')`)
  await assert(`document.querySelector('[data-style-id="ingestion.neo4j.card"]').textContent.includes('4 / 4 games')`)
  await assert(`ingestionTest.errors.length===0`)
  console.log('PASS: Neo4j stays live after PostgreSQL completion, recovers its progress bar after unavailable status, shows counts/heartbeat/retries, independent track/fill styles, retained result and reload. No API requests.')
})
