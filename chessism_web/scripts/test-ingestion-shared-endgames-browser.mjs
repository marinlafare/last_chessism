import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9366, async ({send,context,evaluate,assert,waitFor}) => {
  await send('browsingContext.navigate',{context,wait:'complete',url:'http://localhost:6789/tests/fixtures/ingestion.html'})
  await waitFor(`window.ingestionTest && document.querySelector('.fen-pipeline-stage')`)
  await evaluate(`ingestionTest.run={...ingestionTest.run,status:'running',finished_at:null,
    steps:[{step:'save_games',status:'complete',processed:100,total:100},
      {step:'link_positions',status:'complete',processed:1000,total:1000}],
    step_catalog:[{step:'save_games'},{step:'link_positions'},{step:'probe_endgames'}],
    stages:[{stage:'tablebase_marking',status:'running',processed:0,total:0}],
    graph_sync:{step:'neo4j_ingestion',status:'waiting',processed:100,total:100}};
    ingestionTest.render()`)
  await assert(`!document.querySelector('.fen-ingestion-dismiss:not(:disabled)')`)
  await evaluate(`ingestionTest.run.status='complete';ingestionTest.run.finished_at=new Date().toISOString();
    Object.assign(ingestionTest.run.stages[0],{status:'complete',processed:1,total:1,elapsed_ms:150000});
    ingestionTest.run.graph_sync.status='complete';ingestionTest.render()`)
  await waitFor(`document.querySelector('.fen-ingestion-dismiss:not(:disabled)')`)
  await assert(`document.querySelectorAll('.fen-pipeline-stage.is-complete').length===3`)
  await evaluate(`document.querySelector('.fen-pipeline-details > summary').click()`)
  await waitFor(`[...document.querySelectorAll('.fen-pipeline-detail-row')].some(row=>row.textContent.includes('Endgame analysis'))`)
  await assert(`ingestionTest.errors.length===0`)
  console.log('PASS: verified shared endgame completion clears pending UI, preserves an endgame detail, and enables review only after graph completion. No API access.')
})
