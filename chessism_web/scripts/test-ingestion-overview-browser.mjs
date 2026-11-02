import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9365,async({send,context,evaluate,assert,waitFor})=>{
  await send('browsingContext.navigate',{context,wait:'complete',url:'http://localhost:6789/tests/fixtures/ingestion.html'})
  await waitFor(`window.ingestionTest && document.querySelector('.fen-pipeline-stage')`)
  await evaluate(`ingestionTest.run.steps=[
    ...['download_archives','filter_games','parse_games','save_games'].map(step=>({step,label:step,status:'complete',processed:500,total:500,unit:'games'})),
    ...['extract_positions','combine_positions','save_positions','link_positions','appearance_counts','coverage','ingestion_summaries',
      'count_endgames','discover_endgames','probe_endgames','claim_endgames','save_endgames','endgame_summaries','rating_projection','queue_player_summaries']
      .map(step=>({step,label:step,status:step==='probe_endgames'?'working':'complete',processed:50,total:100,unit:'positions',heartbeat_at:Date.now()/1000})),
    ];ingestionTest.run.graph_sync={step:'neo4j_ingestion',label:'Neo4j ingestion',status:'working',processed:20,total:100,
      unit:'updates',heartbeat_at:Date.now()/1000,detail:'20 / 100 games'};ingestionTest.render()`)
  await waitFor(`document.querySelectorAll('[role=progressbar]').length===3`)
  await assert(`document.querySelectorAll('.fen-pipeline-stage').length===3 && !document.querySelector('.fen-pipeline-detail-row')`)
  await assert(`document.querySelector('[data-ingestion-stage=positions]').textContent.includes('probe_endgames')`)
  await evaluate(`document.querySelector('.fen-pipeline-details > summary').click()`)
  await waitFor(`document.querySelectorAll('.fen-pipeline-detail-row').length===20`)
  await assert(`document.querySelectorAll('[role=progressbar]').length===3`)
  await evaluate(`document.querySelector('.fen-pipeline-details > summary').click();
    ingestionTest.run.steps.find(s=>s.step==='probe_endgames').status='failed';ingestionTest.render()`)
  await waitFor(`document.querySelector('[data-ingestion-stage=positions].is-failed')`)
  await evaluate(`ingestionTest.run.steps.forEach(s=>s.status='complete');ingestionTest.run.status='complete';ingestionTest.run.finished_at=new Date().toISOString();ingestionTest.render()`)
  await waitFor(`document.querySelector('[data-ingestion-stage=positions].is-complete')`)
  await assert(`!document.querySelector('.fen-ingestion-dismiss') && document.querySelector('[data-ingestion-stage=neo4j].is-working')`)
  await evaluate(`ingestionTest.run.graph_sync.status='complete';ingestionTest.run.graph_sync.processed=100;ingestionTest.render()`)
  await waitFor(`document.querySelector('.fen-ingestion-dismiss:not(:disabled)')`)
  await assert(`document.querySelectorAll('[role=progressbar]').length===3`)
  await evaluate(`ingestionTest.run={...ingestionTest.run,run_id:'next-player',player_name:'next',status:'running',finished_at:null,
    graph_sync:{...ingestionTest.run.graph_sync,status:'working',processed:5}};ingestionTest.render()`)
  await waitFor(`document.querySelector('.fen-ingestion-previous')`)
  await assert(`document.querySelectorAll('[role=progressbar]').length===3 && !document.querySelector('.fen-ingestion-result')`)
  await evaluate(`document.querySelector('.fen-ingestion-previous > summary').click()`)
  await waitFor(`document.querySelector('.fen-ingestion-result')`)
  await assert(`document.querySelectorAll('[role=progressbar]').length===3 && document.querySelector('.fen-ingestion-dismiss:disabled')`)
  await assert(`['games','positions','neo4j'].every(stage=>['card','title','track','fill','value'].every(part=>
    document.querySelector('[data-style-id="ingestion.'+stage+'.'+part+'"]')))`)
  await assert(`document.documentElement.scrollWidth<=innerWidth && ingestionTest.errors.length===0`)
  console.log('PASS: 20 steps become three bars; Details adds no bars; failures stay visible; graph completion gates dismissal; previous results stay collapsed; independent style targets.')
})
