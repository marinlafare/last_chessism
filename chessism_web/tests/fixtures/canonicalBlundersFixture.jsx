import React, { useState } from 'react'
import { createRoot } from 'react-dom/client'
import Algorithms from '../../src/pages/Algorithms'
history.replaceState(null,'','/research/algorithms')
window.canonicalFixture={requests:[],phase:'building',offline:false,active:true,benchmark:false,finished:false}
const run=(id,status,name)=>({id,status,name,created_at:'2026-10-26T18:00:00Z',started_at:'2026-10-26T18:00:00Z',
  finished_at:status==='running'?null:'2026-10-26T18:05:00Z',progress:{phase:status,processed:10,total:100},error:null})
const active=run('active','running','Current research run')
const completed=run('complete','complete','Completed experiment')
const failed=run('failed','failed','Failed experiment')
window.fetch=async(input,options={})=>{
  const url=new URL(input,location.href),path=url.pathname
  if(options.method && options.method!=='GET') {
    if(options.method==='POST' && path.endsWith('/canonical-blunders/benchmark')) {
      window.canonicalFixture.benchmark=true
      return new Response(JSON.stringify({id:'timing',status:'queued'}),{headers:{'Content-Type':'application/json'}})
    }
    throw Error('Unexpected fixture mutation: '+path)
  }
  window.canonicalFixture.requests.push(path)
  const phase=window.canonicalFixture.phase
  let data
  if(path.endsWith('/canonical-blunders')) {
    if(window.canonicalFixture.offline) return new Response(JSON.stringify({detail:'Synthetic connection failure'}),{status:503})
    data={target_model:'model-fixture',active_model:phase==='ready'?'model-fixture':null,
    status:phase,pending_games:phase==='ready'?0:1000,progress:{processed_games:8192,cuda_batch_rows:2097152},
    error:phase==='retrying'?'Synthetic GPU interruption':null,
    evidence:{limitations:['Rapid did not pass development calibration/prediction selection.','Independent tests not run.']},
    config:{fits:[{mode:'bullet',a:.2087,b:2.8800}]}}
  } else if(path.endsWith('/catalog')) data={row_types:[]}
  else if(path.endsWith('/matrices') || path.endsWith('/defaults') || path.endsWith('/definitions')) data={definitions:[],has_more:false}
  else if(path.endsWith('/runs')) data=url.searchParams.get('offset')==='30'
    ? {runs:[run('older','failed','Older experiment')],has_more:false} : {runs:[
      ...(window.canonicalFixture.benchmark?[run('timing',window.canonicalFixture.finished?'complete':'running','Pipeline timing')]:[]),
      ...(window.canonicalFixture.active?[active]:[]),completed,failed],has_more:true}
  else if(path.endsWith('/runs/failed')) data={...failed,result:null,error:'Recorded experiment failure'}
  else if(path.endsWith('/runs/timing')) data={...run('timing','complete','Pipeline timing'),result:{
    format:'chessism_canonical_blunder_timing_v1',status:'passed',production_changed:false,scratch_removed:true,
    source:{games:100000,rows:6000000,input_sha256:'fixture-digest',reader_comparison:{
      status:'passed',same_snapshot:true,input_byte_agreement:true,legacy_seconds:76,binary_seconds:40,speedup:1.9}},hardware:{name:'NVIDIA GeForce RTX 4060'},
    model_id:'model-fixture',timings:{source_queries_seconds:20.5,sequential_stage_sum_seconds:42,run_wall_seconds:60},
    checks:{canonical_flags_counts:'passed',published_flags_counts:'passed',player_histograms:'passed'},notes:['Performance sample, not a calibration or quality test.']}}
  else throw Error('Unexpected fixture request: '+path)
  return new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}})
}
function App(){const [key,setKey]=useState(0);window.remountCanonical=()=>setKey(v=>v+1);return <Algorithms key={key}/>}
createRoot(document.getElementById('root')).render(<App/> )
