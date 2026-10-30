import React from 'react'
import {createRoot} from 'react-dom/client'
import PlayerScorePositions from '/src/pages/players/PlayerScorePositions.jsx'
import BoardSoundPreferences from '/src/components/chess/BoardSoundPreferences.jsx'
import AppearanceProvider, { AppearanceControls } from '/src/appearance/AppearanceProvider.jsx'
import '/src/index.css'
import '/src/pages/players/player-score-distribution.css'
history.replaceState(null, '', '/')
window.samples={calls:[],gameCalls:[],reviews:[],errors:[]}
window.addEventListener('error',event=>samples.errors.push(event.message))
window.addEventListener('unhandledrejection',event=>samples.errors.push(String(event.reason)))
window.fetch=async(input,options)=>{
 const pathname=new URL(input,location.href).pathname
 if(window.scoreStylesRegistry && /\/appearance(?:\/save)?$/.test(pathname)) {
  const saved={revision:1,active_preset_id:'official',default_preset_id:'official',presets:[{id:'official',name:'Default'}],overrides:[],...JSON.parse(localStorage.getItem('score-styles-fixture')||'null')}
  if(options?.method==='POST'){saved.overrides=JSON.parse(options.body).overrides;saved.revision++;localStorage.setItem('score-styles-fixture',JSON.stringify(saved))}
  return new Response(JSON.stringify({...saved,registry:window.scoreStylesRegistry}),{headers:{'Content-Type':'application/json'}})
 }
 if(pathname.endsWith('/explore-games')) {
  const query=JSON.parse(options.body);samples.gameCalls.push(query)
  return new Response(JSON.stringify({summary:null,games:[{game_id:99,player_color:query.color,mode:'blitz',player_rating:1800,opponent_rating:1900,opponent_name:'bob',played_at_utc:'2026-10-30T12:00:00Z',result:'win'}],pagination:{has_more:false,next_cursor:null}}),{headers:{'Content-Type':'application/json'}})
 }
 if(pathname.endsWith('/games/99/score')) {
  samples.reviews.push(pathname)
  return new Response(JSON.stringify({game:{initial_fen:'start',white:{name:'alice'},black:{name:'bob'},mode:'blitz',time_control:'180'},moves:[{ply:1,move_number:1,move_color:'white',san:'Ke2',uci:'e1e2',fen_after:'8/8/8/8/8/8/4K3/7k w - - 0 1'}]}),{headers:{'Content-Type':'application/json'}})
 }
 if(!String(input).endsWith('/analysis/measures/score-distribution/positions'))throw Error('Unexpected live request: '+input)
 const query=JSON.parse(options.body);samples.calls.push(query)
 const row=(id,color='white',score=10)=>({fen_id:String(id),player_color:color,score_min:score,score_max:score,fen:'8/8/8/8/8/8/4K3/7k w - - 0 1'})
 if(query.color==='white' && samples.holdWhite)await new Promise(resolve=>{samples.releaseWhite=resolve})
 const data=query.color==='black'?{rows:[row(10,'black',-10),row(11,'black',-30)],next_cursor:null}
   :query.color==='white'?{rows:[row(20)],next_cursor:null}
   :query.cursor==='next'?{rows:[row(1),row(2)],next_cursor:'sparse'}
   :query.cursor==='sparse'?{rows:[],next_cursor:'last'}
   :query.cursor==='last'?{rows:[row(3)],next_cursor:null}:{rows:[row(1)],next_cursor:'next'}
 const payload=samples.ranked?{...data,rows:data.rows.map(row=>({...row,appearances:5})),sort:'appearances_desc'}
   :{...data,sort:'matching_examples'}
 return new Response(JSON.stringify({...payload,snapshot:'matching'}),{headers:{'Content-Type':'application/json'}})
}
const content=<BoardSoundPreferences accountId="score-samples">
 {window.scoreStylesRegistry?<><AppearanceControls/><h3 data-style-scope="home" data-style-id="title" id="outside-score-title">Unrelated title</h3></>:null}
 <PlayerScorePositions playerName="alice"
 selection={{bin:{id:6,lower:-50,upper:50,lower_inclusive:true,upper_inclusive:true},modes:['blitz'],snapshot:'chart'}}
 onClose={()=>{samples.closed=true}} /></BoardSoundPreferences>
createRoot(document.getElementById('root')).render(window.scoreStylesRegistry
 ? <AppearanceProvider account={{role:'admin'}} pageKey="/">{content}</AppearanceProvider> : content)
