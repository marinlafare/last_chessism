import React from 'react'
import {createRoot} from 'react-dom/client'
import PlayerScorePositions from '/src/pages/players/PlayerScorePositions.jsx'
import '/src/pages/players/player-score-distribution.css'
window.samples={calls:[],errors:[]}
window.addEventListener('error',event=>samples.errors.push(event.message))
window.fetch=async(input,options)=>{
 if(!String(input).endsWith('/analysis/measures/score-distribution/positions'))throw Error('Unexpected live request: '+input)
 const query=JSON.parse(options.body);samples.calls.push(query)
 const row=id=>({fen_id:String(id),score_min:10,score_max:10,fen:'8/8/8/8/8/8/4K3/7k w - - 0 1'})
 const data=query.cursor==='next'?{rows:[row(1),row(2)],next_cursor:'sparse'}
   :query.cursor==='sparse'?{rows:[],next_cursor:'last'}
   :query.cursor==='last'?{rows:[row(3)],next_cursor:null}:{rows:[row(1)],next_cursor:'next'}
 return new Response(JSON.stringify({...data,snapshot:'matching',sort:'matching_examples'}),{headers:{'Content-Type':'application/json'}})
}
createRoot(document.getElementById('root')).render(<PlayerScorePositions playerName="alice"
 selection={{bin:{id:6,lower:-50,upper:50,lower_inclusive:true,upper_inclusive:true},modes:['blitz'],snapshot:'chart'}}
 onClose={()=>{samples.closed=true}} />)
