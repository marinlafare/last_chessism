import React, {useLayoutEffect,useState} from 'react'
import {createRoot} from 'react-dom/client'
import PlayerGameExplorerModal from '/src/pages/players/PlayerGameExplorerModal.jsx'
import '/src/index.css'
import '/src/pages/players/player-game-explorer.css'

const state=window.explorerLoading={requests:[],frames:[],errors:[]}
window.addEventListener('error',event=>state.errors.push(event.message))
window.addEventListener('unhandledrejection',event=>state.errors.push(String(event.reason)))
window.fetch=(input,options)=>{
 if(!String(input).endsWith('/analysis/measures/explore-games'))throw Error('Unexpected live request: '+input)
 // Intentionally ignore aborts here: the component must reject late responses.
 return new Promise(resolve=>state.requests.push({input,body:JSON.parse(options.body),resolve}))
}
state.finish=(index,kind='games')=>{
 const request=state.requests[index]
 const date=request.body.date
 const data={summary:{games:1,analyzed_games:0,accuracy:null,wins:1,draws:0,losses:0},
  games:kind==='empty'?[]:[{game_id:index+1,player_color:'white',played_at_local:date+'T12:00:00Z',
   result:'win',opponent_name:'Result '+index,player_rating:2000}],pagination:{has_more:false,next_cursor:null}}
 request.resolve(new Response(JSON.stringify(kind==='error'?{detail:'Failure '+index}:data),
  {status:kind==='error'?503:200,headers:{'Content-Type':'application/json'}}))
}
function Fixture(){
 const [source,setSource]=useState({playerName:'alice',selection:null})
 state.show=(date,playerName='alice')=>setSource({playerName,
  selection:date?{scope:'date',date,label:date,modes:['blitz'],analyzed_only:false}:null})
 useLayoutEffect(()=>{
  const modal=document.querySelector('.game-explorer-modal')
  const box=modal?.getBoundingClientRect()
  state.frames.push({date:source.selection?.date,player:source.playerName,text:modal?.textContent,
   busy:modal?.getAttribute('aria-busy'),height:box?.height,top:box?.top})
 },[source])
 return <PlayerGameExplorerModal {...source} onClose={()=>state.show(null)} onOpenGame={()=>{}} />
}
createRoot(document.getElementById('root')).render(<Fixture />)
