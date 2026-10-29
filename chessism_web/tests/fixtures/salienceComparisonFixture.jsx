import React from 'react'
import { createRoot } from 'react-dom/client'
import { Chess } from 'chess.js'
import { SalienceComparisonContent } from '/src/pages/SalienceComparison.jsx'
import BoardSoundPreferences from '/src/components/chess/BoardSoundPreferences.jsx'
import '/src/index.css'

history.replaceState(null,'','/research/salience-comparison')
const state=window.salienceComparisonTest={requests:[],errors:[],reviews:{}}
window.addEventListener('error',event=>state.errors.push(event.message))
window.addEventListener('unhandledrejection',event=>state.errors.push(String(event.reason)))
window.Audio=class{load(){} pause(){} removeAttribute(){} play(){return Promise.resolve()}}
const board=new Chess(),initial=board.fen(),moves=[]
for(let i=0;i<32;i++){
  const before=board.fen(),move=board.move(['Nf3','Nf6','Ng1','Ng8'][i%4])
  moves.push({ply:i+1,move_number:Math.floor(i/2)+1,move_color:i%2?'black':'white',san:move.san,
    uci:move.from+move.to,fen_before:before,fen_after:board.fen(),evaluation_after:null})
}
const games=Array.from({length:30},(_,i)=>({game_link:i+1,is_white:1,played_at:`2026-10-${String(i+1).padStart(2,'0')}T12:00:00Z`,
  mode:i%2?'bullet':'blitz',result:1,opponent:`Opponent${i+1}`,positions:32,current:.9,equal:.8,opening:.5,delta_equal:-.1,delta_opening:-.4,difference:.4}))
window.fetch=async(input,options={})=>{
  const url=new URL(input,location.href),path=url.pathname,method=options.method||'GET'
  state.requests.push({path,method})
  const reply=body=>new Response(JSON.stringify(body),{headers:{'Content-Type':'application/json'}})
  if(path.endsWith('/salience-comparisons'))return reply({comparisons:[{id:'test',player:'lafareto',games:30,positions:960,short_games:0,
    created_at:'2026-10-29T12:00:00Z',baseline_check:{compared_games:30,max_absolute_difference:1e-15},
    variants:['current','equal','opening'].map((id,i)=>({id,label:['Current formula','Whole game · equal weights','First 15 moves · equal weights'][i],
      median:[.9,.8,.5][i],mean:[.9,.8,.5][i],at_least_075:i<2?30:0,histogram:Array.from({length:10},(_,j)=>j===9-i*2?30:0)}))}]})
  const match=path.match(/\/salience-comparisons\/test\/games\/(\d+)(\/review)?$/)
  if(match){
    const id=Number(match[1])
    if(match[2]&&method==='POST'){state.reviews[id]=JSON.parse(options.body);return reply({saved:true})}
    if(method!=='GET')throw Error('Unexpected mutation '+path)
    return reply({game:{initial_fen:initial,played_at_utc:'2026-10-29T12:00:00Z',mode:'blitz',time_control:'180',
      white:{name:'lafareto',rating:1500},black:{name:'Opponent',rating:1500},result:{white:1,black:0}},
      moves,player_summaries:[],comparison:{scores:{current:.9,equal:.8,opening:.5},review:state.reviews[id],positions:moves.map((m,i)=>({
        ply:i+1,occurrence:Math.floor(i/4)+1,games_with_position:12,opening_games_with_position:10,in_window:i<30,
        depth_weight:Math.min(1,.25+.75*(i+1)/16),current_contribution:.01,equal_contribution:.01,opening_contribution:i<30?.02:0}))}})
  }
  if(path.endsWith('/test/games')){
    const mode=url.searchParams.get('mode'),offset=Number(url.searchParams.get('offset')||0),limit=Number(url.searchParams.get('limit')||25)
    let chosen=games.filter(g=>mode==='all'||g.mode===mode)
    if(url.searchParams.get('sort')==='date')chosen=[...chosen].sort((a,b)=>url.searchParams.get('direction')==='asc'?a.game_link-b.game_link:b.game_link-a.game_link)
    return reply({games:chosen.slice(offset,offset+limit).map(g=>({...g,review:state.reviews[g.game_link]})),total:chosen.length,offset,limit})
  }
  throw Error('No live access permitted: '+path)
}
createRoot(document.getElementById('root')).render(<BoardSoundPreferences accountId="salience-test"><SalienceComparisonContent /></BoardSoundPreferences>)
