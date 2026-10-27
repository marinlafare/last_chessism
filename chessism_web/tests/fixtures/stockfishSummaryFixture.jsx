import React from 'react'
import { createRoot } from 'react-dom/client'
import PlayerMeasuresWorkspace from '/src/pages/players/PlayerMeasuresWorkspace.jsx'
import BoardSoundPreferences from '/src/components/chess/BoardSoundPreferences.jsx'
import '/src/index.css'

const state = window.summaryTest = { requests: [], unexpected: [], pending: true, stale: false, ready: false }
const timeout = window.setTimeout.bind(window)
window.setTimeout = (fn, delay, ...args) => timeout(fn, delay === 30000 ? 250 : delay, ...args)
const modes=['bullet','blitz','rapid']
window.fetch = async (input, options={}) => {
  const url=new URL(input,location.href), path=url.pathname
  state.requests.push(path+url.search)
  const reply=value=>new Response(JSON.stringify(value),{headers:{'Content-Type':'application/json'}})
  if ((options.method||'GET')!=='GET') throw Error('No writes permitted')
  if (path.endsWith('/readiness')) return reply({summary_pending:!state.ready,
    by_mode:Object.fromEntries(modes.map(mode=>[mode,{coverage:{eligible_games:state.ready?5:0}}]))})
  if (path.endsWith('/blunder-distribution')) return reply({bins:[],by_mode:{}})
  if (path.endsWith('/score-distribution') && !url.search) return reply({summary_managed:true,
    summary_pending:state.pending,stale:state.stale,snapshot:'fixture',
    bins:Array.from({length:13},(_,id)=>({id,label:String(id),lower:0,upper:100})),
    by_mode:Object.fromEntries(modes.map(mode=>[mode,{
      counts:Array.from({length:13},(_,id)=>id===6?3:0),endings:{games:3},cp_positions:3,
      mate_positions:mode==='blitz'?3:0,tablebase_positions:1,unscored_positions:2,
      mates:mode==='blitz'?[{moves:2,side:'for',positions:2},{moves:null,side:'against',positions:1}]:[]
    }]))})
  state.unexpected.push(path+url.search)
  throw Error('No live access: '+path)
}
createRoot(document.getElementById('root')).render(<BoardSoundPreferences accountId="stockfish-summary-fixture"><main style={{padding:24}}>
  <PlayerMeasuresWorkspace playerName="example" />
</main></BoardSoundPreferences>)
