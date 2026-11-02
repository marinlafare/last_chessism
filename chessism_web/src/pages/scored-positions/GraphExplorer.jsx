import { useCallback, useState } from 'react'
import { graphStyle as s } from './GraphBoard'
import GraphGames from './GraphGames'
import GraphSelection from './GraphSelection'
import OpeningMap from './OpeningMap'
import { useOpeningLayout } from './useOpeningLayout'
import { useGraphRead } from './useGraphRead'
import { slotIndex } from './openingMapModel'
import './graphExplorer.css'
import './openingMap.css'

const number = n => Number(n || 0).toLocaleString()
export default function GraphExplorer() {
  const [players,setPlayers]=useState({a:'lafareto',b:'hikaru'}),[inputs,setInputs]=useState(players)
  const [scope,setScope]=useState('all'),[color,setColor]=useState('all'),[refresh,setRefresh]=useState(false)
  const [path,setPath]=useState([]),[games,setGames]=useState(null)
  const request=useGraphRead('map',{...players,refresh}),snapshot=request.data,layout=useOpeningLayout(snapshot)
  const selected=path.at(-1)||'chess:initial'
  const filter={player:scope==='all'?'':scope==='b'?players.b:players.a,other:scope==='both'?players.b:'',color:scope==='all'?'all':color,max_ply:30}
  const changeScope=value=>{setScope(value);setPath([]);setGames(null)}
  const select=key=>{if(key!==selected)setPath(current=>[...(current.length?current:[selected]),key].slice(-100))}
  const closeGames=useCallback(()=>setGames(null),[])
  const slot=snapshot?slotIndex(snapshot,scope,color):0
  return <div {...s('explorer')} className="common-graph-explorer">
    <form {...s('toolbar')} className="common-graph-toolbar" onSubmit={event=>{
      event.preventDefault();setPlayers({a:inputs.a.trim().toLowerCase(),b:inputs.b.trim().toLowerCase()});setPath([]);setGames(null)
    }}>
      <label {...s('playerLabel')}>Player A<input {...s('playerInput')} value={inputs.a} required maxLength={64} pattern="[A-Za-z0-9_.-]+"
        onChange={e=>setInputs(v=>({...v,a:e.target.value}))} /></label>
      <label {...s('compareLabel')}>Player B<input {...s('compareInput')} value={inputs.b} required maxLength={64} pattern="[A-Za-z0-9_.-]+"
        onChange={e=>setInputs(v=>({...v,b:e.target.value}))} /></label>
      <button {...s('apply')} type="submit">Set players</button>
      <button {...s('mapRefresh')} type="button" disabled={request.loading} onClick={()=>{setRefresh(true);request.retry()}}>Refresh map</button>
    </form>
    <div {...s('mapScope')} className="opening-map-scope">
      {[['all','All imported games'],['a',players.a],['b',players.b],['both','Both players']].map(([value,label])=>
        <button {...s(value==='all'?'all':`mapScope.${value}`)} key={value} type="button" aria-pressed={scope===value} onClick={()=>changeScope(value)}>{label}</button>)}
      <label {...s('colorLabel')}>Player color<select {...s('colorInput')} disabled={scope==='all'} value={color}
        onChange={e=>{setColor(e.target.value);setPath([]);setGames(null)}}>
        <option value="all">Both colors</option><option value="white">White</option><option value="black">Black</option>
      </select></label>
    </div>
    <p {...s('coverage')}>{snapshot?`${number(snapshot.coverage.games[slot])} games in this map sample`:'Loading imported sample…'} · first 15 full moves · imported games only, not complete player histories.</p>
    {snapshot&&<p {...s('mapSample')}>{players.a}: {number(snapshot.coverage.games[1])} games · {players.b}: {number(snapshot.coverage.games[2])} games. Counts are sample-specific; shared games are counted once.</p>}
    {snapshot&&Object.values(snapshot.limits).some(v=>v===true)&&<p {...s('mapLimit')}>
      Overview is bounded: {snapshot.limits.games&&`first ${number(snapshot.coverage.game_limit)} imported games by game ID; `}
      {snapshot.limits.nodes&&`up to ${number(snapshot.limits.node_limit)} most-used positions; `}
      {snapshot.limits.edges&&`up to ${number(snapshot.limits.edge_limit)} most-used connections; `}details remain available through the board’s connections.
    </p>}
    {(request.loading||layout.message)&&<p {...s('status')} role="status">{request.loading?'Loading opening connections…':layout.message}</p>}
    {(request.error||layout.error)&&<p {...s('error')} role="alert">{request.error||layout.error} <button {...s('retry')} type="button" onClick={request.error?request.retry:layout.retry}>Retry</button></p>}
    {snapshot&&layout.layout&&<div className="opening-map-layout">
      <OpeningMap key={snapshot.revision} snapshot={snapshot} layout={layout.layout} scope={scope} color={color} onSelect={select} />
      {snapshot.coverage.games[slot]>0&&<GraphSelection key={`${JSON.stringify(filter)}:${selected}`} filter={filter} selected={selected} onSelect={select}
        canBack={path.length>1} onBack={()=>setPath(current=>current.slice(0,-1))} onReset={()=>setPath([])}
        onGames={(position,moveKey,move)=>setGames({key:position.key,moveKey,move})} />}
    </div>}
    {games&&<GraphGames selection={games} filter={filter} onClose={closeGames} />}
  </div>
}
