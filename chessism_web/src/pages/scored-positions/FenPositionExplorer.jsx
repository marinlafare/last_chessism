import { lazy, Suspense, useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import PositionBoard from '../../components/chess/PositionBoard'
import CommonPositionPreview from './CommonPositionPreview'
import { fetchPositionExamples, fetchPositionGames } from './fenExplorerApi'
import { formatNumber } from '../../utils/formatters'
import { styleTarget } from '../../appearance/styleCore'
import './fenExplorer.css'

const GameReview=lazy(()=>import('../players/PlayerGameReviewModal'))

export default function FenPositionExplorer({scope,anchor,onClose}) {
  const [page,setPage]=useState(0),[shuffle,setShuffle]=useState(0),[list,setList]=useState({loading:true})
  const [position,setPosition]=useState(null),[hover,setHover]=useState(null),[review,setReview]=useState(null)
  const [games,setGames]=useState({loading:true}),[gamePage,setGamePage]=useState(0),[cursors,setCursors]=useState([0])
  const [viewport,setViewport]=useState({width:window.innerWidth,height:window.innerHeight})
  const cache=useRef(new Map()), token=useRef(null), panel=useRef(null), close=useRef(onClose)
  close.current=onClose
  useEffect(()=>{
    const update=()=>setViewport({width:window.innerWidth,height:window.innerHeight})
    window.addEventListener('resize',update)
    return()=>window.removeEventListener('resize',update)
  },[])
  useEffect(()=>{
    if(review)return undefined
    const previous=document.activeElement
    panel.current?.focus()
    const keys=e=>{
      if(e.key==='Escape'){e.stopPropagation();close.current()}
      if(e.key==='Tab'&&panel.current?.contains(document.activeElement)) {
        const nodes=[...panel.current.querySelectorAll('button:not(:disabled),[tabindex="0"]')]
        const first=nodes[0],last=nodes.at(-1)
        if(e.shiftKey&&document.activeElement===first){e.preventDefault();last?.focus()}
        if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first?.focus()}
      }
    }
    document.addEventListener('keydown',keys)
    return()=>{document.removeEventListener('keydown',keys);if(previous?.isConnected)previous.focus()}
  },[review])
  useEffect(()=>{
    setHover(null)
    if(cache.current.has(page)){setList(cache.current.get(page));return undefined}
    const controller=new AbortController()
    setList({loading:true})
    fetchPositionExamples({scope,token:token.current,page,signal:controller.signal}).then(data=>{
      if(controller.signal.aborted)return
      token.current=data.token;cache.current.set(page,data);setList(data)
    }).catch(error=>{if(!controller.signal.aborted)setList({error:error.message})})
    return()=>controller.abort()
  },[scope,page,shuffle])
  useEffect(()=>{
    if(!position)return undefined
    const controller=new AbortController()
    setGames({loading:true})
    fetchPositionGames({fen:position.fen,after:cursors[gamePage],signal:controller.signal}).then(data=>{
      if(!controller.signal.aborted)setGames(data)
    }).catch(error=>{if(!controller.signal.aborted)setGames({error:error.message})})
    return()=>controller.abort()
  },[position,gamePage,cursors])
  const choose=row=>{setHover(null);setPosition(row);setGamePage(0);setCursors([0])}
  const refresh=()=>{cache.current.clear();token.current=null;setPage(0);setShuffle(v=>v+1)}
  const openGame=(game,o)=>setReview({gameId:game.link,playerName:game.white,
    startingPosition:{fen:position.fen,n_move:o?.n_move,move_color:o?.move_color}})
  if(review)return <Suspense fallback={<p className="fen-explorer-loading" role="status">Opening game…</p>}><GameReview {...review} onBack={()=>setReview(null)} onClose={onClose}/></Suspense>
  const width=Math.min(position?620:420,viewport.width-16)
  const left=Math.max(8,Math.min(anchor.x,viewport.width-width-8)),top=Math.max(8,Math.min(anchor.y,viewport.height-460))
  const title=scope==='unique'?'Unique FENs':scope==='repeated'?'Non-unique FENs':`${scope} appearances`
  return createPortal(<>
    <section ref={panel} className="fen-explorer-popup" {...styleTarget('explorer.panel')} role="dialog" aria-label={title} tabIndex={-1} style={{left,top,width,maxHeight:viewport.height-top-8}}>
      <header><strong>{position?'Games containing this position':title}</strong><button type="button" onClick={onClose} aria-label="Close position explorer">×</button></header>
      {position?<>
        <button type="button" onClick={()=>setPosition(null)}>← Position list</button>
        <div className="fen-explorer-detail">
          <div><PositionBoard fen={position.fen}/><small>{position.label} · {formatNumber(position.appearances)} appearances</small></div>
          <div className="fen-explorer-games">
            {games.loading?<p role="status">Loading games…</p>:games.error?<p role="alert">{games.error}</p>:<>
              {!games.rows?.length&&<p>No matching games remain.</p>}
              {games.rows?.map(game=><article key={game.link}>
                <button type="button" onClick={()=>openGame(game,game.occurrences[0])}>{game.white} ({game.white_elo}) · {game.black} ({game.black_elo})</button>
                <small>{game.year}-{String(game.month).padStart(2,'0')}-{String(game.day).padStart(2,'0')} · White: {game.white_str_result} · {game.time_control}</small>
                <div>{game.occurrences.map(o=><button type="button" key={`${o.n_move}-${o.move_color}`} onClick={()=>openGame(game,o)}>Move {o.n_move}{o.move_color==='white'?'.':'...'}</button>)}</div>
              </article>)}
              {games.occurrences_truncated&&<p>Only the first 2,000 occurrences are displayed.</p>}
            </>}
          </div>
        </div>
        <nav><button type="button" disabled={!gamePage||games.loading} onClick={()=>setGamePage(p=>p-1)}>Previous</button><span>Games · page {gamePage+1}</span><button type="button" disabled={!games.next_after||games.loading} onClick={()=>{setCursors(v=>[...v.slice(0,gamePage+1),games.next_after]);setGamePage(p=>p+1)}}>Next 10</button></nav>
      </>:<>
        <small>Sampled examples—not the entire database. Hover to preview; click to explore games.</small>
        {list.loading?<p role="status">Finding examples…</p>:list.error?<p role="alert">{list.error}</p>:<div className="fen-example-list">
          {!list.rows?.length&&<p>No examples in this sample. Try Shuffle.</p>}
          {list.rows?.map((row,index)=><button type="button" key={row.fen} {...styleTarget('position.button')}
            onMouseEnter={e=>setHover({bar:row,rect:e.currentTarget.getBoundingClientRect()})} onMouseLeave={()=>setHover(null)}
            onFocus={e=>setHover({bar:row,rect:e.currentTarget.getBoundingClientRect()})} onBlur={()=>setHover(null)} onClick={()=>choose(row)}>
            <span>#{page*10+index+1} · {row.label}</span><span>{formatNumber(row.appearances)} appearances</span>
          </button>)}
        </div>}
        <nav><button type="button" disabled={!page||list.loading} onClick={()=>setPage(p=>p-1)}>Previous</button><span>{list.sample_size?`${page*10+1}–${page*10+(list.rows?.length||0)} / ${list.sample_size} sampled`:''}</span><button type="button" disabled={!list.has_next||list.loading} onClick={()=>setPage(p=>p+1)}>Next 10</button><button type="button" disabled={list.loading} onClick={refresh}>Shuffle</button></nav>
      </>}
    </section>
    <CommonPositionPreview selection={hover}/>
  </>,document.body)
}
