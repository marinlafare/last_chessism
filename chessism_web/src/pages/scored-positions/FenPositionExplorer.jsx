import { useEffect, useId, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import PositionBoard from '../../components/chess/PositionBoard'
import { fetchPositionExamples } from './fenExplorerApi'
import { styleTarget } from '../../appearance/styleCore'
import './fenExplorer.css'

export default function FenPositionExplorer({scope,anchor,onClose}) {
  const [page,setPage]=useState(0),[shuffle,setShuffle]=useState(0),[list,setList]=useState({loading:true})
  const [hover,setHover]=useState(null)
  const [viewport,setViewport]=useState({width:window.innerWidth,height:window.innerHeight})
  const cache=useRef(new Map()), token=useRef(null), panel=useRef(null), close=useRef(onClose)
  const previewId=useId()
  close.current=onClose
  useEffect(()=>{
    const update=()=>{setHover(null);setViewport({width:window.innerWidth,height:window.innerHeight})}
    window.addEventListener('resize',update)
    return()=>window.removeEventListener('resize',update)
  },[])
  useEffect(()=>{
    const previous=document.activeElement
    panel.current?.focus({preventScroll:true})
    const keys=e=>{
      if(e.target.closest?.('[data-appearance-tools]'))return
      if(e.key==='Escape'){e.stopPropagation();close.current()}
      if(e.key==='Tab'&&panel.current?.contains(document.activeElement)) {
        const nodes=[...panel.current.querySelectorAll('button:not(:disabled),[tabindex="0"]')]
        const first=nodes[0],last=nodes.at(-1)
        if(e.shiftKey&&(document.activeElement===first||document.activeElement===panel.current)){e.preventDefault();last?.focus()}
        if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first?.focus()}
      }
    }
    document.addEventListener('keydown',keys)
    return()=>{document.removeEventListener('keydown',keys);if(previous?.isConnected)previous.focus({preventScroll:true})}
  },[])
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
  const refresh=()=>{cache.current.clear();token.current=null;setPage(0);setShuffle(v=>v+1)}
  const width=Math.min(280,viewport.width-16)
  const left=Math.max(8,Math.min(anchor.x,viewport.width-width-8)),top=Math.max(8,Math.min(anchor.y,viewport.height-460))
  const title=({unique:'Unique positions',repeated:'Non-unique positions',scored:'Scored positions',unscored:'Unscored positions'})[scope]||`${scope} appearances`
  const showPreview=(row,event)=>setHover({fen:row.fen,rect:event.currentTarget.getBoundingClientRect()})
  const previewWidth=Math.min(224,viewport.width-40,viewport.height-40)
  const previewLeft=Math.max(8,Math.min(left+width+previewWidth+34<=viewport.width?left+width+10:left-previewWidth-34,viewport.width-previewWidth-32))
  return createPortal(<>
    <section ref={panel} className="fen-explorer-popup" {...styleTarget('explorer.panel')} role="dialog" aria-label={title} tabIndex={-1} style={{left,top,width,maxHeight:Math.min(520,viewport.height-top-8)}}>
      <header><strong>{title}</strong><button type="button" onClick={onClose} aria-label="Close position explorer">×</button></header>
      <small>Sampled positions. Hover or focus to enlarge.</small>
      <div className="fen-explorer-body" onScroll={()=>setHover(null)}>
        {list.loading?<p role="status">Finding examples…</p>:list.error?<p role="alert">{list.error}</p>:<>
          {!list.rows?.length?<p>No examples in this sample. Try Shuffle.</p>:<ul className="fen-example-list">
            {list.rows.map(row=><li key={row.fen} {...styleTarget('position.button')} className="fen-example-thumbnail" tabIndex={0}
              aria-label={`Chess position: ${row.fen}`} aria-describedby={hover?.fen===row.fen?previewId:undefined}
              onMouseEnter={event=>showPreview(row,event)} onMouseLeave={()=>setHover(null)}
              onFocus={event=>showPreview(row,event)} onBlur={()=>setHover(null)}>
              <div className="fen-example-board" aria-hidden="true"><PositionBoard fen={row.fen} width={72} showNotation={false}/></div>
            </li>)}
          </ul>}
        </>}
      </div>
      <nav aria-label="Position sample pages"><button type="button" disabled={!page||list.loading} onClick={()=>setPage(p=>p-1)}>Previous</button><span>{list.sample_size?`${page*10+1}–${page*10+(list.rows?.length||0)} / ${list.sample_size} sampled`:''}</span><button type="button" disabled={!list.has_next||list.loading} onClick={()=>setPage(p=>p+1)}>Next 10</button><button type="button" disabled={list.loading} onClick={refresh}>Shuffle</button></nav>
    </section>
    {hover&&<aside id={previewId} role="tooltip" aria-label="Enlarged chess position" className="fen-thumbnail-preview"
      style={{left:previewLeft,top:Math.max(8,Math.min(hover.rect.top,viewport.height-previewWidth-32))}}>
      <PositionBoard fen={hover.fen} width={previewWidth}/>
    </aside>}
  </>,document.body)
}
