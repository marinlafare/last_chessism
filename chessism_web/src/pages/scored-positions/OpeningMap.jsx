import { useEffect, useMemo, useRef, useState } from 'react'
import { useAppearance } from '../../appearance/AppearanceProvider'
import { graphStyle as s } from './GraphBoard'
import OpeningMapCanvas from './OpeningMapCanvas'
import { mapView, PALETTE } from './openingMapModel'

export default function OpeningMap({ snapshot, layout, scope, color, onSelect }) {
  const [minimum,setMinimum]=useState(2), [community,setCommunity]=useState(null), [colorBy,setColorBy]=useState('communities')
  const [active,setActive]=useState(null),[offset,setOffset]=useState(0),[reset,setReset]=useState(0)
  const paletteRef=useRef(null),appearance=useAppearance(), [palette,setPalette]=useState(PALETTE)
  const [colors,setColors]=useState({edge:'#577277',label:'#c4d3d6'})
  useEffect(()=>{
    const frame=requestAnimationFrame(()=>{
      const value={...PALETTE}
      paletteRef.current?.querySelectorAll('[data-palette]').forEach(node=>{value[node.dataset.palette]=getComputedStyle(node).backgroundColor})
      setPalette(value)
      const edge=paletteRef.current?.querySelector('[data-map-edge]'),label=paletteRef.current?.querySelector('[data-map-label]')
      setColors({edge:edge?getComputedStyle(edge).color:'#577277',label:label?getComputedStyle(label).color:'#c4d3d6'})
    })
    return ()=>cancelAnimationFrame(frame)
  },[appearance?.draft,appearance?.saved])
  const view=useMemo(()=>mapView(snapshot,layout,{scope,color,minimum,community,colorBy},palette),[snapshot,layout,scope,color,minimum,community,colorBy,palette])
  useEffect(()=>{setOffset(0);setActive(null)},[scope,color,minimum,community])
  const activate=key=>{
    if(key.startsWith('group:')){setCommunity(key);const first=view.nodes.find(n=>n.key===key)?.members.find(k=>k!=='chess:initial');if(first)onSelect(first)}
    else onSelect(key)
  }
  const selected=view.nodes.find(n=>n.key===active), page=view.nodes.slice(offset,offset+8)
  const editNode=(key,event)=>{
    if(!appearance?.enabled||!appearance?.admin)return
    const node=view.nodes.find(n=>n.key===key),target=paletteRef.current?.querySelector(`[data-palette="${node?.colorKey}"]`)
    if(target){event.preventDefault();event.stopPropagation();appearance.select(target,event.clientX,event.clientY)}
  }
  return <section {...s('mapPanel')} className="opening-map-panel">
    <div {...s('mapControls')} className="opening-map-controls">
      <button {...s('mapOverview')} type="button" onClick={()=>setCommunity(null)} disabled={community===null}>Overview</button>
      <button {...s('mapReset')} type="button" onClick={()=>setReset(n=>n+1)}>Fit map</button>
      <label {...s('mapMinimumLabel')}>Minimum games<input {...s('mapMinimum')} type="number" min="1" max="10000" value={minimum}
        onChange={e=>setMinimum(Math.max(1,Math.min(10000,Number(e.target.value)||1)))} /></label>
      <label {...s('mapColorLabel')}>Colors<select {...s('mapColors')} value={colorBy} onChange={e=>setColorBy(e.target.value)}>
        <option value="communities">Connected groups</option><option value="players">Player comparison</option></select></label>
    </div>
    <p {...s('mapHint')}>{community===null?'Click a group to explore its positions.':'Click a position to show its board and games.'} Drag to pan · scroll to zoom.</p>
    <OpeningMapCanvas view={view} onActivate={activate} onHover={setActive} onStyleNode={editNode} active={active} reset={reset} edgeColor={colors.edge} labelColor={colors.label} />
    <div {...s('mapHover')} className="opening-map-hover" aria-live="polite">{selected ? `${selected.label} · ${selected.description}` : `${view.nodes.length.toLocaleString()} ${community===null?'groups':'positions'} shown`}</div>
    <div {...s('mapLegend')} className="opening-map-legend" ref={paletteRef}>
      {Object.entries(PALETTE).map(([key,value])=><span key={key} hidden={colorBy==='players'?key.startsWith('community'):!key.startsWith('community')}>
        <i {...s(`mapPalette.${key}`)} data-palette={key} style={{backgroundColor:undefined,'--palette-default':value}} />
        {key==='a'?snapshot.players[0]:key==='b'?snapshot.players[1]:key==='both'?'Both samples':key==='other'?'Other players':key==='root'?'Start':''}
      </span>)}
      <span {...s('mapEdge')} data-map-edge>Connections</span><span {...s('mapLabel')} data-map-label>Labels</span>
    </div>
    <p {...s('mapExplanation')}>{community===null?'Node size: number of positions in the group.':'Node size: distinct games reaching the position.'} Line thickness: move appearances. Groups are detected from connections, not official opening names.</p>
    <div {...s('mapNodeList')} className="opening-map-list" role="group" aria-label={community===null?'Opening groups':'Map positions'} onKeyDown={event=>{
      if(!['ArrowDown','ArrowUp','Home','End'].includes(event.key)||!page.length)return
      event.preventDefault()
      const i=page.findIndex(n=>n.key===active),next=event.key==='Home'?0:event.key==='End'?page.length-1:Math.max(0,Math.min(page.length-1,i+(event.key==='ArrowDown'?1:-1)))
      setActive(page[next].key);event.currentTarget.querySelectorAll('button')[next]?.focus({preventScroll:true})
    }}>{page.map(node=><button {...s('mapNodeRow')} key={node.key} type="button" data-map-node={node.key} data-active={active===node.key}
      onFocus={()=>setActive(node.key)} onPointerMove={()=>setActive(node.key)} onClick={()=>activate(node.key)}>
      <span {...s('mapNodeName')}>{node.label}</span><small {...s('mapNodeDescription')}>{node.description}</small>
    </button>)}</div>
    <div className="common-graph-page"><button {...s('mapPrevious')} type="button" disabled={!offset} onClick={()=>setOffset(n=>n-8)}>Previous</button>
      <button {...s('mapNext')} type="button" disabled={offset+8>=view.nodes.length} onClick={()=>setOffset(n=>n+8)}>Next</button></div>
    {view.detailLimited&&<p {...s('mapLimit')}>Showing the 500 most-used positions in this group. Increase Minimum games to reduce it; use the board’s connections to explore further.</p>}
  </section>
}
