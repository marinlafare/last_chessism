import { useEffect, useId, useRef, useState } from 'react'
import { MultiDirectedGraph } from 'graphology'
import Sigma from 'sigma'
import { graphStyle as s } from './GraphBoard'

function SvgFallback({ view, onActivate, onHover, onStyleNode, active }) {
  const [camera, setCamera] = useState({x:0,y:0,scale:1}), drag = useRef(null)
  const marker = useId().replaceAll(':',''), nodes = view.nodes.slice(0, 500)
  const [left,right] = view.bounds.x, [bottom,top] = view.bounds.y, width = right-left || 1, height = top-bottom || 1
  const points = new Map(nodes.map(n => [n.key, {x:40+720*(n.x-left)/width, y:40+420*(n.y-bottom)/height}]))
  return <svg className="opening-map-fallback" viewBox="0 0 800 500" aria-label="Opening map (SVG renderer)"
    onWheel={e => setCamera(c => ({...c,scale:Math.min(8,Math.max(.4,c.scale*(e.deltaY<0?1.15:.87)))}))}
    onPointerDown={e => { if(e.target.tagName==='svg'){drag.current={x:e.clientX,y:e.clientY};e.currentTarget.setPointerCapture(e.pointerId)} }}
    onPointerMove={e => {if(drag.current){const dx=e.clientX-drag.current.x,dy=e.clientY-drag.current.y;drag.current={x:e.clientX,y:e.clientY};setCamera(c=>({...c,x:c.x+dx,y:c.y+dy}))}}}
    onPointerUp={() => {drag.current=null}}>
    <defs><marker id={marker} viewBox="0 0 10 10" refX="10" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="context-stroke" /></marker></defs>
    <g transform={`translate(${camera.x} ${camera.y}) translate(400 250) scale(${camera.scale}) translate(-400 -250)`}>
      {view.edges.filter(e=>points.has(e.source)&&points.has(e.target)).map(edge => {
        const a=points.get(edge.source),b=points.get(edge.target)
        return <line {...s('mapEdge')} className="opening-map-edge" key={edge.key} x1={a.x} y1={a.y} x2={b.x} y2={b.y} markerEnd={`url(#${marker})`} strokeWidth={1+Math.log1p(edge.weight)/3} />
      })}
      {nodes.map(node => { const p=points.get(node.key); return <g key={node.key} role="button" tabIndex={0} aria-label={`${node.label}, ${node.description}`}
        onContextMenu={e=>onStyleNode(node.key,e)}
        onPointerMove={() => onHover(node.key)} onFocus={() => onHover(node.key)} onClick={() => onActivate(node.key)}
        onKeyDown={e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();onActivate(node.key)}}}>
        <circle {...s('mapNode')} cx={p.x} cy={p.y} r={node.size+(active===node.key?3:0)} fill={node.color} stroke={active===node.key?'#fff':'transparent'} strokeWidth="2" />
        <title>{node.label}: {node.description}</title>
      </g>})}
    </g>
  </svg>
}

export default function OpeningMapCanvas({ view, onActivate, onHover, onStyleNode, active, reset, edgeColor, labelColor }) {
  const element = useRef(null), renderer = useRef(null), callbacks = useRef({ onActivate, onHover, onStyleNode })
  const savedCamera = useRef(null)
  const [fallback, setFallback] = useState(false)
  callbacks.current = { onActivate, onHover, onStyleNode }
  useEffect(() => {
    if (fallback) return undefined
    const graph = new MultiDirectedGraph()
    for (const node of view.nodes) graph.addNode(node.key, { ...node })
    for (const edge of view.edges) graph.addDirectedEdgeWithKey(edge.key, edge.source, edge.target, {
      size: Math.min(6, .5 + Math.log1p(edge.weight) / 3), color: edgeColor, type: 'arrow', label: edge.move || '',
    })
    let sigma
    try {
      sigma = new Sigma(graph, element.current, { renderEdgeLabels: false, labelDensity: .3,
        labelColor: {color:labelColor}, labelSize: 11, stagePadding: 36, allowInvalidContainer: true,
        hideEdgesOnMove: true, defaultDrawNodeHover: () => {}, minCameraRatio: .03, maxCameraRatio: 6 })
      renderer.current = sigma
      sigma.setCustomBBox(view.bounds)
      if(savedCamera.current?.key===view.frameKey) sigma.getCamera().setState(savedCamera.current.state)
      sigma.on('clickNode', ({node}) => callbacks.current.onActivate(node))
      sigma.on('enterNode', ({node}) => callbacks.current.onHover(node))
      sigma.on('rightClickNode', ({node,event}) => callbacks.current.onStyleNode(node,event.original))
    } catch { sigma?.kill(); element.current.replaceChildren(); setFallback(true); return undefined }
    return () => { savedCamera.current={key:view.frameKey,state:sigma.getCamera().getState()};renderer.current = null; sigma.kill() }
  }, [view, fallback, edgeColor, labelColor])
  useEffect(() => {
    renderer.current?.setSetting('nodeReducer', (key, data) => ({...data,highlighted:key===active,size:data.size+(key===active?3:0)}))
  }, [active,view])
  useEffect(() => { renderer.current?.getCamera().animatedReset({duration:200}) }, [reset])
  return <div {...s('mapCanvas')} className="opening-map-canvas"><div className="opening-map-renderer" ref={element}>
    {fallback && <SvgFallback key={`${view.frameKey}:${reset}`} view={view} onActivate={onActivate} onHover={onHover} onStyleNode={onStyleNode} active={active} />}
    {!view.nodes.length && <p {...s('empty')}>No positions match these filters.</p>}
    {fallback&&view.nodes.length>500&&<small {...s('mapLimit')}>SVG fallback: first 500 groups drawn. All groups remain available in the list.</small>}
  </div></div>
}
