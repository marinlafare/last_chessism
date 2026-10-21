import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useAppearance } from './AppearanceProvider'
import { registryScope } from './styleCore'
import { fitEditor, placeEditorOpposite, resizeEditor, RESIZE_EDGES } from './editorGeometry'
import { editorTargets, restoreSavedStyle } from './editorTargets'
import AppearancePropertyFields from './AppearancePropertyFields'

const viewport=()=>({width:window.innerWidth,height:window.innerHeight})

export default function AppearanceEditor() {
  const ctx=useAppearance(), {selection,saved,draft,setDraft}=ctx
  const [position,setPosition]=useState(()=>fitEditor({left:24,top:80,width:350,height:560},viewport())), [minimized,setMinimized]=useState(false), [rect,setRect]=useState(null)
  const dragCleanup=useRef(null)
  useEffect(()=>()=>dragCleanup.current?.(),[])
  useEffect(()=>{
    const clamp=()=>setPosition(p=>fitEditor(p,viewport()))
    window.addEventListener('resize',clamp)
    return()=>window.removeEventListener('resize',clamp)
  },[])
  useLayoutEffect(()=>{
    dragCleanup.current?.()
    if(selection){setPosition(p=>placeEditorOpposite(p,selection,viewport()));setMinimized(false)}
  },[selection])
  useLayoutEffect(()=>{
    if(!selection)return undefined
    const update=()=>setRect(selection.element.isConnected?selection.element.getBoundingClientRect():null)
    update();const observer=new ResizeObserver(update);observer.observe(selection.element)
    window.addEventListener('scroll',update,true);window.addEventListener('resize',update)
    return()=>{observer.disconnect();window.removeEventListener('scroll',update,true);window.removeEventListener('resize',update)}
  },[selection,draft])
  if(!selection)return null
  const targets=editorTargets(saved.registry,selection)
  if(!targets.length)return null
  const target=targets[0].spec
  const restore=()=>setDraft(rows=>targets.reduce((current,item)=>restoreSavedStyle(current,saved.overrides,item.scope,item.id),rows))
  const drag=(e,edge=null)=>{
    if(e.button!==0 || (!edge&&e.target.closest('button')))return
    e.preventDefault()
    dragCleanup.current?.()
    const node=e.currentTarget,start={x:e.clientX,y:e.clientY,...position}
    node.setPointerCapture(e.pointerId)
    const move=event=>setPosition(edge?resizeEditor(start,edge,event.clientX-start.x,event.clientY-start.y,viewport())
      :fitEditor({...start,left:start.left+event.clientX-start.x,top:start.top+event.clientY-start.y},viewport()))
    const escape=event=>{if(event.key==='Escape'){event.stopPropagation();setPosition({left:start.left,top:start.top,width:start.width,height:start.height});end()}}
    const end=()=>{
      node.removeEventListener('pointermove',move);node.removeEventListener('pointerup',end);node.removeEventListener('pointercancel',end);node.removeEventListener('lostpointercapture',end)
      window.removeEventListener('keydown',escape);window.removeEventListener('blur',end)
      if(node.hasPointerCapture(e.pointerId))node.releasePointerCapture(e.pointerId)
      dragCleanup.current=null
    }
    dragCleanup.current=end
    window.addEventListener('keydown',escape);window.addEventListener('blur',end)
    node.addEventListener('pointermove',move);node.addEventListener('pointerup',end);node.addEventListener('pointercancel',end);node.addEventListener('lostpointercapture',end)
  }
  return createPortal(<div data-appearance-tools>
    {rect&&<div className="appearance-inspector-outline" style={{left:rect.left,top:rect.top,width:rect.width,height:rect.height}} />}
    <section className="appearance-editor" role="dialog" aria-label="Edit appearance" style={{...position,...(minimized?{height:'auto'}:{})}}>
      <header onPointerDown={e=>drag(e)}><strong>{target.label}</strong>
        <button type="button" disabled={ctx.busy} aria-label="Restore this element's saved style" onClick={restore}>↶</button>
        <button type="button" aria-label={minimized?'Restore editor':'Minimize editor'} onClick={()=>setMinimized(v=>!v)}>{minimized?'□':'−'}</button>
        <button type="button" aria-label="Close appearance popup" onClick={()=>ctx.setSelection(null)}>×</button>
      </header>
      {!minimized&&<div className="appearance-editor-body">
        <small>{registryScope(saved.registry,selection.scope).label} · {saved.presets.find(p=>p.id===saved.active_preset_id)?.name||'Default (editing a copy)'}</small>
        <button type="button" disabled={!selection.element.parentElement?.closest('[data-style-id]')} onClick={()=>ctx.select(selection.element.parentElement,selection.x,selection.y)}>Select enclosing container</button>
        {targets.map((item,index)=><div className="appearance-property-group" data-appearance-target={item.id} key={item.id}>
          {index>0&&<strong>{item.spec.label}</strong>}
          <AppearancePropertyFields editable={item} related={index>0}/>
        </div>)}
        {ctx.error&&<p role="alert">{ctx.error}</p>}
        <button type="button" disabled={!ctx.dirty||ctx.busy} onClick={()=>ctx.save()}>{ctx.busy?'Saving…':'Save all style changes'}</button>
        <small>Closing keeps your draft. Saving Default creates a named preset.</small>
      </div>}
      {!minimized&&RESIZE_EDGES.map(edge=><div key={edge} aria-hidden="true" data-resize-edge={edge}
        className={`appearance-resize-edge appearance-resize-${edge}`} onPointerDown={e=>drag(e,edge)}/>)}
    </section>
  </div>,document.body)
}
