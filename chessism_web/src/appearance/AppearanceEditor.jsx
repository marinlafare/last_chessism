import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useAppearance } from './AppearanceProvider'
import { normalizeColor, replaceRgb, validValue } from './styleCore'

export default function AppearanceEditor() {
  const ctx=useAppearance(), {selection,saved,draft,setDraft}=ctx
  const [position,setPosition]=useState({left:24,top:80}), [minimized,setMinimized]=useState(false), [rect,setRect]=useState(null)
  const dragCleanup=useRef(null)
  useEffect(()=>()=>dragCleanup.current?.(),[])
  useEffect(()=>{
    const clamp=()=>setPosition(p=>({left:Math.max(8,Math.min(p.left,innerWidth-358)),top:Math.max(8,Math.min(p.top,innerHeight-100))}))
    window.addEventListener('resize',clamp)
    return()=>window.removeEventListener('resize',clamp)
  },[])
  useEffect(()=>{
    if(selection){setPosition({left:Math.max(8,Math.min(selection.x,innerWidth-368)),top:Math.max(8,Math.min(selection.y,innerHeight-180))});setMinimized(false)}
  },[selection])
  useLayoutEffect(()=>{
    if(!selection)return undefined
    const update=()=>setRect(selection.element.isConnected?selection.element.getBoundingClientRect():null)
    update();const observer=new ResizeObserver(update);observer.observe(selection.element)
    window.addEventListener('scroll',update,true);window.addEventListener('resize',update)
    return()=>{observer.disconnect();window.removeEventListener('scroll',update,true);window.removeEventListener('resize',update)}
  },[selection,draft])
  if(!selection)return null
  const target=saved.registry.targets[selection.id]
  const matches=row=>row.scope===saved.registry.scope&&row.target===selection.id
  const set=(property,value)=>setDraft(rows=>[...rows.filter(row=>!(matches(row)&&row.property===property)),
    ...(value===null?[]:[{scope:saved.registry.scope,target:selection.id,property,value}])])
  const restore=()=>setDraft(rows=>[...rows.filter(row=>!matches(row)),...saved.overrides.filter(matches)])
  const drag=e=>{
    if(e.target.closest('button'))return
    dragCleanup.current?.()
    const node=e.currentTarget,start={x:e.clientX,y:e.clientY,...position}
    node.setPointerCapture(e.pointerId)
    const move=event=>setPosition({left:Math.max(8,Math.min(innerWidth-358,start.left+event.clientX-start.x)),top:Math.max(8,Math.min(innerHeight-100,start.top+event.clientY-start.y))})
    const escape=event=>{if(event.key==='Escape'){setPosition({left:start.left,top:start.top});end()}}
    const end=()=>{
      node.removeEventListener('pointermove',move);node.removeEventListener('pointerup',end);node.removeEventListener('lostpointercapture',end)
      window.removeEventListener('keydown',escape);window.removeEventListener('blur',end)
      if(node.hasPointerCapture(e.pointerId))node.releasePointerCapture(e.pointerId)
      dragCleanup.current=null
    }
    dragCleanup.current=end
    window.addEventListener('keydown',escape);window.addEventListener('blur',end)
    node.addEventListener('pointermove',move);node.addEventListener('pointerup',end);node.addEventListener('lostpointercapture',end)
  }
  return createPortal(<div data-appearance-tools>
    {rect&&<div className="appearance-inspector-outline" style={{left:rect.left,top:rect.top,width:rect.width,height:rect.height}} />}
    <section className="appearance-editor" role="dialog" aria-label="Edit appearance" style={{...position,maxHeight:`calc(100vh - ${position.top+8}px)`}}>
      <header onPointerDown={drag}><strong>{target.label}</strong>
        <button type="button" disabled={ctx.busy} aria-label="Restore this element's saved style" onClick={restore}>↶</button>
        <button type="button" aria-label={minimized?'Restore editor':'Minimize editor'} onClick={()=>setMinimized(v=>!v)}>{minimized?'□':'−'}</button>
        <button type="button" aria-label="Close appearance popup" onClick={()=>ctx.setSelection(null)}>×</button>
      </header>
      {!minimized&&<div className="appearance-editor-body">
        <small>Scored Positions · {saved.presets.find(p=>p.id===saved.active_preset_id)?.name||'Default (editing a copy)'}</small>
        <button type="button" disabled={!selection.element.parentElement?.closest('[data-style-id]')} onClick={()=>ctx.select(selection.element.parentElement,position.left,position.top)}>Select enclosing container</button>
        {target.properties.map(property=>{
          const spec=saved.registry.properties[property], override=draft.find(row=>matches(row)&&row.property===property)
          const computed=getComputedStyle(selection.element).getPropertyValue(spec.css)||String(spec.default??'')
          const value=override?.value??(spec.kind==='color'?normalizeColor(computed):spec.kind==='number'?parseFloat(computed)||0:computed.split(',')[0].replaceAll('"','').trim())
          return <fieldset className="appearance-property" key={property} disabled={ctx.busy}>
            <label htmlFor={`appearance-${property}`}>{spec.label}</label>
            {spec.kind==='color'?<>
              <input id={`appearance-${property}`} type="color" value={normalizeColor(value).slice(0,7)} onChange={e=>set(property,replaceRgb(value,e.target.value))}/>
              <label className="appearance-opacity">Opacity {Math.round(parseInt(normalizeColor(value).slice(7),16)/255*100)}%
                <input aria-label={`${spec.label} opacity`} type="range" min="0" max="100" value={Math.round(parseInt(normalizeColor(value).slice(7),16)/255*100)} onChange={e=>set(property,normalizeColor(value).slice(0,7)+Math.round(Number(e.target.value)*2.55).toString(16).padStart(2,'0'))}/>
              </label>
            </>:spec.kind==='number'?<input id={`appearance-${property}`} type="number" min={spec.min} max={spec.max} value={value} onChange={e=>{const number=Number(e.target.value);if(validValue(spec,number))set(property,number)}}/>
              :<select id={`appearance-${property}`} value={spec.options.includes(value)?value:''} onChange={e=>set(property,e.target.value)}><option value="" disabled>Inherited</option>{spec.options.map(option=><option key={option}>{option}</option>)}</select>}
            <button type="button" disabled={!override} onClick={()=>set(property,null)}>Inherit</button>
          </fieldset>
        })}
        {ctx.error&&<p role="alert">{ctx.error}</p>}
        <button type="button" disabled={!ctx.dirty||ctx.busy} onClick={()=>ctx.save()}>{ctx.busy?'Saving…':'Save all style changes'}</button>
        <small>Closing keeps your draft. Saving Default creates a named preset.</small>
      </div>}
    </section>
  </div>,document.body)
}
