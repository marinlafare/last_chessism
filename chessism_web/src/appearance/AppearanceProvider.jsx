import { createContext, lazy, Suspense, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { fetchAppearance, saveAppearance } from './appearanceApi'
import { appearanceCss, sameOverrides, registryTarget, registryScope, seedIndependentStyles } from './styleCore'
import { scopeForPath } from './pageScopes'
import './appearance.css'

const Editor = lazy(() => import('./AppearanceEditor'))
const Context = createContext(null)
export const useAppearance = () => useContext(Context)

export default function AppearanceProvider({ account, pageKey, children }) {
  const [saved,setSaved] = useState(null), [draft,setDraft] = useState([])
  const [enabled,setEnabled] = useState(false), [selection,setSelection] = useState(null)
  const [error,setError] = useState(''), [busy,setBusy] = useState(false)
  const mounted = useRef(false), readSequence = useRef(0), writing = useRef(false)
  const dirty = Boolean(saved && !sameOverrides(draft,saved.overrides))
  const current = useRef({dirty,saved}); current.current={dirty,saved}
  useEffect(()=>setSelection(null),[pageKey])
  const accept = useCallback(data => {
    const snapshot = { ...data, overrides: seedIndependentStyles(data.registry, data.overrides) }
    current.current={dirty:false,saved:snapshot};setSaved(snapshot); setDraft(snapshot.overrides); setError('')
  },[])
  const load = useCallback(async () => {
    if (writing.current) return
    const request = ++readSequence.current
    try {
      const data=await fetchAppearance()
      if (!mounted.current || request !== readSequence.current || writing.current) return
      if(data.revision<(current.current.saved?.revision??0))return
      if (current.current.dirty) {
        const previous = current.current.saved
        const incomingRows = seedIndependentStyles(data.registry, data.overrides)
        const previousRows = seedIndependentStyles(data.registry, previous.overrides)
        // A rename/default promotion can advance metadata without changing the
        // active theme's styles. Rebase that revision safely, keeping the draft.
        const metadataOnly = data.active_preset_id === previous.active_preset_id && sameOverrides(incomingRows, previousRows)
        if(data.revision!==previous.revision && !metadataOnly) setError('Saved styles changed elsewhere. Your draft is preserved. Reload to discard it and use the new version.')
        else setError('')
        // Registry additions are not preset edits. Refresh the available controls
        // without replacing the user's draft or advancing its saved revision.
        const snapshot = metadataOnly ? { ...data, overrides: incomingRows }
          : { ...previous, registry: data.registry, overrides: previousRows }
        current.current = { dirty: true, saved: snapshot }
        setSaved(snapshot)
        setDraft(rows => seedIndependentStyles(data.registry, rows))
      } else accept(data)
    } catch(e) { if(mounted.current && request === readSequence.current) setError(e.message) }
  },[accept])
  useEffect(() => {
    mounted.current=true
    load()
    window.addEventListener('focus',load)
    const storage = event => { if(event.key==='chessism-style-revision')load() }
    window.addEventListener('storage',storage)
    return()=>{mounted.current=false;readSequence.current++;window.removeEventListener('focus',load);window.removeEventListener('storage',storage)}
  },[accept,load])
  useEffect(() => {
    if (!dirty) return undefined
    const warn=e=>{e.preventDefault();e.returnValue=''}
    window.addEventListener('beforeunload',warn)
    return()=>window.removeEventListener('beforeunload',warn)
  },[dirty])
  const select = useCallback((element, x=24,y=80) => {
    const target=element?.closest?.('[data-style-id][data-style-scope]')
    const scope=target?.dataset.styleScope
    if(target && registryTarget(saved?.registry,scope,target.dataset.styleId)) {
      setSelection({element:target,id:target.dataset.styleId,scope,x,y})
      return true
    }
    return false
  },[saved])
  useEffect(() => {
    if(!enabled || account?.role!=='admin')return undefined
    const context=e=>{
      if(e.shiftKey || e.target.closest?.('[data-appearance-tools]'))return
      if(select(e.target,e.clientX,e.clientY))e.preventDefault()
    }
    const keyboard=e=>{
      if((e.key==='ContextMenu'||(e.shiftKey&&e.key==='F10'))&&!e.target.closest?.('[data-appearance-tools]')) {
        const rect=e.target.getBoundingClientRect()
        if(select(e.target,rect.left,rect.bottom))e.preventDefault()
      }
    }
    const outside=e=>{if(e.button===0&&!e.target.closest?.('[data-appearance-tools]'))setSelection(null)}
    document.addEventListener('contextmenu',context);document.addEventListener('keydown',keyboard);document.addEventListener('pointerdown',outside)
    return()=>{document.removeEventListener('contextmenu',context);document.removeEventListener('keydown',keyboard);document.removeEventListener('pointerdown',outside)}
  },[enabled,account?.role,select])
  const change=async(action,payload)=>{
    if(!saved || writing.current)return
    writing.current=true;readSequence.current++
    setBusy(true);setError('')
    try {
      const data=await saveAppearance(action,{revision:saved.revision,...payload})
      if (!mounted.current) return
      accept(data)
      try {localStorage.setItem('chessism-style-revision',String(data.revision))} catch{}
    } catch(e) {if(mounted.current)setError(e.message)} finally {writing.current=false;if(mounted.current)setBusy(false)}
  }
  const save=(copy=false)=>{
    const preset_id=copy?null:saved.active_preset_id
    const name=preset_id?undefined:window.prompt('Name the new style preset (Default will stay unchanged):')
    if(!preset_id && name===null)return
    return change('save',{preset_id,name,overrides:draft})
  }
  const canDiscard=()=>!dirty||window.confirm('Discard unsaved appearance changes?')
  const pageScope=scopeForPath(pageKey || window.location.pathname)
  const value={saved,draft,setDraft,enabled,selection,setSelection,select,error,busy,dirty,save,pageScope,
    toggle:()=>{if(!enabled){setEnabled(true);load();return}if(canDiscard()){setDraft(saved.overrides);setEnabled(false);setSelection(null)}},
    activate:id=>{if(canDiscard())change('activate',{preset_id:id||null})},
    rename:()=>{const name=window.prompt('New preset name:',saved.presets.find(p=>p.id===saved.active_preset_id)?.name||'');if(name!==null)change('rename',{preset_id:saved.active_preset_id,name})},
    reload:()=>{if(canDiscard()){current.current.dirty=false;load()}},
    admin:account?.role==='admin'}
  const css=useMemo(()=>appearanceCss(saved?.registry,draft),[saved?.registry,draft])
  return <Context.Provider value={value}>
    {saved ? children : <div className="appearance-loading" role="status" aria-live="polite">
      {error ? <>Could not load your appearance. <button type="button" onClick={load}>Retry</button></> : 'Loading appearance…'}
    </div>}
    {createPortal(<style data-appearance-overrides>{css}</style>,document.head)}
    {enabled&&value.admin&&<Suspense fallback={null}><Editor /></Suspense>}
  </Context.Provider>
}

export function AppearanceControls() {
  const ctx=useAppearance()
  if(!ctx?.admin)return null
  return <div className="appearance-account-controls" data-appearance-tools>
    <button type="button" disabled={!ctx.saved||ctx.busy} onClick={ctx.toggle} aria-pressed={ctx.enabled}>
      {ctx.enabled?'Exit appearance editing':'Edit appearance'}{ctx.dirty?' *':''}
    </button>
    {ctx.enabled&&<>
      <select aria-label="Style preset" disabled={ctx.busy} value={ctx.saved.active_preset_id||''} onChange={e=>ctx.activate(e.target.value)}>
        <option value={ctx.saved.default_preset_id||''}>Default</option>{ctx.saved.presets.filter(p=>p.id!==ctx.saved.default_preset_id).map(p=><option key={p.id} value={p.id}>{p.name}</option>)}
      </select>
      <button type="button" disabled={ctx.busy} onClick={()=>ctx.save(true)}>New preset</button>
      <button type="button" disabled={ctx.busy||!ctx.saved.active_preset_id||ctx.saved.active_preset_id===ctx.saved.default_preset_id||ctx.dirty} onClick={ctx.rename}>Rename</button>
      <button type="button" onClick={()=>ctx.select(document.querySelector(`[data-style-scope="${ctx.pageScope}"][data-style-id="page"]`) || document.querySelector('[data-style-id="page"]'))}>Select page</button>
      <button type="button" disabled={!ctx.dirty||ctx.busy} onClick={()=>ctx.save()}>Save styles</button>
      <small className="appearance-editing-hint">{registryScope(ctx.saved.registry,ctx.pageScope)?.label || 'Appearance'} · Right-click an element to edit it</small>
    </>}
    {ctx.error&&<span className="appearance-account-error" role="alert">{ctx.error} <button type="button" onClick={ctx.reload}>Reload saved styles</button></span>}
  </div>
}
