import { createContext, lazy, Suspense, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { fetchAppearance, saveAppearance } from './appearanceApi'
import { appearanceCss, sameOverrides } from './styleCore'
import './appearance.css'

const Editor = lazy(() => import('./AppearanceEditor'))
const Context = createContext(null)
export const useAppearance = () => useContext(Context)

export default function AppearanceProvider({ account, pageKey, children }) {
  const [saved,setSaved] = useState(null), [draft,setDraft] = useState([])
  const [enabled,setEnabled] = useState(false), [selection,setSelection] = useState(null)
  const [error,setError] = useState(''), [busy,setBusy] = useState(false)
  const dirty = Boolean(saved && !sameOverrides(draft,saved.overrides))
  const current = useRef({dirty,saved}); current.current={dirty,saved}
  useEffect(()=>setSelection(null),[pageKey])
  const accept = useCallback(data => { current.current={dirty:false,saved:data};setSaved(data); setDraft(data.overrides); setError('') },[])
  const load = useCallback(async () => {
    try {
      const data=await fetchAppearance()
      if(data.revision<(current.current.saved?.revision??0))return
      if (current.current.dirty) {
        if(data.revision!==current.current.saved?.revision) setError('Saved styles changed elsewhere. Your draft is preserved. Reload to discard it and use the new version.')
      } else accept(data)
    } catch(e) { setError(e.message) }
  },[accept])
  useEffect(() => {
    let active=true
    fetchAppearance().then(data=>{if(active)accept(data)}).catch(e=>{if(active)setError(e.message)})
    window.addEventListener('focus',load)
    const storage = event => { if(event.key==='chessism-style-revision')load() }
    window.addEventListener('storage',storage)
    return()=>{active=false;window.removeEventListener('focus',load);window.removeEventListener('storage',storage)}
  },[accept,load])
  useEffect(() => {
    if (!dirty) return undefined
    const warn=e=>{e.preventDefault();e.returnValue=''}
    window.addEventListener('beforeunload',warn)
    return()=>window.removeEventListener('beforeunload',warn)
  },[dirty])
  const select = useCallback((element, x=24,y=80) => {
    const target=element?.closest?.('[data-style-id][data-style-scope]')
    if(target && target.dataset.styleScope===saved?.registry.scope && saved.registry.targets[target.dataset.styleId]) {
      setSelection({element:target,id:target.dataset.styleId,x,y})
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
    document.addEventListener('contextmenu',context);document.addEventListener('keydown',keyboard)
    return()=>{document.removeEventListener('contextmenu',context);document.removeEventListener('keydown',keyboard)}
  },[enabled,account?.role,select])
  const change=async(action,payload)=>{
    if(!saved || busy)return
    setBusy(true);setError('')
    try {
      const data=await saveAppearance(action,{revision:saved.revision,...payload})
      accept(data)
      try {localStorage.setItem('chessism-style-revision',String(data.revision))} catch{}
    } catch(e) {setError(e.message)} finally {setBusy(false)}
  }
  const save=(copy=false)=>{
    const preset_id=copy?null:saved.active_preset_id
    const name=preset_id?undefined:window.prompt('Name the new style preset (Default will stay unchanged):')
    if(!preset_id && name===null)return
    return change('save',{preset_id,name,overrides:draft})
  }
  const canDiscard=()=>!dirty||window.confirm('Discard unsaved appearance changes?')
  const value={saved,draft,setDraft,enabled,selection,setSelection,select,error,busy,dirty,save,
    toggle:()=>{if(!enabled){setEnabled(true);return}if(canDiscard()){setDraft(saved.overrides);setEnabled(false);setSelection(null)}},
    activate:id=>{if(canDiscard())change('activate',{preset_id:id||null})},
    rename:()=>{const name=window.prompt('New preset name:',saved.presets.find(p=>p.id===saved.active_preset_id)?.name||'');if(name!==null)change('rename',{preset_id:saved.active_preset_id,name})},
    reload:()=>{if(canDiscard()){current.current.dirty=false;load()}},
    admin:account?.role==='admin'}
  const css=useMemo(()=>appearanceCss(saved?.registry,draft),[saved?.registry,draft])
  return <Context.Provider value={value}>
    {children}
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
        <option value="">Default</option>{ctx.saved.presets.map(p=><option key={p.id} value={p.id}>{p.name}</option>)}
      </select>
      <button type="button" disabled={ctx.busy} onClick={()=>ctx.save(true)}>New preset</button>
      <button type="button" disabled={ctx.busy||!ctx.saved.active_preset_id||ctx.dirty} onClick={ctx.rename}>Rename</button>
      <button type="button" onClick={()=>ctx.select(document.querySelector('[data-style-id="page"]'))}>Select page</button>
      <button type="button" disabled={!ctx.dirty||ctx.busy} onClick={()=>ctx.save()}>Save styles</button>
    </>}
    {ctx.error&&<span className="appearance-account-error" role="alert">{ctx.error} <button type="button" onClick={ctx.reload}>Reload saved styles</button></span>}
  </div>
}
