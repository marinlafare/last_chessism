import React from 'react'
import { createRoot } from 'react-dom/client'
import AppearanceProvider,{AppearanceControls} from '/src/appearance/AppearanceProvider.jsx'
import {styleTarget} from '/src/appearance/styleCore.js'
const registry={version:1,scope:'scored_positions',properties:{background:{kind:'color',css:'background-color',label:'Background'},radius:{kind:'number',css:'border-radius',label:'Radius',min:0,max:40,unit:'px'}},targets:{page:{label:'Page',properties:['background']},'metrics.panel':{label:'Metrics panel',properties:['background','radius']}}}
const key='chessism-test-only-appearance'
const empty=()=>({revision:0,active_preset_id:null,presets:[],overrides:[],registry})
const state=window.appearanceTest={requests:[],prompts:0,fail:false,errors:[],data:JSON.parse(localStorage.getItem(key)||'null')||empty(),rows:{}}
window.addEventListener('error',e=>state.errors.push(e.message))
window.addEventListener('unhandledrejection',e=>state.errors.push(String(e.reason)))
window.prompt=()=>{state.prompts++;return state.name??'My test preset'}
window.confirm=()=>state.confirm!==false
window.fetch=async(input,options={})=>{
 const url=new URL(input,location.href), method=options.method||'GET'
 if(!url.pathname.includes('/appearance'))throw Error('No real API calls in this test')
 const payload=options.body?JSON.parse(options.body):null
 state.requests.push({method,url:url.pathname,payload})
 const reply=(value,status=200)=>new Response(JSON.stringify(value),{status,headers:{'Content-Type':'application/json'}})
 if(method==='GET')return reply(state.data)
 if(state.fail)return reply({detail:'Styles changed in another session.'},409)
 if(payload.revision!==state.data.revision)return reply({detail:'Stale revision'},409)
 const action=url.pathname.split('/').at(-1)
 if(action==='save'){
  const id=payload.preset_id||`preset-${state.data.presets.length+1}`
  if(!payload.preset_id)state.data.presets.push({id,name:payload.name})
  state.rows[id]=payload.overrides
  state.data.active_preset_id=id;state.data.overrides=payload.overrides
 }else if(action==='activate'){
  state.rows[state.data.active_preset_id]=state.data.overrides
  state.data.active_preset_id=payload.preset_id;state.data.overrides=state.rows[payload.preset_id]||[]
 }else if(action==='rename')state.data.presets.find(p=>p.id===payload.preset_id).name=payload.name
 state.data.revision++;localStorage.setItem(key,JSON.stringify(state.data));return reply(state.data)
}
const h=React.createElement
createRoot(document.getElementById('root')).render(h(AppearanceProvider,{account:{role:new URLSearchParams(location.search).get('role')||'admin'}},
 h(AppearanceControls),h('main',{...styleTarget('page'),style:{padding:30}},
 h('section',{...styleTarget('metrics.panel'),id:'editable',tabIndex:0},'Scored Positions panel')),
 h('section',{'data-style-scope':'different_page','data-style-id':'metrics.panel',id:'outside'},'Other page')))
