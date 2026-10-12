import React from 'react'
import { createRoot } from 'react-dom/client'
import AppearanceProvider,{AppearanceControls} from '/src/appearance/AppearanceProvider.jsx'
import {styleTarget} from '/src/appearance/styleCore.js'
import '/src/appearance/chartColors.css'
import PendingRepetitionChart from '/src/pages/scored-positions/PendingRepetitionChart.jsx'
import BuilderChart from '/src/pages/research/algorithms/BuilderChart.jsx'
import AppearanceCircles from '/src/pages/scored-positions/AppearanceCircles.jsx'
import SummaryGamesChart from '/src/pages/home/SummaryGamesChart.jsx'
import '/src/pages/home/home.css'
import '/src/pages/scored-positions/scoredPositions.css'
const registry={version:1,scope:'scored_positions',properties:{background:{kind:'color',css:'background-color',label:'Background'},radius:{kind:'number',css:'border-radius',label:'Radius',min:0,max:40,unit:'px'}},targets:{page:{label:'Page',properties:['background']},'metrics.panel':{label:'Metrics panel',properties:['background','radius']}}}
const charts=window.appearanceTestCharts||{properties:{},targets:{}}
const circles=window.appearanceTestRegistry||{properties:{},targets:{}}
registry.properties={...circles.properties,...registry.properties,...charts.properties}
registry.targets={...circles.targets,...registry.targets}
registry.scopes={scored_positions:{label:'Scored Positions',targets:{...registry.targets,...charts.targets}},home:{label:'Home',targets:{...charts.targets,...window.appearanceTestScopes?.scopes.home.targets}}}
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
 h('section',{'data-style-scope':'different_page','data-style-id':'metrics.panel',id:'outside'},'Other page'),
 h('div',{id:'chart-fixture',style:{height:180}},h(PendingRepetitionChart,{data:{refreshed_at:'2026-10-11T12:00:00Z',rows:[{repetitions:1,fen_count:20},{repetitions:2,fen_count:10}]}})),
 h(BuilderChart,{output:{name:'Test',type:'bar',x:'X',y:'Y',points:[{x:1,y:2,row_key:'one'}]}}),
 h('div',{id:'circle-fixture',style:{height:220,'--accent':'#15c780','--surface':'#121920'}},h(AppearanceCircles,{active:true,data:{refreshed_at:'2026-10-11T12:00:00Z'},view:{bars:[{appearances:1,count:50},{appearances:2,count:30},{appearances:3,count:20}]}})),
 h('div',{id:'home-bars-fixture',style:{width:420,maxWidth:'100%'}},h(SummaryGamesChart,{ready:true,players:Array.from({length:6},(_,i)=>({player_name:'player'+i,title:'GM',games:100-i*10}))}))))
