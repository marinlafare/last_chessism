import React from 'react'
import {createRoot} from 'react-dom/client'
import AppearanceProvider, {AppearanceControls} from '/src/appearance/AppearanceProvider.jsx'
import {styleTarget} from '/src/appearance/styleCore.js'

const state=window.radiusTest={requests:[]}, key='radius-style-fixture'
window.prompt=()=> 'Radius test'
window.fetch=async(input,options={})=>{
  const path=new URL(input,location.href).pathname, method=options.method||'GET'
  state.requests.push({path,method})
  let saved={revision:0,presets:[],overrides:[],active_preset_id:null,...JSON.parse(localStorage.getItem(key)||'null')}
  if(method==='POST' && path.endsWith('/appearance/save')) {
    saved={...saved,revision:saved.revision+1,presets:[{id:'test',name:'Radius test'}],active_preset_id:'test',overrides:JSON.parse(options.body).overrides}
    localStorage.setItem(key,JSON.stringify(saved))
  } else if(method!=='GET' || !path.endsWith('/appearance')) throw Error('No live API access: '+path)
  return new Response(JSON.stringify({...saved,registry:window.radiusRegistry}),{headers:{'Content-Type':'application/json'}})
}
const target=id=>styleTarget(id,'home')
createRoot(document.getElementById('root')).render(<AppearanceProvider account={{role:'admin'}} pageKey="/">
  <AppearanceControls />
  <main style={{padding:24,maxWidth:700}}>
    <section {...target('panel')} data-sample="panel" style={{padding:16,background:'#d9efdf'}}>
      <h2 {...target('heading')} data-sample="heading" style={{background:'#b4dcd0'}}>Heading</h2>
      <button {...target('button')} data-sample="button">Button</button>
      <input {...target('field')} data-sample="field" aria-label="Example input" defaultValue="Value" />
      <img {...styleTarget('card','research')} data-sample="image" width="40" height="40" alt="Test image"
        src="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='40' height='40'%3E%3Crect width='40' height='40' fill='%23123456'/%3E%3C/svg%3E" />
    </section>
    <svg {...target('chart.bars')} data-sample="chart" width="300" height="100" style={{background:'#ddd'}}>
      <rect {...styleTarget('title','scored_positions')} data-sample="rect" x="10" y="10" width="60" height="60" rx="3" fill="#124" />
      <path {...styleTarget('unique.slice','scored_positions')} data-sample="path" d="M100 10 L150 10 L125 60 Z" fill="#124" />
      <circle {...target('summary.titled')} data-sample="circle" cx="210" cy="40" r="25" fill="#124" />
    </svg>
    <table {...target('table')} data-sample="table" style={{borderCollapse:'collapse',background:'#ddd'}}>
      <tbody><tr {...target('value')} data-sample="row"><td {...target('text')} data-sample="cell">Table cell</td></tr></tbody>
    </table>
  </main>
</AppearanceProvider>)
