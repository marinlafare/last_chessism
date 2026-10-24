// Real disclosure/editor components; every API request uses local fixture data.
import {readFile} from 'node:fs/promises'
import {browserFixture} from './browser-fixture.mjs'

const read=name=>readFile(new URL(`../../chessism_api/${name}.json`,import.meta.url),'utf8').then(JSON.parse)
const [base,scopes,charts]=await Promise.all(['appearance_registry','appearance_scopes','appearance_charts'].map(read))
const registry={...base,properties:{...base.properties,...charts.properties},scopes:Object.fromEntries(
  Object.entries(scopes.scopes).map(([scope,spec])=>[scope,{...spec,targets:{...scopes.common_targets,...charts.targets,...spec.targets}}]))}
await browserFixture(9302,async({send,context,evaluate,assert,waitFor})=>{
  await send('script.addPreloadScript',{functionDeclaration:`()=>{window.profileStylesRegistry=${JSON.stringify(registry)}}`})
  await send('browsingContext.setViewport',{context,viewport:{width:1440,height:1100}})
  const load=()=>send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/profileStyles.html',wait:'complete'})
  const node=id=>`document.querySelector('[data-style-id="${id}"]')`
  const open=async id=>{
    await evaluate(`${node(id)}.dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:300,clientY:400}))`)
    await waitFor(`document.querySelector('.appearance-property-group[data-appearance-target="${id}"] #appearance-marginRight')`)
  }
  const set=async(property,value)=>evaluate(`(()=>{const input=document.querySelector('#appearance-${property}');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,'${value}');input.dispatchEvent(new Event('input',{bubbles:true}));input.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  const close=()=>evaluate(`document.querySelector('[aria-label="Close appearance popup"]').click()`)
  const geometry=id=>`(()=>{const b=${node(id)},r=b.getBoundingClientRect(),p=b.parentElement.getBoundingClientRect();return {left:r.left-p.left,right:p.right-r.right,top:r.top-p.top,bottom:p.bottom-r.bottom,width:r.width,parentWidth:p.width}})()`
  const expect=async(id,values)=>waitFor(`(()=>{const g=${geometry(id)};return Object.entries(${JSON.stringify(values)}).every(([key,value])=>Math.abs(g[key]-value)<1)})()`)
  await load()
  await waitFor(`${node('player.analysis.toggle')} && getComputedStyle(document.documentElement).getPropertyValue('--surface-2').trim()==='#080808'`)
  await evaluate(`document.fonts.ready`)
  await expect('player.analysis.toggle',{left:0,right:0})
  await evaluate(`document.querySelector('.appearance-account-controls > button').click()`)
  await open('player.analysis.toggle');await set('marginRight',72);await close()
  await expect('player.analysis.toggle',{left:0,right:72})
  await expect('player.measures.toggle',{left:0,right:0})
  await open('player.analysis.toggle');await set('marginLeft',24);await close()
  await expect('player.analysis.toggle',{left:24,right:72})
  await open('player.analysis.toggle');await set('margin',12);await set('marginRight',40);await close()
  await expect('player.analysis.toggle',{left:12,right:40,top:12,bottom:12})
  await evaluate(`${node('player.analysis.toggle')}.click()`)
  await waitFor(`${node('player.activityHours.toggle')}`)
  await assert(`(()=>{const n=${node('player.activityHours.toggle')},h=n.parentElement;return Math.abs(h.getBoundingClientRect().width/h.parentElement.getBoundingClientRect().width-.6)<.01})()`)
  await open('player.activityHours.toggle');await set('marginRight',20);await close()
  await expect('player.activityHours.toggle',{left:0,right:20})
  await expect('player.analysis.toggle',{left:12,right:40,top:12,bottom:12})
  await open('player.analysis');await set('background','#123456');await set('hoverBackground','#654321');await close()
  const hover=async()=>{
    const point=JSON.parse(await evaluate(`JSON.stringify((()=>{const r=${node('player.analysis')}.getBoundingClientRect();return{x:Math.round(r.left+4),y:Math.round(r.bottom-4)}})())`))
    await send('input.performActions',{context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[{type:'pointerMove',origin:'viewport',...point}]}]})
    await waitFor(`getComputedStyle(${node('player.analysis')}).backgroundColor==='rgb(101, 67, 33)'`)
    await assert(`getComputedStyle(${node('player.measures')}).backgroundColor==='rgb(8, 8, 8)'`)
    await assert(`getComputedStyle(${node('player.analysis.toggle')}).backgroundColor==='rgba(0, 0, 0, 0)'`)
  }
  await hover()
  await evaluate(`[...document.querySelectorAll('button')].find(n=>n.textContent==='Save styles').click()`)
  await waitFor(`localStorage.getItem('profile-styles-fixture')!==null`)
  await send('input.performActions',{context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[{type:'pointerMove',origin:'viewport',x:1,y:1}]}]})
  await load()
  await waitFor(`${node('player.analysis.toggle')}`)
  await waitFor(`getComputedStyle(${node('player.analysis')}).backgroundColor==='rgb(18, 52, 86)'`)
  await hover()
  await expect('player.analysis.toggle',{left:12,right:40,top:12,bottom:12})
  await expect('player.measures.toggle',{left:0,right:0})
  await send('browsingContext.setViewport',{context,viewport:{width:640,height:1000}})
  await expect('player.analysis.toggle',{left:12,right:40,top:12,bottom:12})
  await assert(`document.documentElement.scrollWidth<=innerWidth`)
  console.log('PASS: independent Behavioral analysis container hover and save/reload, right/left/all-side margins, nested 60% headings, unaffected controls and responsive layout.')
})
