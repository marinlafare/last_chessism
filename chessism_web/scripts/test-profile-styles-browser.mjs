// Only local components and mocked appearance/game responses; no live data.
import {readFile} from 'node:fs/promises'
import {browserFixture} from './browser-fixture.mjs'

const read=name=>readFile(new URL(`../../chessism_api/${name}.json`,import.meta.url),'utf8').then(JSON.parse)
const [base,scopes,charts]=await Promise.all(['appearance_registry','appearance_scopes','appearance_charts'].map(read))
const registry={...base,properties:{...base.properties,...charts.properties},scopes:Object.fromEntries(
  Object.entries(scopes.scopes).map(([scope,spec])=>[scope,{...spec,targets:{...scopes.common_targets,...charts.targets,...spec.targets}}]))}
const fields=['player','country','name','joined','last-play']
await browserFixture(9300,async({send,context,evaluate,assert,waitFor})=>{
  await send('script.addPreloadScript',{functionDeclaration:`()=>{window.profileStylesRegistry=${JSON.stringify(registry)}}`})
  await send('browsingContext.setViewport',{context,viewport:{width:1440,height:1000}})
  const load=()=>send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/profileStyles.html',wait:'complete'})
  const node=field=>`document.querySelector('[data-style-id="player.metadata.${field}"]')`
  const open=async field=>{
    await evaluate(`${node(field)}.dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:300,clientY:250}))`)
    await waitFor(`document.querySelector('.appearance-property-group[data-appearance-target="player.metadata.${field}"]')`)
  }
  const set=async(property,value)=>evaluate(`(()=>{const input=document.querySelector('#appearance-${property}');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,${JSON.stringify(value)});input.dispatchEvent(new Event('input',{bubbles:true}));input.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  const close=()=>evaluate(`document.querySelector('[aria-label="Close appearance popup"]').click()`)
  await load()
  await waitFor(`${node('last-play.value')}?.textContent==='OCT 23 2026'`)
  await waitFor(`getComputedStyle(${node('player')}).backgroundColor==='rgb(8, 8, 8)'`)
  await evaluate(`document.querySelector('.appearance-account-controls > button').click()`)
  const before=JSON.parse(await evaluate(`JSON.stringify(${JSON.stringify(fields)}.map(field=>getComputedStyle(document.querySelector('[data-style-id="player.metadata.'+field+'"]')).backgroundColor))`))
  for(const [index,field]of fields.entries()){
    for(const part of ['label','value']){
      await open(`${field}.${part}`)
      await assert(`document.querySelector('[data-appearance-target="player.metadata.${field}"] #appearance-related-background') && document.querySelector('#appearance-related-hoverBackground')`)
      await close()
    }
    await open(field);await set('background','#123456');await close()
    await assert(`getComputedStyle(${node(field)}).backgroundColor==='rgb(18, 52, 86)'`)
    for(const [otherIndex,other]of fields.entries())if(otherIndex>index)await assert(`getComputedStyle(${node(other)}).backgroundColor===${JSON.stringify(before[otherIndex])}`)
    await assert(`getComputedStyle(${node(`${field}.label`)}).color==='rgb(171, 205, 239)'`)
  }
  await open('country');await set('hoverBackground','#654321');await close()
  const point=JSON.parse(await evaluate(`JSON.stringify((()=>{const r=${node('country')}.getBoundingClientRect();return{x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())`))
  await send('input.performActions',{context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[{type:'pointerMove',origin:'viewport',...point}]}]})
  await waitFor(`getComputedStyle(${node('country')}).backgroundColor==='rgb(101, 67, 33)'`)
  await assert(`getComputedStyle(${node('player')}).backgroundColor==='rgb(18, 52, 86)'`)
  await open('dates');await assert(`document.querySelector('#appearance-background')`);await close()
  await evaluate(`[...document.querySelectorAll('button')].find(n=>n.textContent==='Save styles').click()`)
  await waitFor(`localStorage.getItem('profile-styles-fixture')!==null`)
  await load()
  await waitFor(`${node('player')} && getComputedStyle(${node('player')}).backgroundColor==='rgb(18, 52, 86)'`)
  for(const field of fields.filter(f=>f!=='country'))await assert(`getComputedStyle(${node(field)}).backgroundColor==='rgb(18, 52, 86)'`)
  await assert(`JSON.parse(localStorage.getItem('profile-styles-fixture')).overrides.some(r=>r.target==='player.metadata.country'&&r.property==='hoverBackground'&&r.value.startsWith('#654321'))`)
  console.log('PASS: five independent profile backgrounds, hover isolation, text-to-background controls, dates wrapper, legacy text preservation and save/reload; all requests mocked.')
})
