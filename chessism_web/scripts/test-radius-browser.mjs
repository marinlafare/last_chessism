import {readFile} from 'node:fs/promises'
import {browserFixture} from './browser-fixture.mjs'

const read=name=>readFile(new URL(`../../chessism_api/${name}.json`,import.meta.url),'utf8').then(JSON.parse)
const [base,scopes,charts]=await Promise.all(['appearance_registry','appearance_scopes','appearance_charts'].map(read))
const registry={...base,properties:{...base.properties,...charts.properties},scopes:Object.fromEntries(
  Object.entries(scopes.scopes).map(([scope,spec])=>[scope,{...spec,targets:{...scopes.common_targets,...charts.targets,...spec.targets,...(scope===base.scope?base.targets:{})}}]))}

await browserFixture(9297,async({send,context,evaluate,assert,waitFor})=>{
  await send('script.addPreloadScript',{functionDeclaration:`()=>{window.radiusRegistry=${JSON.stringify(registry)}}`})
  await send('browsingContext.setViewport',{context,viewport:{width:1280,height:1000}})
  const load=()=>send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/radius.html',wait:'complete'})
  const node=name=>`document.querySelector('[data-sample="${name}"]')`
  const open=async name=>{
    await evaluate(`${node(name)}.dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:200,clientY:300}))`)
    await waitFor(`document.querySelector('.appearance-property-group')?.dataset.appearanceTarget===${node(name)}.dataset.styleId`)
  }
  const set=async value=>{
    await evaluate(`(()=>{const n=document.querySelector('#appearance-radius');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,'${value}');n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  }
  await load(); await waitFor(`document.querySelector('.appearance-account-controls button')?.disabled===false`)
  await evaluate(`document.querySelector('.appearance-account-controls button').click()`)
  for(const name of ['panel','heading','button','field','image','chart','rect']) {
    await open(name); await assert(`!!document.querySelector('#appearance-radius')`)
    await set(12)
    await assert(`getComputedStyle(${node(name)}).borderRadius==='12px'`)
    if(name==='rect') await assert(`getComputedStyle(${node(name)}).getPropertyValue('rx')==='12px' && getComputedStyle(${node(name)}).getPropertyValue('ry')==='12px'`)
  }
  await assert(`getComputedStyle(${node('panel')}).overflow==='visible'`)
  for(const name of ['path','circle','row','cell','table']) {
    await open(name); await assert(`!document.querySelector('#appearance-radius')`)
  }
  await evaluate(`${node('table')}.style.borderCollapse='separate'`)
  for(const name of ['table','cell']) {
    await open(name); await assert(`!!document.querySelector('#appearance-radius')`); await set(8)
  }
  await evaluate(`document.querySelector('[aria-label="Close appearance popup"]').click()`)
  await evaluate(`[...document.querySelectorAll('button')].find(n=>n.textContent==='Save styles').click()`)
  await waitFor(`!document.querySelector('.appearance-account-controls').textContent.includes('*')`)
  await load(); await waitFor(`getComputedStyle(${node('heading')}).borderRadius==='12px'`)
  await assert(`getComputedStyle(${node('rect')}).getPropertyValue('rx')==='12px'`)
  await evaluate(`document.querySelector('.appearance-account-controls button').click()`)
  await open('heading'); await set(20)
  await evaluate(`document.querySelector('#appearance-radius').closest('fieldset').querySelector('button').click()`)
  await assert(`getComputedStyle(${node('heading')}).borderRadius==='12px'`)
  await assert(`radiusTest.requests.every(r=>r.path.endsWith('/appearance'))`)
  console.log('PASS: corner radius for layout/text/controls/images/SVG containers and rectangles, unsupported shapes hidden, table capability, save/reload and restore; no live API access.')
})
