// Every fetch (including style save/reload) is mocked by dailySalience.html.
import { readFile } from 'node:fs/promises'
import { browserFixture } from './browser-fixture.mjs'

const read = name => readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url), 'utf8').then(JSON.parse)
const [base, scopes, charts] = await Promise.all(['appearance_registry', 'appearance_scopes', 'appearance_charts'].map(read))
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: Object.fromEntries(
  Object.entries(scopes.scopes).map(([scope, spec]) => [scope, { ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets } }])) }
const prefix = 'player.accuracy.salience.'

await browserFixture(9296, async ({send,context,evaluate,assert,waitFor}) => {
  await send('script.addPreloadScript',{functionDeclaration:`()=>{window.salienceRegistry=${JSON.stringify(registry)}}`})
  await send('browsingContext.setViewport',{context,viewport:{width:1440,height:1000}})
  const load=()=>send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/dailySalience.html?appearance=1',wait:'complete'})
  const node=part=>`document.querySelector('[data-style-id="${prefix+part}"]')`
  const open=async part=>{
    await evaluate(`${node(part)}.dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:1200,clientY:300}))`)
    await waitFor(`document.querySelector('.appearance-editor header strong')?.textContent===${JSON.stringify(scopes.scopes.home.targets[prefix+part].label)}`)
  }
  const close=()=>evaluate(`document.querySelector('[aria-label="Close appearance popup"]').click()`)
  const set=async (property,value)=>{
    await evaluate(`(()=>{const input=document.querySelector('#appearance-${property}');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,${JSON.stringify(String(value))});input.dispatchEvent(new Event('input',{bubbles:true}));input.dispatchEvent(new Event('change',{bubbles:true}))})()`)
    if(typeof value==='string'&&value.startsWith('#')) await evaluate(`(()=>{const input=document.querySelector('#appearance-${property}').closest('fieldset').querySelector('input[type=range]');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,'100');input.dispatchEvent(new Event('input',{bubbles:true}));input.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  }
  const hover=async part=>{
    const point=JSON.parse(await evaluate(`JSON.stringify((()=>{const n=${node(part)};n.scrollIntoView({block:'center'});const r=n.getBoundingClientRect();return {x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())`))
    await send('input.performActions',{context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[{type:'pointerMove',origin:'viewport',...point}]}]})
  }
  await load()
  await waitFor(`document.querySelectorAll('.measure-daily-efficiency-point').length===5 && document.querySelector('.appearance-account-controls button')?.disabled===false`)
  await evaluate(`[...document.querySelectorAll('button')].find(n=>n.textContent==='Edit appearance').click()`)
  await open('panel')
  await assert(`document.querySelectorAll('.appearance-property-group').length===10`)
  await assert(`(()=>{const ids=[...document.querySelectorAll('.appearance-editor input[id],.appearance-editor select[id]')].map(n=>n.id);return ids.length===new Set(ids).size})()`)
  await assert(`['0','25','50','75','100'].every(v=>[...document.querySelectorAll('.daily-salience-tick')].some(n=>n.textContent===v))`)
  for(const [part,property,value,computed] of [
    ['panel','background','#123456','backgroundColor'], ['title','color','#abcdef','color'],
    ['rail','background','#654321','backgroundColor'], ['fill','background','#345678','backgroundColor'],
    ['ticks','color','#fedcba','color'], ['minimum.knob','background','#112233','backgroundColor'],
    ['maximum.knob','background','#556677','backgroundColor'],
  ]) {
    await open(part); await set(property,value); await close()
    await assert(`getComputedStyle(${node(part)}).${computed}==='rgb(${[1,3,5].map(i=>parseInt(value.slice(i,i+2),16)).join(', ')})'`)
  }
  await open('panel'); await set('radius',14); await set('margin',8); await close()
  await open('track'); await set('salienceTrackHeight',240); await set('borderWidth',4); await set('padding',6); await close()
  await assert(`getComputedStyle(${node('track')}).height==='240px' && getComputedStyle(${node('panel')}).borderRadius==='14px'`)
  await open('minimum.knob'); await set('hoverBackground','#ff0000'); await set('hoverBorderColor','#00ff00'); await close()
  await hover('minimum.knob')
  await waitFor(`getComputedStyle(${node('minimum.knob')}).backgroundColor==='rgb(255, 0, 0)' && getComputedStyle(${node('minimum.knob')}).borderTopColor==='rgb(0, 255, 0)'`)
  await assert(`getComputedStyle(${node('maximum.knob')}).backgroundColor==='rgb(85, 102, 119)'`)
  // Keyboard and pointer geometry still work after track border/padding edits.
  await evaluate(`${node('minimum.handle')}.dispatchEvent(new KeyboardEvent('keydown',{key:'PageUp',bubbles:true,cancelable:true}))`)
  await waitFor(`${node('minimum.handle')}.getAttribute('aria-valuenow')==='10'`)
  const geometry=JSON.parse(await evaluate(`JSON.stringify((()=>{const track=${node('track')},r=track.getBoundingClientRect(),h=${node('minimum.knob')}.getBoundingClientRect();return {x:Math.round(h.left+h.width/2),y:Math.round(h.top+h.height/2),middle:Math.round(r.top+track.clientTop+track.clientHeight/2)}})())`))
  await send('input.performActions',{context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[{type:'pointerMove',x:geometry.x,y:geometry.y},{type:'pointerDown',button:0},{type:'pointerMove',x:geometry.x,y:geometry.middle,duration:250},{type:'pointerUp',button:0}]}]})
  await waitFor(`${node('minimum.handle')}.getAttribute('aria-valuenow')==='50'`)
  await evaluate(`[...document.querySelectorAll('button')].find(n=>n.textContent==='Save styles').click()`)
  await waitFor(`!document.querySelector('.appearance-account-controls').textContent.includes('*')`)
  await assert(`JSON.parse(localStorage.getItem('salience-styles-fixture')).overrides.every(row=>row.scope==='home' && row.target.startsWith('${prefix}'))`)
  await load()
  await waitFor(`${node('panel')} && getComputedStyle(${node('panel')}).backgroundColor==='rgb(18, 52, 86)'`)
  await assert(`getComputedStyle(${node('track')}).height==='240px' && getComputedStyle(${node('panel')}).margin==='8px'`)
  await evaluate(`salienceTest.show('missing')`)
  await waitFor(`document.querySelectorAll('.daily-salience-handle:disabled').length===2`)
  await assert(`salienceTest.requests.every(r=>r.path.includes('/analysis/measures/daily-') || r.path.endsWith('/appearance'))`)
  console.log('PASS: ten independent salience targets, unique editor labels, fill/border hover, margins, track geometry, keyboard/drag, disabled state and save/reload; no live API calls.')
})
