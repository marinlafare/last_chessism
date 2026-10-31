import { readFile } from 'node:fs/promises'
import { browserFixture } from './browser-fixture.mjs'
const read = name => readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url),'utf8').then(JSON.parse)
const [base,scopes,charts] = await Promise.all(['appearance_registry','appearance_scopes','appearance_charts'].map(read))
const registry = {...base,properties:{...base.properties,...charts.properties},scopes:Object.fromEntries(Object.entries(scopes.scopes).map(([id,spec])=>[id,{...spec,targets:{...scopes.common_targets,...charts.targets,...spec.targets}}]))}

await browserFixture(9311,async ({send,context,evaluate,assert,waitFor})=>{
  await send('script.addPreloadScript',{functionDeclaration:`()=>{window.gameReviewRegistry=${JSON.stringify(registry)}}`})
  await send('browsingContext.setViewport',{context,viewport:{width:1440,height:1000}})
  const load=()=>send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/gameReview.html',wait:'complete'})
  const field=`document.querySelector('.game-review-sound-set')`, button=`${field}?.querySelector('button')`, menu=`document.querySelector('.game-review-sound-list')`
  const option=name=>`document.querySelector('[data-sound-set="${name}"]')`
  const key=value=>send('input.performActions',{context,actions:[{type:'key',id:'keyboard',actions:[{type:'keyDown',value},{type:'keyUp',value}]}]})
  const hover=async expression=>{
    const point=JSON.parse(await evaluate(`JSON.stringify((()=>{const r=${expression}.getBoundingClientRect();return{x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())`))
    await send('input.performActions',{context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[{type:'pointerMove',origin:'viewport',...point}]}]})
  }
  const open=async()=>{await evaluate(`${button}.focus();${button}.click()`);await waitFor(`${button}.getAttribute('aria-expanded')==='true'`)}
  const edit=async()=>{
    await evaluate(`${button}.dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:220,clientY:180}))`)
    await waitFor(`document.querySelector('[data-appearance-target="gameReview.soundSet.option"]')`)
  }
  const set=(target,prop,value)=>evaluate(`(()=>{const input=document.querySelector('[data-appearance-target="${target}"] [id$="-${prop}"]');Object.getOwnPropertyDescriptor(input.tagName==='SELECT'?HTMLSelectElement.prototype:HTMLInputElement.prototype,'value').set.call(input,${JSON.stringify(String(value))});input.dispatchEvent(new Event('input',{bubbles:true}));input.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  const closeEditor=()=>evaluate(`document.querySelector('[aria-label="Close appearance popup"]').click()`)
  await load();await waitFor(`${button} && document.querySelectorAll('.game-review-square').length===64`)
  await assert(`${field}.offsetHeight<28 && ${button}.offsetHeight<=${field}.offsetHeight`)
  await assert(`${field}.getBoundingClientRect().bottom<=document.querySelector('.game-review-stage').getBoundingClientRect().top`)
  // Keyboard opens and changes the choice without moving through the game.
  await evaluate(`${button}.focus()`);await key('\uE015');await key('\uE015');await key('\uE007')
  await waitFor(`${field}.dataset.selectedLabel==='NES' && ${menu}.hidden`)
  await open();await key('f');await key('\uE007');await waitFor(`${field}.dataset.selectedLabel==='Futuristic'`)
  await open();await hover(option('glass'));await key('\uE015')
  await assert(`${option('metal')}.dataset.highlighted==='true' && document.querySelectorAll('.game-review-sound-option[data-highlighted="true"]').length===1`)
  await key('\uE007');await waitFor(`${field}.dataset.selectedLabel==='Metal'`)
  await assert(`!document.querySelector('.game-review-moves button.active') && gameReviewTest.sounds===0`)
  // List remains above the board, rather than hiding behind moving pieces.
  await open()
  await assert(`(()=>{const n=${option('wooden')},r=n.getBoundingClientRect();return document.elementFromPoint(r.left+r.width/2,r.top+r.height/2).closest('[role="option"]')===n})()`)
  await key('\uE00C');await assert(`${menu}.hidden && document.querySelector('.game-review-modal')`)
  await open();await key('\uE004');await assert(`${menu}.hidden && document.activeElement.getAttribute('data-style-id')==='gameReview.soundToggle'`)
  // All related controls are available even while the dropdown is closed.
  await evaluate(`document.querySelector('.appearance-account-controls > button').click()`);await edit()
  for(const target of ['gameReview.soundSet','gameReview.soundSet.list','gameReview.soundSet.option']){
    for(const prop of ['background','hoverBackground','borderColor','hoverBorderColor','radius','padding'])await assert(`document.querySelector('[data-appearance-target="${target}"] [id$="-${prop}"]')`)
  }
  await set('gameReview.soundSet','reviewSoundActiveBorderColor','#abcdef')
  await set('gameReview.soundSet','reviewSoundActiveBorderWidth',3)
  await set('gameReview.soundSet.list','background','#132435')
  await set('gameReview.soundSet.list','borderColor','#456789')
  await set('gameReview.soundSet.list','radius',9)
  await set('gameReview.soundSet.option','background','#001122')
  await set('gameReview.soundSet.option','hoverBackground','#334455')
  await set('gameReview.soundSet.option','reviewSoundSelectedBackground','#667788')
  await set('gameReview.soundSet.option','reviewSoundSelectedBorder','#aabbcc')
  await closeEditor();await hover(`document.querySelector('.game-review-header')`);await open()
  await assert(`getComputedStyle(${field}).outlineColor==='rgb(171, 205, 239)' && getComputedStyle(${field}).outlineWidth==='3px' && getComputedStyle(${field}).outlineOffset==='-3px'`)
  await assert(`getComputedStyle(${menu}).backgroundColor==='rgb(19, 36, 53)' && getComputedStyle(${menu}).borderColor==='rgb(69, 103, 137)' && getComputedStyle(${menu}).borderRadius==='9px'`)
  await hover(option('glass'))
  await assert(`getComputedStyle(${option('glass')}).backgroundColor==='rgb(51, 68, 85)' && getComputedStyle(${option('metal')}).backgroundColor==='rgb(102, 119, 136)' && getComputedStyle(${option('metal')}).borderColor==='rgb(170, 187, 204)'`)
  await assert(`getComputedStyle(${option('piano')}).backgroundColor==='rgb(0, 17, 34)'`)
  await evaluate(`${option('glass')}.click()`);await assert(`${menu}.hidden && ${field}.dataset.selectedLabel==='Glass'`)
  await evaluate(`[...document.querySelectorAll('.appearance-account-controls button')].find(n=>n.textContent==='Save styles').click()`)
  await waitFor(`localStorage.getItem('game-review-styles-fixture') && !document.querySelector('.appearance-account-controls > button').textContent.includes('*')`)
  await load();await waitFor(`${button}`);await assert(`${field}.dataset.selectedLabel==='Glass'`)
  await send('browsingContext.setViewport',{context,viewport:{width:390,height:750}})
  await open();await assert(`(()=>{const r=${menu}.getBoundingClientRect();return r.left>=0 && r.right<=innerWidth && r.bottom<=innerHeight})()`)
  await assert(`getComputedStyle(${field}).outlineColor==='rgb(171, 205, 239)' && getComputedStyle(${menu}).backgroundColor==='rgb(19, 36, 53)'`)
  // Outside clicks close the list, and Escape then still closes the viewer.
  await evaluate(`document.querySelector('.game-review-header').dispatchEvent(new PointerEvent('pointerdown',{bubbles:true}))`)
  await assert(`${menu}.hidden`)
  await evaluate(`${button}.focus()`);await key('\uE00C');await waitFor(`!document.querySelector('.game-review-modal')`)
  await assert(`!gameReviewTest.errors.length && !gameReviewTest.unexpected.length`)
  console.log('PASS: compact height, inset active border, editable list/options from closed trigger, live colors and save/reload, keyboard/typeahead/shared highlight, stacking, mobile and unchanged game navigation. Mock data only.')
})
