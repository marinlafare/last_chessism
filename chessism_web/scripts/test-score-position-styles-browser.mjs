import { readFile } from 'node:fs/promises'
import { browserFixture } from './browser-fixture.mjs'
const read = name => readFile(new URL(`../../chessism_api/${name}.json`,import.meta.url),'utf8').then(JSON.parse)
const [base,scopes,charts] = await Promise.all(['appearance_registry','appearance_scopes','appearance_charts'].map(read))
const registry = {...base,properties:{...base.properties,...charts.properties},scopes:Object.fromEntries(Object.entries(scopes.scopes)
  .map(([id,spec])=>[id,{...spec,targets:{...scopes.common_targets,...charts.targets,...spec.targets}}]))}
await browserFixture(9342,async({send,context,evaluate,assert,waitFor})=>{
 await send('script.addPreloadScript',{functionDeclaration:`()=>{window.scoreStylesRegistry=${JSON.stringify(registry)}}`})
 await send('browsingContext.setViewport',{context,viewport:{width:1440,height:1000}})
 await send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/scorePositionSamples.html',wait:'complete'})
 const node=id=>`document.querySelector('[data-style-id="${id}"]')`
 const close=()=>evaluate(`document.querySelector('[aria-label="Close appearance popup"]').click()`)
 const edit=async id=>{
  await evaluate(`${node(id)}.dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:300,clientY:200}))`)
  await waitFor(`document.querySelector('.appearance-property-group[data-appearance-target="${id}"]')`)
 }
 const set=(prop,value)=>evaluate(`(()=>{const el=document.querySelector('#appearance-${prop}');Object.getOwnPropertyDescriptor(el.tagName==='SELECT'?HTMLSelectElement.prototype:HTMLInputElement.prototype,'value').set.call(el,${JSON.stringify(String(value))});el.dispatchEvent(new Event('input',{bubbles:true}));el.dispatchEvent(new Event('change',{bubbles:true}))})()`)
 await waitFor(`${node('scorePositions.board')}?.querySelector('[data-square]')`)
 await evaluate(`document.querySelector('.appearance-account-controls > button').click()`)
 await assert(`[...document.querySelectorAll('.score-positions-backdrop [data-style-id]')].every(el=>el.dataset.styleId.startsWith('scorePositions.') && scoreStylesRegistry.scopes.home.targets[el.dataset.styleId])`)
 for(const target of ['header','title','filters','body','listPane','list','rank','identity','row.score','labels.appearances','appearances','preview','preview.details','preview.score','preview.player','seeGames','board','board.square.light','board.square.dark','board.rank','board.file','board.piece.white','board.piece.black']) {
  await edit('scorePositions.'+target)
  for(const prop of ['background','hoverBackground','borderColor','hoverBorderColor','radius','padding'])await assert(`document.querySelector('#appearance-${prop}')`)
  await close()
 }
 await edit('scorePositions.title');await set('textAlign','right');await set('color','#abcdef');await close()
 await assert(`getComputedStyle(${node('scorePositions.title')}).textAlign==='right' && getComputedStyle(${node('scorePositions.title')}).color==='rgb(171, 205, 239)' && getComputedStyle(document.querySelector('#outside-score-title')).color!=='rgb(171, 205, 239)'`)
 await edit('scorePositions.row.score');await set('color','#123456');await close()
 await assert(`getComputedStyle(${node('scorePositions.row.score')}).color==='rgb(18, 52, 86)' && getComputedStyle(${node('scorePositions.preview.score')}).color!=='rgb(18, 52, 86)'`)
 await edit('scorePositions.row');await set('positionSelectedBackground','#112233');await set('positionSelectedBorder','#445566');await close()
 await assert(`getComputedStyle(${node('scorePositions.row')}).backgroundColor==='rgb(17, 34, 51)' && getComputedStyle(${node('scorePositions.row')}).borderColor==='rgb(68, 85, 102)'`)
 await edit('scorePositions.board');await set('padding',8);await set('radius',12);await close()
 await waitFor(`(()=>{const f=${node('scorePositions.board')}.getBoundingClientRect(),s=document.querySelector('.score-position-board [data-square="a8"]').getBoundingClientRect();return Math.abs(s.left-f.left-8)<1 && s.width*8<=f.width-15})()`)
 await evaluate(`[...document.querySelectorAll('.appearance-account-controls button')].find(b=>b.textContent==='Save styles').click()`)
 await waitFor(`localStorage.getItem('score-styles-fixture')`)
 await assert(`samples.errors.length===0`)
 console.log('PASS: dedicated popup/board/field controls, title alignment, selected state, padding, saving and no shared title/score styles.')
})
