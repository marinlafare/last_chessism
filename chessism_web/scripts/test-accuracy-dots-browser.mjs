import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9295, async ({send,context,evaluate,assert,waitFor}) => {
  await send('browsingContext.setViewport',{context,viewport:{width:1440,height:1000}})
  await send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/dailySalience.html',wait:'complete'})
  await waitFor("document.querySelectorAll('.measure-daily-efficiency-point').length===5")
  const dot="document.querySelector('[data-date=\"2024-01-03\"]')"
  await assert(`getComputedStyle(${dot}).r==='2px' && getComputedStyle(${dot}).stroke==='rgba(0, 0, 0, 0)'`)
  const point=JSON.parse(await evaluate(`JSON.stringify((()=>{const r=${dot}.getBoundingClientRect();return {x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())`))
  await send('input.performActions',{context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[{type:'pointerMove',origin:'viewport',...point}]}]})
  await waitFor(`getComputedStyle(${dot}).r==='4px' && getComputedStyle(${dot}).stroke==='rgb(255, 255, 255)'`)
  await send('input.performActions',{context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[{type:'pointerMove',origin:'viewport',x:1,y:1}]}]})
  await waitFor(`getComputedStyle(${dot}).r==='2px'`)
  await evaluate(`${dot}.focus()`)
  await waitFor(`getComputedStyle(${dot}).r==='4px'`)
  await send('input.performActions',{context,actions:[{type:'key',id:'keyboard',actions:[{type:'keyDown',value:'\uE007'},{type:'keyUp',value:'\uE007'}]}]})
  await assert("salienceTest.selections.length===1 && salienceTest.selections[0].date==='2024-01-03'")
  await evaluate(`${dot}.blur();salienceTest.show('dense')`)
  await waitFor("document.querySelectorAll('.measure-daily-efficiency-point').length===3000")
  await assert("[...document.querySelectorAll('.measure-daily-efficiency-point')].every(n=>n.getAttribute('r')==='2')")
  await assert("salienceTest.requests.every(r=>r.method==='GET' && r.path.includes('/analysis/measures/daily-'))")
  console.log('PASS: small accuracy dots, hover/focus circles, Enter drilldown, dense series; every API request mocked.')
})
