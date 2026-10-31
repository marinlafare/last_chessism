import { browserFixture } from './browser-fixture.mjs'
await browserFixture(9301,async({send,context,evaluate,assert,waitFor})=>{
 await send('browsingContext.setViewport',{context,viewport:{width:1400,height:1000}})
 await send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/dateChartNavigation.html',wait:'complete'})
 await waitFor("document.querySelectorAll('.measure-daily-efficiency-point').length===6")
 await evaluate(`document.querySelector('[data-style-id="player.rating.toggle"]').click()`)
 await waitFor("document.querySelectorAll('.behavior-rating-point').length===1")
 await evaluate("window.initialRequests=calendarTest.requests.length")
 const rejectPointSelection=async()=>{
  await evaluate(`document.querySelector('.measure-daily-efficiency-point').dispatchEvent(new MouseEvent('click',{bubbles:true}));
   document.querySelector('.measure-daily-efficiency-point').dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',bubbles:true}))`)
  await assert("calendarTest.selections.length===0 && [...document.querySelectorAll('.measure-daily-efficiency-point')].every(p=>p.getAttribute('role')==='img'&&!p.hasAttribute('tabindex'))")
 }
 await rejectPointSelection()
 for(const chart of ['.measure-daily-efficiency-graph','.behavior-graph']){
  const controls=chart.startsWith('.measure')?'.measure-daily-years':'.behavior-rating-years'
  await evaluate(`document.querySelector('${chart} [data-year="2024"]').dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',bubbles:true}))`)
  await waitFor(`document.querySelector('${chart}').dataset.dateZoom==='year'`)
  if(chart.startsWith('.measure'))await rejectPointSelection()
  await assert(`document.querySelectorAll('${chart} [data-month]').length===3 && !document.querySelector('${chart} [data-month="2024-04"]')`)
  await assert(`document.querySelector('${chart}').textContent.includes('Feb')`)
  await evaluate(`document.querySelector('${chart} [data-month="2024-02"]').dispatchEvent(new MouseEvent('click',{bubbles:true}))`)
  await waitFor(`document.querySelector('${chart}').dataset.dateZoom==='month'`)
  await assert(`document.querySelector('${chart}').getAttribute('aria-label').includes('Feb 2024') && !document.querySelector('${chart} [data-month]')`)
  await assert(`document.activeElement===document.querySelector('${controls} [aria-label="Show all of 2024"]')`)
  if(chart.startsWith('.measure')){
   await assert("[...document.querySelectorAll('.measure-daily-efficiency-point')].length===3 && [...document.querySelectorAll('.measure-daily-efficiency-point')].every(p=>p.dataset.date.startsWith('2024-02')&&p.getAttribute('r')==='4')")
   await assert("[...document.querySelectorAll('.measure-daily-x-tick')].at(-1).textContent==='29'")
   await evaluate("document.querySelector('.measure-daily-efficiency-point').dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',bubbles:true}))")
   await assert("calendarTest.selections[0].date==='2024-02-01'")
   await evaluate("document.querySelector('[data-salience-handle=\"0\"]').dispatchEvent(new KeyboardEvent('keydown',{key:'End',bubbles:true}))")
   await waitFor("document.querySelectorAll('.measure-daily-efficiency-point').length===1")
   await assert("document.querySelector('.measure-daily-efficiency-point').dataset.date==='2024-02-01' && document.querySelector('.measure-daily-efficiency-graph').dataset.dateZoom==='month'")
   await evaluate("document.querySelector('[data-salience-handle=\"0\"]').dispatchEvent(new KeyboardEvent('keydown',{key:'Home',bubbles:true}))")
  }else{
   await assert("document.querySelector('.behavior-rating-point').dataset.pointRadius==='4' && document.querySelector('.behavior-rating-point').getAttribute('d').split('M').length===3")
  }
  await evaluate(`document.querySelector('${controls} [aria-label="Show all of 2024"]').click()`)
  await waitFor(`document.querySelector('${chart}').dataset.dateZoom==='year'`)
  await evaluate(`document.querySelector('${chart} [data-month="2024-01"]').dispatchEvent(new KeyboardEvent('keydown',{key:' ',bubbles:true}))`)
  await waitFor(`document.querySelector('${chart}').dataset.dateZoom==='month'`)
  await assert(`document.querySelector('${chart}').getAttribute('aria-label').includes('Jan 2024')`)
  await evaluate(`document.querySelector('${controls} button').click()`)
  await waitFor(`document.querySelector('${chart}').dataset.dateZoom==='all'`)
 }
 await assert("document.querySelector('.measure-daily-efficiency-point').getAttribute('r')==='2' && document.querySelector('.behavior-rating-point').dataset.pointRadius==='1.8'")
 await assert("calendarTest.requests.length===initialRequests && calendarTest.requests.every(r=>r.method==='GET')")
 // Player changes reset a previously zoomed chart; no stale date range is carried over.
 await evaluate("document.querySelector('.measure-daily-efficiency-graph [data-year=\"2024\"]').dispatchEvent(new MouseEvent('click',{bubbles:true}))")
 await waitFor("document.querySelector('.measure-daily-efficiency-graph').dataset.dateZoom==='year'")
 await evaluate("calendarTest.show('bob')")
 await waitFor("document.querySelector('.measure-daily-efficiency-graph')?.dataset.dateZoom==='all'")
 console.log('PASS: year/month drill-down, leap-day and single-day views, larger points, breadcrumbs, keyboard/mouse, salience preservation, game selection and no drill-down requests')
})
