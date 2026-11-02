import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9351, async ({send,context,evaluate,assert,waitFor})=>{
  await send('browsingContext.navigate',{context,wait:'complete',url:'http://localhost:6789/tests/fixtures/graphSync.html'})
  await waitFor(`document.querySelector('progress')`)
  await assert(`document.querySelector('progress').value===125 && document.querySelector('progress').max===5000`)
  await assert(`document.querySelector('[data-style-id="graph.panel"]').dataset.styleScope==='system_status'`)
  await assert(`document.querySelector('[role="status"]').textContent==='retrying'`)
  await evaluate(`document.querySelector('[data-style-id="graph.retry"]').click()`)
  await waitFor(`document.querySelector('[role="status"]').textContent==='current'`)
  await assert(`graphTest.retry===1 && !document.querySelector('[data-style-id="graph.retry"]') && graphTest.errors.length===0`)
  await assert(`graphTest.requests.every(path=>path.endsWith('/graph-sync') || path.endsWith('/graph-sync/retry'))`)
  console.log('PASS: graph progress, retry, polling, independent style targets; all API requests mocked.')
})
