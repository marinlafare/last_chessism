// Optional read-only snapshot visual check. The browser never contacts live APIs.
import {readFile,writeFile} from 'node:fs/promises'
import {browserFixture} from './browser-fixture.mjs'
const snapshot=JSON.parse(await readFile(process.argv[2],'utf8'))
await browserFixture(9358,async({send,context,evaluate,waitFor,assert})=>{
  await send('browsingContext.navigate',{context,wait:'complete',url:'http://localhost:6789/tests/fixtures/graphExplorer.html'})
  await waitFor(`window.graphTest&&document.querySelector('.common-positions-toggle')`)
  await evaluate(`graphTest.snapshot=${JSON.stringify(snapshot)};document.querySelector('.common-positions-toggle').click()`)
  const start=Date.now()
  // Includes the actual worker and renderer, not merely a unit-test layout.
  for(let i=0;i<6;i++){
    try{await waitFor(`document.querySelector('[data-map-node]')`);break}catch(error){if(i===5)throw error}
  }
  await assert(`graphTest.errors.length===0`)
  await send('browsingContext.setViewport',{context,viewport:{width:1440,height:1050}})
  const image=await send('browsingContext.captureScreenshot',{context,origin:'viewport'})
  await writeFile('/tmp/chessism-opening-map-preview.png',Buffer.from(image.data,'base64'))
  console.log(JSON.stringify({render_ms:Date.now()-start,ui:await evaluate(`document.querySelector('.opening-map-hover').textContent`),renderer:await evaluate(`document.querySelector('.opening-map-canvas canvas')?'WebGL':'SVG fallback'`)}))
})
