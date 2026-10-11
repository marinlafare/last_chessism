// Real page, synthetic large responses; never reaches the API or Google Cloud.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-appearance-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile,
  '--remote-debugging-port', '9256'], { stdio: ['ignore', 'pipe', 'pipe'] })
let socket
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('Browser startup timeout')), 15000)
    const output = chunk => {
      const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/)
      if (match) { clearTimeout(timer); resolve(match[1]) }
    }
    browser.stdout.on('data', output); browser.stderr.on('data', output)
    browser.once('error', error => { clearTimeout(timer); reject(error) })
  })
  socket = new WebSocket(endpoint.replace(/\/$/, '') + '/session')
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject })
  const requests = new Map(); let nextId = 0
  socket.onmessage = ({ data }) => {
    const response = JSON.parse(data), pending = requests.get(response.id)
    if(response.method==='log.entryAdded'&&response.params.level==='error')console.error(response.params.text)
    if (pending) {
      requests.delete(response.id); clearTimeout(pending.timer)
      response.type === 'error' ? pending.reject(new Error(JSON.stringify(response))) : pending.resolve(response.result)
    }
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId
    const timer = setTimeout(() => { requests.delete(id); reject(new Error(`Timeout: ${method}`)) }, 45000)
    requests.set(id, { resolve, reject, timer }); socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  await send('session.subscribe', {events:['log.entryAdded']})
  const { context } = await send('browsingContext.create', { type: 'tab' })
  const charts=JSON.parse(await readFile(new URL('../../chessism_api/appearance_charts.json',import.meta.url),'utf8'))
  await send('script.addPreloadScript',{functionDeclaration:`function(){window.appearanceTestCharts=${JSON.stringify(charts)}}`})
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const assert = async expression => {
    if (!await evaluate(expression)) throw new Error('Failed: ' + expression)
  }
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) {
      if (await evaluate(`Boolean(${expression})`)) return
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw new Error('Timeout: ' + expression + '\n' + await evaluate(`JSON.stringify({errors:window.appearanceTest?.errors,requests:window.appearanceTest?.requests,body:document.body.innerText.slice(0,3000)})`))
  }

  const load=role=>send('browsingContext.navigate',{context,wait:'complete',url:'http://localhost:6789/tests/fixtures/appearance.html?role='+role})
  const click=text=>evaluate(`Array.from(document.querySelectorAll('button')).find(b=>b.textContent===${JSON.stringify(text)}).click()`)
  const color=value=>evaluate(`(()=>{const n=document.querySelector('#appearance-background');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,${JSON.stringify(value)});n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  const open=()=>evaluate(`document.querySelector('#editable').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:80,clientY:80}))`)
  await load('admin')
  await waitFor(`document.querySelector('.appearance-account-controls button')?.disabled===false`)
  await assert(`!document.querySelector('.appearance-editor')&&!performance.getEntriesByType('resource').some(r=>r.name.includes('AppearanceEditor.jsx'))`)
  await click('Edit appearance');await open()
  await waitFor(`document.querySelector('#appearance-background')`)
  await color('#123456')
  await waitFor(`getComputedStyle(document.querySelector('#editable')).backgroundColor==='rgb(18, 52, 86)'`)
  await assert(`getComputedStyle(document.querySelector('#outside')).backgroundColor==='rgb(20, 30, 40)'&&window.appearanceTest.requests.every(r=>r.method==='GET')`)
  await click('Save styles')
  await waitFor(`window.appearanceTest.data.revision===1&&!document.querySelector('.appearance-account-controls').textContent.includes('*')`)
  await assert(`window.appearanceTest.prompts===1&&window.appearanceTest.data.presets.length===1&&window.appearanceTest.requests.at(-1).payload.preset_id===null&&document.querySelector('[aria-label="Style preset"] option').textContent==='Default'`)
  await color('#abcdef');await click('Save styles')
  await waitFor(`window.appearanceTest.data.revision===2`)
  await assert(`window.appearanceTest.prompts===1&&window.appearanceTest.data.presets.length===1&&window.appearanceTest.requests.at(-1).payload.preset_id==='preset-1'`)
  await load('admin');await waitFor(`getComputedStyle(document.querySelector('#editable')).backgroundColor==='rgb(171, 205, 239)'`)
  await assert(`!document.querySelector('.appearance-editor')&&document.querySelector('.appearance-account-controls button').getAttribute('aria-pressed')==='false'`)
  await click('Edit appearance');await open();await waitFor(`document.querySelector('#appearance-background')`)
  await color('#ffffff')
  await evaluate(`document.querySelector('.appearance-editor header button').click()`)
  await waitFor(`getComputedStyle(document.querySelector('#editable')).backgroundColor==='rgb(171, 205, 239)'`)
  await color('#010203')
  await evaluate(`window.appearanceTest.fail=true`);await click('Save styles')
  await waitFor(`document.querySelector('[role="alert"]')?.textContent.includes('another session')`)
  await assert(`getComputedStyle(document.querySelector('#editable')).backgroundColor==='rgb(1, 2, 3)'&&document.querySelector('.appearance-account-controls').textContent.includes('*')`)
  await evaluate(`window.appearanceTest.confirm=false`);await click('Exit appearance editing *')
  await assert(`document.querySelector('.appearance-editor')&&document.querySelector('[aria-label="Style preset"]')`)
  await evaluate(`window.appearanceTest.fail=false;window.appearanceTest.confirm=true`);await click('Exit appearance editing *')
  await waitFor(`!document.querySelector('.appearance-editor')&&getComputedStyle(document.querySelector('#editable')).backgroundColor==='rgb(171, 205, 239)'`)
  await click('Edit appearance')
  await evaluate(`(()=>{const s=document.querySelector('[aria-label="Style preset"]');s.value='';s.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  await waitFor(`window.appearanceTest.data.active_preset_id===null&&getComputedStyle(document.querySelector('#editable')).backgroundColor==='rgb(20, 30, 40)'`)
  await assert(`window.appearanceTest.data.presets.length===1&&window.appearanceTest.data.overrides.length===0`)
  await open();await waitFor(`document.querySelector('.appearance-editor')`)
  await send('browsingContext.setViewport',{context,viewport:{width:390,height:844}})
  await assert(`document.querySelector('.appearance-editor').getBoundingClientRect().right<=innerWidth&&document.querySelector('.appearance-editor').getBoundingClientRect().bottom<=innerHeight`)
  await evaluate(`document.querySelector('[aria-label="Close appearance popup"]').click()`)
  await assert(`document.querySelector('#outside').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true}))===true`)
  await assert(`document.querySelector('#editable').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,shiftKey:true}))===true`)
  await assert(`window.appearanceTest.errors.length===0`)

  // Real pointer gestures: resize edges/corners, drag header, scroll hidden body.
  await send('browsingContext.setViewport',{context,viewport:{width:1200,height:900}})
  await open();await waitFor(`document.querySelector('.appearance-editor')`)
  const rectangle=()=>evaluate(`JSON.stringify((()=>{const r=document.querySelector('.appearance-editor').getBoundingClientRect();return {left:r.left,top:r.top,width:r.width,height:r.height}})())`).then(JSON.parse)
  const gesture=async(selector,dx,dy)=>{
    const point=JSON.parse(await evaluate(`JSON.stringify((()=>{const r=document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect();return {x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())`))
    await send('input.performActions',{context,actions:[{type:'pointer',id:'editor-mouse',parameters:{pointerType:'mouse'},actions:[
      {type:'pointerMove',x:point.x,y:point.y,origin:'viewport'},{type:'pointerDown',button:0},
      {type:'pointerMove',x:point.x+dx,y:point.y+dy,origin:'viewport',duration:200},{type:'pointerUp',button:0}]}]})
  }
  let before=await rectangle()
  await gesture('[data-resize-edge="e"]',120,0)
  await assert(`Math.abs(document.querySelector('.appearance-editor').getBoundingClientRect().width-${before.width+120})<2`)
  before=await rectangle();await gesture('[data-resize-edge="sw"]',-30,60)
  await assert(`Math.abs(document.querySelector('.appearance-editor').getBoundingClientRect().height-${before.height+60})<2&&Math.abs(document.querySelector('.appearance-editor').getBoundingClientRect().left-${before.left-30})<2`)
  before=await rectangle();await gesture('.appearance-editor header strong',20,20)
  await assert(`Math.abs(document.querySelector('.appearance-editor').getBoundingClientRect().left-${before.left+20})<2`)
  await assert(`getComputedStyle(document.querySelector('.appearance-editor-body')).scrollbarWidth==='none'`)
  before=await rectangle();await gesture('[data-resize-edge="s"]',0,180-before.height)
  const wheelPoint=JSON.parse(await evaluate(`JSON.stringify((()=>{const r=document.querySelector('.appearance-editor-body').getBoundingClientRect();return {x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())`))
  await send('input.performActions',{context,actions:[{type:'wheel',id:'editor-wheel',actions:[{type:'scroll',origin:'viewport',...wheelPoint,deltaX:0,deltaY:600,duration:200}]}]})
  await waitFor(`document.querySelector('.appearance-editor-body').scrollTop>0`)
  await color('#102030')
  await evaluate(`document.querySelector('#outside').dispatchEvent(new PointerEvent('pointerdown',{button:2,bubbles:true}))`)
  await assert(`!!document.querySelector('.appearance-editor')`)
  await evaluate(`document.querySelector('#outside').dispatchEvent(new PointerEvent('pointerdown',{button:0,bubbles:true}))`)
  await waitFor(`!document.querySelector('.appearance-editor')`)
  await assert(`document.querySelector('.appearance-account-controls button').getAttribute('aria-pressed')==='true'&&getComputedStyle(document.querySelector('#editable')).backgroundColor==='rgb(16, 32, 48)'`)
  await open();await waitFor(`document.querySelector('#appearance-background')?.value==='#102030'`)
  await click('Save styles');await waitFor(`!document.querySelector('.appearance-account-controls').textContent.includes('*')`)
  console.log('PASS: border/corner pointer resize, header drag, scrolling without a scrollbar, outside left-click dismissal and draft preservation.')

  // Use real SVG components: the palette changes marks, not geometry or data.
  for(const [target,mark] of [['chart.pending','.repetition-bar'],['chart.research','rect']]){
    const selector='[data-style-id="'+target+'"]'
    await evaluate(`document.querySelector(${JSON.stringify(selector)}).dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:100,clientY:100}))`)
    await waitFor(`document.querySelector('#appearance-chartColor1')`)
    await evaluate(`(()=>{const n=document.querySelector('#appearance-chartColor1');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,'#ab1234');n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}))})()`)
    await waitFor(`getComputedStyle(document.querySelector(${JSON.stringify(selector+' '+mark)})).fill==='rgb(171, 18, 52)'`)
    await click('Save styles');await waitFor(`!document.querySelector('.appearance-account-controls').textContent.includes('*')`)
  }
  await load('admin');await waitFor(`document.querySelector('.repetition-bar')&&getComputedStyle(document.querySelector('.repetition-bar')).fill==='rgb(171, 18, 52)'`)
  await assert(`window.appearanceTest.errors.length===0`)
  console.log('PASS: real histogram and research chart colors update live and survive save/reload.')
  await load('user')
  await waitFor(`window.appearanceTest?.requests.length>0`)
  await assert(`!document.querySelector('.appearance-account-controls')&&!document.querySelector('.appearance-editor')&&window.appearanceTest.requests.every(r=>r.method==='GET')`)
  console.log('PASS: immutable Default, named save/update/reload, scoped live preview, restore, conflict-preserved drafts, discard confirmation, native menu, mobile bounds and admin-only tools.')
} finally {
  socket?.close();browser.kill('SIGTERM')
  await Promise.race([once(browser,'exit'),new Promise(resolve=>setTimeout(resolve,3000))])
  await rm(temporary,{recursive:true,force:true})
}
