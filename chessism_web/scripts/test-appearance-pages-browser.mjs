// Real page, synthetic large responses; never reaches the API or Google Cloud.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-all-pages-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile,
  '--remote-debugging-port', '9257'], { stdio: ['ignore', 'pipe', 'pipe'] })
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
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const assert = async expression => {
    if (!await evaluate(expression)) throw new Error('Failed: ' + expression+'\n'+await evaluate('JSON.stringify(window.appearancePagesTest)'))
  }
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) {
      if (await evaluate(`Boolean(${expression})`)) return
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw new Error('Timeout: ' + expression + '\n' + await evaluate(`JSON.stringify({errors:window.appearancePagesTest?.errors,requests:window.appearancePagesTest?.requests,body:document.body.innerText.slice(0,3000)})`))
  }


  const legacy=JSON.parse(await readFile(new URL('../../chessism_api/appearance_registry.json',import.meta.url),'utf8'))
  const extension=JSON.parse(await readFile(new URL('../../chessism_api/appearance_scopes.json',import.meta.url),'utf8'))
  const charts=JSON.parse(await readFile(new URL('../../chessism_api/appearance_charts.json',import.meta.url),'utf8'))
  const registry={...legacy,properties:{...legacy.properties,...charts.properties},scopes:Object.fromEntries(Object.entries(extension.scopes).map(([scope,spec])=>[scope,{...spec,targets:{...extension.common_targets,...charts.targets,...spec.targets,...(scope===legacy.scope?legacy.targets:{})}}]))}
  await send('script.addPreloadScript',{functionDeclaration:`function(){
    const registry=${JSON.stringify(registry)};
    const state=window.appearancePagesTest={requests:[],errors:[]};
    window.addEventListener('error',e=>state.errors.push(e.message));
    window.addEventListener('unhandledrejection',e=>state.errors.push(String(e.reason)));
    window.prompt=()=> 'All pages test';window.confirm=()=>true;
    window.fetch=async(input,options={})=>{
      const url=new URL(input,location.href),path=url.pathname.replace(/^\\/api/,'');
      const method=options.method||'GET';state.requests.push({path,method});
      const reply=(data,status=200)=>new Response(JSON.stringify(data),{status,headers:{'Content-Type':'application/json'}});
      const saved=JSON.parse(localStorage.getItem('appearance-pages-test')||'null')||{revision:0,presets:[],active_preset_id:null,overrides:[],registry};
      if(path==='/auth/me')return reply({account:{id:'test-admin',name:'Test Admin',role:'admin'}});
      if(path==='/appearance')return reply(saved);
      if(path==='/research/coefficient/overview')return reply({totals:{},modes:[]});
      if(method==='POST'&&path==='/appearance/save'){
        const payload=JSON.parse(options.body);
        const next={...saved,revision:saved.revision+1,active_preset_id:'test-preset',presets:[{id:'test-preset',name:'All pages test'}],overrides:payload.overrides};
        localStorage.setItem('appearance-pages-test',JSON.stringify(next));return reply(next);
      }
      if(method!=='GET'){state.errors.push('Unexpected write '+path);throw Error('No job writes allowed in UI test')}
      return reply({detail:'Synthetic offline test response'},503);
    };
  }`})
  const load=route=>send('browsingContext.navigate',{context,wait:'complete',url:'http://localhost:6789'+route})
  const click=text=>evaluate(`Array.from(document.querySelectorAll('button')).find(b=>b.textContent===${JSON.stringify(text)}).click()`)
  const setColor=(property,color)=>evaluate(`(()=>{const n=document.querySelector('#appearance-'+${JSON.stringify(property)});Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,${JSON.stringify(color)});n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  for(const [scope,spec] of Object.entries(extension.scopes)){
    if(!spec.routes.length)continue
    await load(spec.routes[0])
    await waitFor(`document.querySelector('.appearance-account-controls button')?.disabled===false&&document.querySelector('[data-style-id="page"][data-style-scope="${scope}"]')`)
    await click('Edit appearance')
    const selector=`[data-style-id="page"][data-style-scope="${scope}"]`
    await evaluate(`document.querySelector(${JSON.stringify(selector)}).dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:100,clientY:100}))`)
    await waitFor(`document.querySelector('.appearance-editor-body small')?.textContent.startsWith(${JSON.stringify(spec.label)})`)
    await setColor('background','#132435')
    await waitFor(`getComputedStyle(document.querySelector(${JSON.stringify(selector)})).backgroundColor==='rgb(19, 36, 53)'`)
    await click('Save styles')
    await waitFor(`!document.querySelector('.appearance-account-controls').textContent.includes('*')`)
    await evaluate(`document.querySelector('[aria-label="Close appearance popup"]').click()`)
    const child=`[data-style-scope="${scope}"][data-style-id="heading"],[data-style-scope="${scope}"][data-style-id="title"],[data-style-scope="${scope}"][data-style-id="metric.title"]`
    await evaluate(`document.querySelector(${JSON.stringify(child)}).dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:120,clientY:150}))`)
    await waitFor(`document.querySelector('#appearance-color')`)
    await setColor('color','#abccde')
    await waitFor(`getComputedStyle(document.querySelector(${JSON.stringify(child)})).color==='rgb(171, 204, 222)'`)
    await click('Save styles');await waitFor(`!document.querySelector('.appearance-account-controls').textContent.includes('*')`)
    await assert(`window.appearancePagesTest.errors.length===0`)
    await assert(`window.appearancePagesTest.requests.every(r=>r.method==='GET'||['/appearance/save','/research/coefficient/overview'].includes(r.path))`)
    console.log('PASS:',spec.label,'page and text edits, scoped save, no job writes.')
  }
  // Draft styles saved on every page must coexist in the same preset.
  await load('/analize_positions')
  await waitFor(`document.querySelector('.appearance-account-controls button')?.disabled===false`)
  await assert(`getComputedStyle(document.querySelector('[data-style-id="page"]')).backgroundColor==='rgb(19, 36, 53)'`)
  await click('Edit appearance')
  await evaluate(`document.querySelector('#analysis-gcloud-toggle').click()`)
  await waitFor(`document.querySelector('.cloud-selection input:not([type="checkbox"])')`)
  await evaluate(`document.querySelector('.cloud-selection input:not([type="checkbox"])').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:120,clientY:150}))`)
  await waitFor(`document.querySelector('.appearance-editor header strong')?.textContent==='Form fields on this page'`)
  await setColor('background','#38495a')
  await waitFor(`getComputedStyle(document.querySelector('.cloud-selection input:not([type="checkbox"])')).backgroundColor==='rgb(56, 73, 90)'`)
  await assert(`window.appearancePagesTest.requests.every(r=>r.method==='GET'||r.path==='/appearance/save')`)
  // Do not navigate away with the final unsaved test change.
  await click('Save styles');await waitFor(`!document.querySelector('.appearance-account-controls').textContent.includes('*')`)
  await assert(`JSON.parse(localStorage.getItem('appearance-pages-test')).overrides.filter(row=>row.target==='page').length===16`)
  console.log('PASS: cross-page preset persistence and Analyze Positions cloud form editing without submitting work.')
  await send('browsingContext.setViewport',{context,viewport:{width:1400,height:1000}})
  await load('/')
  await waitFor("document.querySelector('.appearance-account-controls button')?.disabled===false")
  await assert("Math.abs(document.querySelector('.home-overview').getBoundingClientRect().top-3*parseFloat(getComputedStyle(document.documentElement).fontSize))<1")
  await click('Edit appearance')
  const openPanel=selector=>evaluate(`document.querySelector(${JSON.stringify(selector)}).dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:120,clientY:150}))`)
  const setGap=value=>evaluate(`(()=>{const n=document.querySelector('#appearance-related-homeTopMargin');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,${JSON.stringify(String(value))});n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  await openPanel('.dashboard-panel')
  await waitFor("document.querySelector('#appearance-related-homeTopMargin')?.value==='3'")
  await assert("document.querySelector('#appearance-related-homeTopMargin').step==='0.25'")
  await setGap(4.5)
  await waitFor("Math.abs(document.querySelector('.home-overview').getBoundingClientRect().top-4.5*parseFloat(getComputedStyle(document.documentElement).fontSize))<1")
  await click('Save styles');await waitFor("!document.querySelector('.appearance-account-controls').textContent.includes('*')")
  await load('/')
  await waitFor("document.querySelector('.appearance-account-controls button')?.disabled===false && getComputedStyle(document.querySelector('.home-overview')).getPropertyValue('--home-top-gap')==='4.5rem'")
  await click('Edit appearance')
  await openPanel('.main-characters-carousel')
  await waitFor("document.querySelector('#appearance-related-homeTopMargin')?.value==='4.5'")
  await setGap(1)
  await waitFor("document.querySelector('#appearance-related-homeTopMargin')?.value==='1'")
  await evaluate("document.querySelector('[aria-label=\"Restore saved top margin (rem)\"]').click()")
  await waitFor("document.querySelector('#appearance-related-homeTopMargin')?.value==='4.5' && !document.querySelector('.appearance-account-controls').textContent.includes('*')")
  await assert("window.appearancePagesTest.errors.length===0 && window.appearancePagesTest.requests.every(r=>r.method==='GET'||r.path==='/appearance/save')")
  console.log('PASS: homepage 3rem top gap; editable from either first-row panel; fractional rem values; save/reload and restore to saved spacing.')
} finally {
 socket?.close();browser.kill('SIGTERM')
 await Promise.race([once(browser,'exit'),new Promise(resolve=>setTimeout(resolve,3000))])
 await rm(temporary,{recursive:true,force:true})
}
