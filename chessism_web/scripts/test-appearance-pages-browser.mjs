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
      if(path==='/database/schema')return reply({tables:[],relationships:[],generated_at:'2026-10-11T00:00:00Z'});
      if(path==='/research/coefficient/overview')return reply({totals:{},modes:[]});
      if(path==='/players/main_characters/top')return reply({players:[{player_name:'hikaru',full_name:'Hikaru',rating:3200}]});
      if(path==='/players/hikaru')return reply({player_name:'hikaru',name:'Hikaru',joined:1700000000,country:'US',followers:100});
      if(path==='/games/hikaru/hours_played')return reply({bullet_games:30,bullet_hours:2,blitz_games:20,blitz_hours:3,rapid_games:10,rapid_hours:4});
      if(path==='/fens/players/hikaru/analysis_counts')return reply({total_games:60,analyzed_games:50,total_fens:3000,analyzed_fens:2500});
      if(path==='/players/hikaru/neighbors')return reply({previous_player:null,next_player:null});
      if(path==='/players/hikaru/analysis/behavioural/activity')return reply({hours:[],weekdays:[],weekday_hours:[]});
      if(path==='/players/hikaru/analysis/behavioural/ratings')return reply({series:[]});
      if(path==='/players/hikaru/analysis/measures/quality-calendar')return reply({coverage:{},hours:[],weekdays:[],weekday_hours:[]});
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
    if(scope==='database'){
      await waitFor(`document.querySelector('.database-canvas')`)
      await assert(`document.querySelector('.rail-btn[href="/database"]').classList.contains('active')`)
      await assert(`window.appearancePagesTest.requests.every(r=>['/auth/me','/appearance','/appearance/save','/database/schema'].includes(r.path))`)
      console.log('PASS: Database canvas, editable page background, active sidebar link and metadata-only request.')
      continue
    }
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
  await assert(`JSON.parse(localStorage.getItem('appearance-pages-test')).overrides.filter(row=>row.target==='page').length===${Object.values(extension.scopes).filter(spec=>spec.routes.length).length}`)
  console.log('PASS: cross-page preset persistence and Analyze Positions cloud form editing without submitting work.')
  await send('browsingContext.setViewport',{context,viewport:{width:1400,height:1000}})
  await load('/')
  await waitFor("document.querySelector('.appearance-account-controls button')?.disabled===false")
  await waitFor("Math.abs(document.querySelector('.home-overview').getBoundingClientRect().top-3*parseFloat(getComputedStyle(document.documentElement).fontSize))<1")
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
  await load('/?player=hikaru')
  await waitFor("document.querySelector('.profile-item strong')?.textContent==='hikaru' && document.querySelector('.appearance-account-controls button')?.disabled===false")
  await click('Edit appearance')
  const untouchedHeroColors = await evaluate("JSON.stringify(['.players-top-grid','.players-position-stat','.players-followers-card','.players-title-arrow'].map(s=>getComputedStyle(document.querySelector(s)).backgroundColor))")
  await openPanel('.players-profile-section')
  await waitFor("document.querySelector('.appearance-editor header strong')?.textContent==='Player profile' && document.querySelector('#appearance-background')")
  await setColor('background','#334455')
  await waitFor("document.querySelector('#appearance-background')?.value==='#334455'")
  await evaluate("(()=>{const n=document.querySelector('[aria-label=\"Background opacity\"]');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,'100');n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}))})()")
  await waitFor("['.players-profile-section','.players-profile-navigation-section'].every(s=>getComputedStyle(document.querySelector(s)).backgroundColor==='rgb(51, 68, 85)')")
  await assert(`JSON.stringify(['.players-top-grid','.players-position-stat','.players-followers-card','.players-title-arrow'].map(s=>getComputedStyle(document.querySelector(s)).backgroundColor))===${JSON.stringify(untouchedHeroColors)}`)
  await click('Save styles')
  await waitFor("!document.querySelector('.appearance-account-controls').textContent.includes('*')")
  await load('/?player=hikaru')
  await waitFor("document.querySelector('.profile-item strong')?.textContent==='hikaru' && ['.players-profile-section','.players-profile-navigation-section'].every(s=>getComputedStyle(document.querySelector(s)).backgroundColor==='rgb(51, 68, 85)')")
  await click('Edit appearance')
  await openPanel('.players-profile-navigation-section')
  await waitFor("document.querySelector('#appearance-background')?.value==='#334455'")
  await setColor('background','#abcdef')
  await evaluate("document.querySelector('[aria-label=\"Restore saved background\"]').click()")
  await waitFor("document.querySelector('#appearance-background')?.value==='#334455' && !document.querySelector('.appearance-account-controls').textContent.includes('*')")
  await assert("window.appearancePagesTest.errors.length===0 && window.appearancePagesTest.requests.every(r=>r.method==='GET'||r.path==='/appearance/save')")
  console.log('PASS: both gray player-profile surfaces expose Background, update together, persist on reload, restore saved color, and leave outer/data/button colors unchanged.')
  // An old shared button color becomes independent initial values, not live links.
  await evaluate("(()=>{const data=JSON.parse(localStorage.getItem('appearance-pages-test'));data.overrides.push({scope:'home',target:'button',property:'background',value:'#245634'},{scope:'home',target:'value',property:'color',value:'#366699'});localStorage.setItem('appearance-pages-test',JSON.stringify(data))})()")
  await load('/?player=hikaru')
  await waitFor("document.querySelector('.players-title-select')?.disabled===false && document.querySelector('.appearance-account-controls button')?.disabled===false")
  await click('Edit appearance')
  const buttonSelector=id=>`[data-style-scope="home"][data-style-id="${id}"]`
  const allControls=Object.entries(extension.scopes.home.targets).filter(([,spec])=>spec.initialStyleFrom==='button').map(([id])=>id)
  for(const id of ['player.analysis.toggle','player.measures.toggle']) {
    await evaluate(`document.querySelector(${JSON.stringify(buttonSelector(id))}).click()`)
  }
  await waitFor("document.querySelector('[data-style-id=\"player.qualityHours.toggle\"]') && document.querySelector('[data-style-id=\"player.rating.toggle\"]')")
  await assert(`${JSON.stringify(allControls)}.every(id=>getComputedStyle(document.querySelector('[data-style-id="'+id+'"]')).backgroundColor==='rgb(36, 86, 52)')`)
  const independentColors = {}
  for (const [index,id] of allControls.entries()) {
    const value='#'+(0x334455+index*0x010101).toString(16)
    await openPanel(buttonSelector(id))
    await waitFor(`document.querySelector('.appearance-editor header strong')?.textContent===${JSON.stringify(extension.scopes.home.targets[id].label)}`)
    await setColor('background',value)
    const expected=`rgb(${[1,3,5].map(offset=>parseInt(value.slice(offset,offset+2),16)).join(', ')})`
    await waitFor(`getComputedStyle(document.querySelector(${JSON.stringify(buttonSelector(id))})).backgroundColor===${JSON.stringify(expected)}`)
    independentColors[id]=expected
    await assert(`${JSON.stringify(allControls)}.every(id=>getComputedStyle(document.querySelector('[data-style-id="'+id+'"]')).backgroundColor===(${JSON.stringify(independentColors)}[id]||'rgb(36, 86, 52)'))`)
  }
  // Right-clicking the two action buttons must edit separate controls, without
  // invoking either their update or delete handler (all writes remain mocked).
  for(const [id,color] of [['player.update','#12ab34'],['player.delete','#ab1234']]){
    await openPanel(buttonSelector(id))
    await waitFor(`document.querySelector('.appearance-editor header strong')?.textContent===${JSON.stringify(extension.scopes.home.targets[id].label)}`)
    await setColor('color',color)
    await setColor('borderColor',color)
    await evaluate("(()=>{const n=document.querySelector('[aria-label=\"Border color opacity\"]');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,'100');n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}))})()")
  }
  const actionColors="['player.update','player.delete'].every((id,i)=>{const style=getComputedStyle(document.querySelector('[data-style-id=\"'+id+'\"]')),color=['rgb(18, 171, 52)','rgb(171, 18, 52)'][i];return style.color===color && style.borderTopColor===color})"
  await waitFor(actionColors)
  await click('Save styles'); await waitFor("!document.querySelector('.appearance-account-controls').textContent.includes('*')")
  // Changing a formerly shared saved style must not recolor the split buttons.
  await evaluate("(()=>{const data=JSON.parse(localStorage.getItem('appearance-pages-test'));data.overrides=data.overrides.map(row=>row.scope==='home' && row.target==='button' && row.property==='background'?{...row,value:'#aabbcc'}:row);localStorage.setItem('appearance-pages-test',JSON.stringify(data))})()")
  await load('/?player=hikaru')
  await waitFor("document.querySelector('.players-title-select')?.disabled===false && document.querySelector('.appearance-account-controls button')?.disabled===false")
  for(const id of ['player.analysis.toggle','player.measures.toggle'])await evaluate(`document.querySelector(${JSON.stringify(buttonSelector(id))}).click()`)
  await waitFor("document.querySelector('[data-style-id=\"player.qualityHours.toggle\"]') && document.querySelector('[data-style-id=\"player.rating.toggle\"]')")
  await assert(`${JSON.stringify(allControls)}.every(id=>getComputedStyle(document.querySelector('[data-style-id="'+id+'"]')).backgroundColor===${JSON.stringify(independentColors)}[id])`)
  await waitFor(actionColors)
  await click('Edit appearance')
  console.log('PASS: all 16 home buttons have distinct popups and colors, including Update/Delete text and borders; old preset colors survive the split, independent edits survive reload, and generic buttons no longer recolor them.')
  await waitFor("getComputedStyle(document.querySelector('.character-rating-value')).color==='rgb(54, 102, 153)' && getComputedStyle(document.querySelector('.players-profile-section [data-style-id=\"value\"]')).color==='rgb(54, 102, 153)'")
  await openPanel('.character-rating-value')
  await waitFor("document.querySelector('.appearance-editor header strong')?.textContent==='Player list rating numbers'")
  await setColor('color','#6611cc')
  await waitFor("getComputedStyle(document.querySelector('.character-rating-value')).color==='rgb(102, 17, 204)'")
  await assert("getComputedStyle(document.querySelector('.players-profile-section [data-style-id=\"value\"]')).color==='rgb(54, 102, 153)'")
  await openPanel('.players-profile-section [data-style-id="value"]')
  await waitFor("document.querySelector('.appearance-editor header strong')?.textContent==='Metric values'")
  await setColor('color','#1166cc')
  await waitFor("getComputedStyle(document.querySelector('.players-profile-section [data-style-id=\"value\"]')).color==='rgb(17, 102, 204)'")
  await assert("getComputedStyle(document.querySelector('.character-rating-value')).color==='rgb(102, 17, 204)'")
  console.log('PASS: player-list ratings preserve old saved colors and edit independently from player-profile values in both directions.')

  // Text must physically move, not merely acquire an ineffective text-align rule.
  for(const id of allControls.filter(id=>id.endsWith('.toggle'))){
    const selector=buttonSelector(id)
    await openPanel(selector)
    await waitFor(`document.querySelector('.appearance-editor header strong')?.textContent===${JSON.stringify(extension.scopes.home.targets[id].label)}`)
    for(const alignment of ['left','center','right']){
      await evaluate(`(()=>{const input=document.querySelector('#appearance-textAlign');input.value=${JSON.stringify(alignment)};input.dispatchEvent(new Event('change',{bubbles:true}))})()`)
      await waitFor(`(()=>{const b=document.querySelector(${JSON.stringify(selector)}),s=b.querySelector('span'),r=document.createRange();r.selectNodeContents(s);const text=r.getBoundingClientRect(),box=s.getBoundingClientRect();return getComputedStyle(b).textAlign===${JSON.stringify(alignment)} && box.width>text.width+50 && Math.abs(${alignment==='left'?'text.left-box.left':alignment==='right'?'text.right-box.right':'(text.left+text.right-box.left-box.right)/2'})<1 && b.querySelector('svg').getBoundingClientRect().left>=box.right})()`)
    }
  }
  console.log('PASS: all eight analysis disclosure labels visibly align left, center and right; chevrons stay on the right.')
  await openPanel('.character-rating-name')
  await waitFor("document.querySelector('#appearance-color') && document.querySelector('.appearance-editor header strong')?.textContent==='Links on this page'")
  const rankColor=await evaluate("getComputedStyle(document.querySelector('.character-rating-rank')).color")
  const ratingColor=await evaluate("getComputedStyle(document.querySelector('.character-rating-value')).color")
  await setColor('color','#ab12cd')
  await waitFor("getComputedStyle(document.querySelector('.character-rating-name')).color==='rgb(171, 18, 205)'")
  await assert(`getComputedStyle(document.querySelector('.character-rating-rank')).color===${JSON.stringify(rankColor)} && getComputedStyle(document.querySelector('.character-rating-value')).color===${JSON.stringify(ratingColor)}`)
  await openPanel('.character-medal-toggle')
  await waitFor("document.querySelector('.appearance-editor header strong')?.textContent==='Player list · medal'")
  await setColor('hoverColor','#12abcd')
  await setColor('color','#ab3412') // Later normal-color row must not override the active state.
  await setColor('hoverBackground','#305172')
  await setColor('background','#712335') // Likewise for an existing normal-background override.
  await waitFor("document.querySelector('.character-medal-toggle').getAttribute('aria-pressed')==='true' && getComputedStyle(document.querySelector('.character-medal-toggle svg')).color==='rgb(18, 171, 205)'")
  await waitFor("getComputedStyle(document.querySelector('.character-medal-toggle')).backgroundColor==='rgb(48, 81, 114)'")
  await click('Save styles'); await waitFor("!document.querySelector('.appearance-account-controls').textContent.includes('*')")
  await evaluate("document.querySelector('[aria-label=\"Close appearance popup\"]').click();document.querySelector('.character-medal-toggle').click();document.querySelector('.character-medal-toggle').scrollIntoView({block:'center'})")
  await waitFor("document.querySelector('.character-medal-toggle').getAttribute('aria-pressed')==='false' && getComputedStyle(document.querySelector('.character-medal-toggle svg')).color==='rgb(171, 52, 18)'")
  await waitFor("getComputedStyle(document.querySelector('.character-medal-toggle')).backgroundColor==='rgb(113, 35, 53)'")
  const hoverMedal=JSON.parse(await evaluate("JSON.stringify((()=>{const r=document.querySelector('.character-medal-toggle').getBoundingClientRect();return{x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())"))
  await send('input.performActions',{context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[{type:'pointerMove',...hoverMedal}]}]})
  await waitFor("document.querySelector('.character-medal-toggle').matches(':hover') && getComputedStyle(document.querySelector('.character-medal-toggle svg')).color==='rgb(18, 171, 205)'")
  await waitFor("getComputedStyle(document.querySelector('.character-medal-toggle')).backgroundColor==='rgb(48, 81, 114)'")
  await send('input.performActions',{context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[{type:'pointerMove',x:1,y:1}]}]})
  await waitFor("getComputedStyle(document.querySelector('.character-medal-toggle svg')).color==='rgb(171, 52, 18)'")
  await waitFor("getComputedStyle(document.querySelector('.character-medal-toggle')).backgroundColor==='rgb(113, 35, 53)'")
  await evaluate("document.querySelector('.character-medal-toggle').click()")
  await waitFor("getComputedStyle(document.querySelector('.character-medal-toggle svg')).color==='rgb(18, 171, 205)'")
  await waitFor("!document.querySelector('.character-medal-toggle').matches(':hover') && getComputedStyle(document.querySelector('.character-medal-toggle')).backgroundColor==='rgb(48, 81, 114)'")
  await load('/?player=hikaru')
  await waitFor("document.querySelector('.character-rating-name') && document.querySelector('.character-medal-toggle svg') && getComputedStyle(document.querySelector('.character-rating-name')).color==='rgb(171, 18, 205)' && getComputedStyle(document.querySelector('.character-medal-toggle svg')).color==='rgb(18, 171, 205)'")
  await waitFor("document.querySelector('.players-profile-section [data-style-id=\"value\"]') && getComputedStyle(document.querySelector('.character-rating-value')).color==='rgb(102, 17, 204)' && getComputedStyle(document.querySelector('.players-profile-section [data-style-id=\"value\"]')).color==='rgb(17, 102, 204)'")
  await waitFor("!document.querySelector('.character-medal-toggle').matches(':hover') && getComputedStyle(document.querySelector('.character-medal-toggle')).backgroundColor==='rgb(48, 81, 114)'")
  await waitFor("document.querySelector('[data-style-id=\"player.analysis.toggle\"]') && getComputedStyle(document.querySelector('[data-style-id=\"player.analysis.toggle\"]')).textAlign==='right'")
  console.log('PASS: player names use the editor color, rankings remain unchanged, medal hover/active text AND background colors match off-pointer, and saved styles survive reload.')
  await evaluate("document.querySelector('.rail-btn[href=\"/database\"]').click()")
  await waitFor("location.pathname==='/database' && document.querySelector('.database-canvas')")
  await assert("document.querySelector('.rail-btn[href=\"/database\"]').classList.contains('active') && window.appearancePagesTest.errors.length===0")
  await load('/database')
  await waitFor("document.querySelector('main[aria-label=\"Database\"]') && document.querySelector('.appearance-account-controls button')?.disabled===false")
  await assert("window.appearancePagesTest.requests.every(r=>['/auth/me','/appearance','/database/schema'].includes(r.path))")
  console.log('PASS: Database sidebar navigation and direct reload open the canvas without starting work.')
} finally {
 socket?.close();browser.kill('SIGTERM')
 await Promise.race([once(browser,'exit'),new Promise(resolve=>setTimeout(resolve,3000))])
 await rm(temporary,{recursive:true,force:true})
}
