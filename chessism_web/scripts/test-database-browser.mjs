// Actual Database page; all API responses mocked, no application records touched.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, readFile, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-database-ui-'))
const profile = path.join(temporary, 'profile'); await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9259'], { stdio: ['ignore', 'pipe', 'pipe'] })
let socket
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('Browser startup timeout')), 15000)
    const output = chunk => { const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/); if (match) { clearTimeout(timer); resolve(match[1]) } }
    browser.stdout.on('data', output); browser.stderr.on('data', output)
    browser.once('error', error => { clearTimeout(timer); reject(error) })
  })
  socket = new WebSocket(endpoint.replace(/\/$/, '') + '/session')
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject })
  const pending = new Map(); let nextId = 0
  socket.onmessage = ({ data }) => {
    const response = JSON.parse(data), call = pending.get(response.id)
    if (!call) return
    pending.delete(response.id); clearTimeout(call.timer)
    response.type === 'error' ? call.reject(Error(JSON.stringify(response))) : call.resolve(response.result)
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId, timer = setTimeout(() => { pending.delete(id); reject(Error(`Timeout: ${method}`)) }, 30000)
    pending.set(id, { resolve, reject, timer }); socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  await send('browsingContext.setViewport', { context, viewport: { width: 1400, height: 1000 } })
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw Error(JSON.stringify(response))
    return response.result.value
  }
  const assert = async expression => { if (!await evaluate(expression)) throw Error(`Failed: ${expression}`) }
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) { if (await evaluate(`Boolean(${expression})`)) return; await new Promise(resolve => setTimeout(resolve, 100)) }
    throw Error(`Timeout: ${expression}\n` + await evaluate('JSON.stringify({requests:window.databaseTest.requests,errors:window.databaseTest.errors,view:document.querySelector(".database-world")?.style.transform,saved:JSON.parse(localStorage.getItem("chessism:database-workspace:v1:database-test-admin"))?.view,body:document.body.innerText.slice(0,2000)})'))
  }
  const read = file => readFile(new URL(file, import.meta.url), 'utf8').then(JSON.parse)
  const legacy = await read('../../chessism_api/appearance_registry.json'), extra = await read('../../chessism_api/appearance_scopes.json'), charts = await read('../../chessism_api/appearance_charts.json')
  const registry = { ...legacy, properties: { ...legacy.properties, ...charts.properties }, scopes: Object.fromEntries(Object.entries(extra.scopes).map(([id, spec]) => [id, { ...spec, targets: { ...extra.common_targets, ...charts.targets, ...spec.targets } }])) }
  await send('script.addPreloadScript', { functionDeclaration: `function(){
    const scenario=new URLSearchParams(location.search).get('case');
    const state=window.databaseTest={requests:[],errors:[],fail:scenario==='error'};
    window.addEventListener('error',e=>state.errors.push(e.message));window.addEventListener('unhandledrejection',e=>state.errors.push(String(e.reason)));
    window.confirm=()=>state.confirm!==false;
    const table=(name,index)=>({id:'public.'+name,schema:'public',name,kind:'table',is_partition:false,estimated_rows:index*100000,
      columns:Array.from({length:22},(_,i)=>({name:i===0?'id':i===1?'game_id':'column_'+index+'_'+i,data_type:i===0?'bigint':'character varying(100)',nullable:i>1,primary_key:i===0,foreign_key:name==='game_fen_association'&&i===1}))});
    const named=['game','fen','game_fen_association','player','moves','game_opening','game_player','player_stats','months',
      'game_analysis_summary','game_player_engine_summary','fen_continuation','fen_appearance_summary','fen_appearance_bin',
      'player_position_frequency','player_salience_summary','home_titled_summary','cloud_analysis_job','cloud_analysis_run',
      'cloud_control_document','cloud_control_record','cloud_control_spec','cloud_result_batch','cloud_usage','cloud_game_draft',
      'cloud_sequence_control','ingestion_pipeline_run','matrix_definition','matrix_artifact','algorithm_definition','algorithm_run',
      'account','auth_session','appearance_preset','appearance_override'];
    state.catalog={tables:Array.from({length:160},(_,i)=>table(named[i]||'summary_'+(i-4),i)),relationships:[
      {id:'fk-game',name:'game_fen_association_game_id',source_table:'public.game_fen_association',target_table:'public.game',source_columns:['game_id'],target_columns:['id']},
      {id:'fk-fen',name:'game_fen_association_fen',source_table:'public.game_fen_association',target_table:'public.fen',source_columns:['fen','version'],target_columns:['fen','version']},
      {id:'fk-self',name:'player_referrer',source_table:'public.player',target_table:'public.player',source_columns:['referrer'],target_columns:['id']}
    ],generated_at:'2026-10-11T00:00:00Z'};
    if(scenario==='empty')state.catalog={...state.catalog,tables:[],relationships:[]};
    if(scenario==='legacy')localStorage.setItem('chessism:database-workspace:v1:database-legacy-admin',JSON.stringify({version:1,
      positions:Object.fromEntries(state.catalog.tables.map((t,i)=>[t.id,{x:123+(i%12)*352,y:45+Math.floor(i/12)*352}])),
      expanded:['public.game'],view:{x:10,y:20,zoom:1}}));
    if(scenario==='storage')Storage.prototype.setItem=function(){throw Error('Storage blocked')};
    window.fetch=async(input,options={})=>{
      const path=new URL(input,location.href).pathname.replace(/^\\/api/,'');const method=options.method||'GET';state.requests.push({path,method});
      const reply=(data,status=200)=>new Response(JSON.stringify(data),{status,headers:{'Content-Type':'application/json'}});
      if(method!=='GET')throw Error('No writes permitted in this test');
      if(path==='/auth/me')return reply({account:{id:scenario==='legacy'?'database-legacy-admin':'database-test-admin',name:'Test Admin',role:scenario==='user'?'user':'admin'}});
      if(path==='/appearance')return reply({revision:0,presets:[],active_preset_id:null,overrides:[],registry:${JSON.stringify(registry)}});
      if(path==='/database/schema')return state.fail?reply({detail:'Database structure unavailable'},503):reply(state.catalog);
      if(path==='/database/preview'){
        state.previewTable=new URL(input,location.href).searchParams.get('table');
        if(state.previewDelay)await new Promise((resolve,reject)=>{
          const timer=setTimeout(resolve,state.previewDelay);
          options.signal?.addEventListener('abort',()=>{clearTimeout(timer);state.previewAborted=true;reject(new DOMException('Aborted','AbortError'))},{once:true});
        });
        if(state.previewFail)return reply({detail:'Preview temporarily unavailable'},503);
        const count=state.previewRows??50;
        return reply({table:state.previewTable,columns:[{name:'id',data_type:'bigint'},{name:'title',data_type:'text'},{name:'password_hash',data_type:'text',redacted:true}],
          rows:Array.from({length:count},(_,i)=>[String(9000+i),i===0?'<img src=x onerror=alert(1)>':i===1?null:'Sample row '+i,'[redacted]']),
          row_count:count,row_limit:50,sample_method:'random_blocks',candidate_limit:1000,omitted_columns:0,cell_limit:512,truncated_cells:0});
      }
      state.errors.push('Unexpected request '+path);throw Error('Unexpected request');
    }
  }` })
  const load = query => send('browsingContext.navigate', { context, wait: 'complete', url: 'http://localhost:6789/database' + (query || '') })
  const click = label => evaluate(`Array.from(document.querySelectorAll('button')).find(b=>b.textContent.trim()===${JSON.stringify(label)}).click()`)
  const search = async query => {
    await evaluate(`(()=>{const n=document.querySelector('#database-search');n.focus();n.click();Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,${JSON.stringify(query)});n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}))})()`)
    await waitFor("document.querySelector('.database-search-results button')")
    await evaluate("document.querySelector('.database-search-results button').click()")
  }
  const selector = id => `.database-table[data-table-id="${id}"]`
  const rectPoint = async selector => JSON.parse(await evaluate(`JSON.stringify((()=>{const r=document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect();return{x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())`))
  const drag = async (point, dx, dy, button = 0) => send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [
    { type: 'pointerMove', ...point }, { type: 'pointerDown', button }, { type: 'pointerMove', x: point.x + dx, y: point.y + dy, duration: 200 }, { type: 'pointerUp', button },
  ] }] })
  const doubleClick = async selector => send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [
    { type: 'pointerMove', ...await rectPoint(selector) }, { type: 'pointerDown', button: 0 }, { type: 'pointerUp', button: 0 },
    { type: 'pause', duration: 80 }, { type: 'pointerDown', button: 0 }, { type: 'pointerUp', button: 0 },
  ] }] })
  const wheel = async (point, deltaY) => send('input.performActions', { context, actions: [{ type: 'wheel', id: 'wheel', actions: [
    { type: 'scroll', origin: 'viewport', ...point, deltaX: 0, deltaY, duration: 200 },
  ] }] })
  const stored = "JSON.parse(localStorage.getItem('chessism:database-saved-layout:v1:database-test-admin'))?.layout"
  const live = `(()=>{const values=document.querySelector('.database-world').style.transform.match(/-?\\d*\\.?\\d+(?:e[-+]?\\d+)?/gi).map(Number);return {
    positions:Object.fromEntries(Array.from(document.querySelectorAll('.database-table'),t=>[t.dataset.tableId,{x:parseFloat(t.style.left),y:parseFloat(t.style.top)}])),
    view:{x:values[0],y:values[1],zoom:values[2]}}})()`
  await load()
  await waitFor("document.querySelector('.database-table') && document.querySelector('.appearance-account-controls button')?.disabled===false")
  await assert("!document.body.textContent.includes('tables / views') && document.querySelectorAll('.database-table').length<25")
  await assert("!document.querySelector('.database-zoom, .database-hint, .site-footer') && !document.body.textContent.includes('Save layout') && !document.body.textContent.includes('Left-drag a title')")
  await assert("(()=>{const c=document.querySelector('.database-canvas'),r=c.getBoundingClientRect(),s=document.querySelector('.database-search').getBoundingClientRect();return c.contains(document.querySelector('#database-search'))&&Math.abs(s.left-r.left-13)<2&&Math.abs(s.top-r.top-13)<2})()")
  await assert("(()=>{const c=document.querySelector('.database-canvas'),r=c.getBoundingClientRect(),p=c.parentElement.getBoundingClientRect(),h=c.querySelector('.database-heading')?.getBoundingClientRect(),s=c.querySelector('.database-search').getBoundingClientRect();return !document.querySelector('.database-bottom-controls')&&Math.abs(r.left-p.left)<1&&Math.abs(r.right-p.right)<1&&Math.abs(r.height-innerHeight)<1&&h&&Math.abs(r.right-h.right-13)<2&&Math.abs(r.bottom-h.bottom-13)<2&&h.top>s.bottom})()")
  const refreshCount = await evaluate("window.databaseTest.requests.filter(r=>r.path==='/database/schema').length")
  const refreshView = await evaluate("document.querySelector('.database-world').style.transform")
  await drag(await rectPoint('.database-heading button'), 0, 0)
  await waitFor(`window.databaseTest.requests.filter(r=>r.path==='/database/schema').length===${refreshCount + 1} && !document.querySelector('.database-heading button').disabled`)
  await wheel(await rectPoint('.database-heading button'), -160)
  await assert(`!document.querySelector('.database-canvas.dragging') && document.querySelector('.database-world').style.transform===${JSON.stringify(refreshView)}`)
  const searchView = await evaluate("document.querySelector('.database-world').style.transform")
  await drag(await rectPoint('#database-search'), 0, 0)
  await assert(`document.activeElement.id==='database-search' && !document.querySelector('.database-canvas.dragging') && document.querySelector('.database-world').style.transform===${JSON.stringify(searchView)}`)
  await evaluate("(()=>{const n=document.querySelector('#database-search');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,'public');n.dispatchEvent(new Event('input',{bubbles:true}))})()")
  await waitFor("document.querySelectorAll('.database-search-results button').length===30")
  await wheel(await rectPoint('.database-search-results'), 180)
  await waitFor("document.querySelector('.database-search-results').scrollTop>0")
  await assert(`document.querySelector('.database-world').style.transform===${JSON.stringify(searchView)}`)
  await evaluate("document.querySelector('#database-search').dispatchEvent(new KeyboardEvent('keydown',{key:'Escape',bubbles:true}))")
  await assert("!document.querySelector('.database-columns') && Array.from(document.querySelectorAll('.database-table-header')).every(n=>n.getAttribute('aria-expanded')==='false')")
  await assert(`(()=>{const g=document.querySelector('[data-table-id="public.game"]').getBoundingClientRect(),c=document.querySelector('.database-canvas').getBoundingClientRect();return g.left>=c.left&&g.right<=c.right&&g.top>=c.top&&g.bottom<=c.bottom})()`)
  await assert(`${stored}==null`)
  await assert("document.querySelector('.database-table[data-table-id=\"public.game\"]')?.dataset.databaseGroup==='games' && document.querySelectorAll('.database-region').length>10 && !document.querySelector('[data-relationship-id]')")
  if (process.env.DATABASE_SCREENSHOT) {
    const shot = await send('browsingContext.captureScreenshot', { context })
    await writeFile(process.env.DATABASE_SCREENSHOT.replace(/\.png$/, '-default.png'), Buffer.from(shot.data, 'base64'))
  }
  await search('public.game_fen_association')
  await waitFor("document.querySelectorAll('[data-relationship-id]').length===2 && document.querySelector('.database-table.selected')?.dataset.tableId==='public.game_fen_association'")
  await doubleClick('.database-table.selected .database-table-header')
  await waitFor("document.querySelector('.database-table.selected .database-table-header')?.getAttribute('aria-expanded')==='true'")
  await assert("document.querySelector('.database-relationships').textContent.includes('fen, version → fen, version') && document.querySelector('.database-table.selected abbr[title=\"Foreign key\"]')")
  const original = await evaluate(`parseFloat(document.querySelector(${JSON.stringify(selector('public.game_fen_association'))}).style.left)`)
  await drag(await rectPoint(selector('public.game_fen_association') + ' .database-table-header'), 80, 30)
  await waitFor(`${live}.positions['public.game_fen_association']?.x===${original + 80}`)
  await assert(`${stored}==null`)
  await load()
  await waitFor(`document.querySelector(${JSON.stringify(selector('public.game_fen_association'))}) && parseFloat(document.querySelector(${JSON.stringify(selector('public.game_fen_association'))}).style.left)===${original}`)
  await assert("!document.querySelector('.database-table.expanded') && document.querySelector('.database-region')")
  await search('public.game_fen_association')
  await evaluate(`document.querySelector(${JSON.stringify(selector('public.game_fen_association')+' .database-table-header')}).focus();document.activeElement.dispatchEvent(new KeyboardEvent('keydown',{key:'ArrowRight',bubbles:true,cancelable:true}))`)
  await waitFor(`${live}.positions['public.game_fen_association']?.x===${original + 10}`)
  await assert(`${stored}==null`)
  console.log('PASS: full-size canvas contains title/refresh at bottom-right and search at top-left; controls do not pan/zoom, and dragging/keyboard moves remain temporary.')
  await search('column_159_21')
  await waitFor(`document.querySelector('.database-table.selected')?.dataset.tableId==='public.summary_155' && ${live}.view.zoom===1`)
  const baseline = await evaluate("window.databaseTest.requests.filter(r=>r.path==='/database/schema').length")
  await evaluate("const c=document.querySelector('.database-canvas');c.focus();c.dispatchEvent(new KeyboardEvent('keydown',{key:'Home',bubbles:true,cancelable:true}))")
  await waitFor("document.querySelectorAll('.database-table').length===160")
  await assert("document.querySelectorAll('.database-columns li').length===0")
  await search('public.game_fen_association')
  await doubleClick('.database-table.selected .database-table-header')
  await waitFor("document.querySelector('.database-table.selected .database-columns')")
  const emptyPoint = JSON.parse(await evaluate(`JSON.stringify((()=>{
    const r=document.querySelector('.database-canvas').getBoundingClientRect();
    for(let y=r.top+15;y<r.bottom-60;y+=35)for(let x=r.left+15;x<r.right-80;x+=35){
      const e=document.elementFromPoint(x,y);
      if(e?.closest('.database-canvas')&&!e.closest('.database-table, .database-canvas-controls'))return{x:Math.round(x),y:Math.round(y)};
    }
  })())`))
  const previousView = await evaluate("document.querySelector('.database-world').style.transform")
  await drag(emptyPoint, 45, 35)
  await waitFor(`document.querySelector('.database-world').style.transform!==${JSON.stringify(previousView)}`)
  await wheel(emptyPoint, -160)
  await waitFor(`${live}.view.zoom!==1`)
  await assert(`window.databaseTest.requests.filter(r=>r.path==='/database/schema').length===${baseline}`)
  await search('public.player')
  await waitFor("document.querySelectorAll('[data-relationship-id]').length===1")
  await assert("document.querySelector('.database-relationships').textContent.includes('player_referrer')")
  await search('public.game_fen_association')
  const header = selector('public.game_fen_association') + ' .database-table-header'
  await waitFor(`${live}.view.zoom===1 && document.querySelector('.database-table.selected')?.dataset.tableId==='public.game_fen_association'`)
  const beforeMiddlePan = JSON.parse(await evaluate(`JSON.stringify(${live})`))
  const beforeScroll = await evaluate('window.scrollY')
  await drag(await rectPoint(header), 60, 25, 1)
  await waitFor(`Math.abs(${live}.view.x-(${beforeMiddlePan.view.x + 60}))<0.01 && Math.abs(${live}.view.y-(${beforeMiddlePan.view.y + 25}))<0.01`)
  await assert(`${live}.positions['public.game_fen_association'].x===${beforeMiddlePan.positions['public.game_fen_association'].x} && window.scrollY===${beforeScroll} && !document.querySelector('.database-canvas.dragging')`)
  await wheel(await rectPoint('.database-table.selected .database-columns'), -80)
  await waitFor(`${live}.view.zoom>1`)
  await search('public.game_fen_association')
  await evaluate(`document.querySelector(${JSON.stringify(header)}).focus();document.activeElement.dispatchEvent(new KeyboardEvent('keydown',{key:'+',bubbles:true,cancelable:true}))`)
  await waitFor(`${live}.view.zoom===1.2`)
  await evaluate("document.activeElement.dispatchEvent(new KeyboardEvent('keydown',{key:'-',bubbles:true,cancelable:true}))")
  await waitFor(`${live}.view.zoom===1`)
  await doubleClick(header)
  await waitFor("document.querySelector('.database-table.selected .database-table-header')?.getAttribute('aria-expanded')==='false'")
  await assert("!document.querySelector('.database-table.selected .database-columns')")
  await evaluate("document.activeElement.dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',bubbles:true,cancelable:true}))")
  await waitFor("document.querySelector('.database-table.selected .database-table-header')?.getAttribute('aria-expanded')==='true'")
  console.log('PASS: Home fits the catalog, wheel zoom over columns, +/− from header focus, middle-button pan without moving tables, collapse and keyboard expansion.')
  await click('Edit appearance')
  await evaluate(`document.querySelector(${JSON.stringify(header)}).dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:500,clientY:250}))`)
  await waitFor("document.querySelector('.appearance-editor header strong')?.textContent==='Database table headers'")
  await evaluate("document.querySelector('[aria-label=\"Close appearance popup\"]').click()")
  const beforeEditDrag = await evaluate(`parseFloat(document.querySelector(${JSON.stringify(selector('public.game_fen_association'))}).style.left)`)
  await drag(await rectPoint(header), 50, 20)
  await waitFor(`parseFloat(document.querySelector(${JSON.stringify(selector('public.game_fen_association'))}).style.left)===${beforeEditDrag + 50}`)
  await wheel(await rectPoint(header), -80)
  await waitFor(`${live}.view.zoom>1`)
  await click('Exit appearance editing')
  console.log('PASS: column search, large-catalog fit, compact rendering, pan/zoom, self links and appearance editing without data refetch or dragging conflicts.')
  if (process.env.DATABASE_SCREENSHOT) {
    const shot = await send('browsingContext.captureScreenshot', { context })
    await writeFile(process.env.DATABASE_SCREENSHOT, Buffer.from(shot.data, 'base64'))
  }
  await evaluate('window.databaseTest.fail=true')
  await click('Refresh structure')
  await waitFor("document.querySelector('.database-error')?.textContent.includes('Showing the previous structure')")
  await assert("document.querySelector('.database-canvas') && !Array.from(document.querySelectorAll('button')).find(b=>b.textContent==='Refresh structure').disabled")
  await evaluate('window.databaseTest.fail=false')
  await click('Refresh structure')
  await waitFor("!document.querySelector('.database-error') && Array.from(document.querySelectorAll('button')).find(b=>b.textContent==='Refresh structure')?.disabled===false")
  await assert("window.databaseTest.errors.length===0 && window.databaseTest.requests.every(r=>r.method==='GET')")
  await search('public.game')
  const gameBox = JSON.parse(await evaluate("JSON.stringify((()=>{const n=document.querySelector('[data-database-region=\"games\"]');return{x:parseFloat(n.style.left),y:parseFloat(n.style.top)}})())"))
  await drag(await rectPoint(selector('public.game') + ' .database-table-header'), 70, 40)
  await waitFor(`parseFloat(document.querySelector('[data-database-region="games"]').style.left)===${gameBox.x + 70} && parseFloat(document.querySelector('[data-database-region="games"]').style.top)===${gameBox.y + 40}`)
  await search('public.player')
  const playerBoxWidth = await evaluate("parseFloat(document.querySelector('[data-database-region=\"players\"]').style.width)")
  await drag(await rectPoint(selector('public.player') + ' .database-table-header'), -90, -30)
  await waitFor(`parseFloat(document.querySelector('[data-database-region="players"]').style.width)===${playerBoxWidth + 90}`)
  await assert(`${stored}==null`)
  console.log('PASS: single-table region follows dragging and multi-table region stretches to include moved members, without writing saved layouts.')
  await load('?case=legacy')
  await waitFor("document.querySelector('.database-table[data-table-id=\"public.game\"]')")
  await assert("document.querySelector('.database-table[data-table-id=\"public.game\"]').style.left!=='123px' && document.querySelector('.database-region') && !document.querySelector('.database-table.expanded')")
  await assert("localStorage.getItem('chessism:database-saved-layout:v1:database-legacy-admin')===null && JSON.parse(localStorage.getItem('chessism:database-workspace:v1:database-legacy-admin')).positions['public.game'].x===123")
  await load()
  await waitFor("document.querySelector('.database-region')")
  await assert("!document.querySelector('.database-table.expanded')")
  await assert("document.querySelector('[data-database-region=\"players\"]')?.style.width==='348px'")
  console.log('PASS: legacy saved layouts remain untouched and each page entry returns to the default hierarchy.')
  await assert("!window.databaseTest.requests.some(r=>r.path==='/database/preview') && document.querySelectorAll('.database-table-eye').length===document.querySelectorAll('.database-table-header').length")
  await search('public.game')
  const eye = selector('public.game') + ' .database-table-eye'
  const openPreview = async () => {
    await drag(await rectPoint(eye), 0, 0)
    await waitFor("document.querySelector('dialog.database-preview[open]')")
  }
  const previewView = await evaluate("document.querySelector('.database-world').style.transform")
  const tableLeft = await evaluate("document.querySelector('[data-table-id=\"public.game\"]').style.left")
  await openPreview()
  await waitFor("document.querySelectorAll('.database-preview tbody tr').length===50")
  await assert("window.databaseTest.previewTable==='public.game' && !document.querySelector('.database-preview img') && document.querySelector('.database-preview tbody td').textContent==='9000'")
  await assert("document.querySelector('.database-preview').textContent.includes('Credential fields are hidden') && document.querySelector('.database-preview-null').textContent==='NULL'")
  await assert(`document.querySelector('[data-table-id="public.game"]').style.left===${JSON.stringify(tableLeft)} && !document.querySelector('.database-table.expanded')`)
  await assert("(()=>{const box=document.querySelector('.database-preview-scroll').getBoundingClientRect();return Array.from(document.querySelectorAll('.database-preview tbody tr')).filter(r=>{const b=r.getBoundingClientRect();return b.top>=box.top&&b.bottom<=box.bottom}).length>=10})()")
  await wheel(await rectPoint('.database-preview-scroll'), 1800)
  await waitFor("document.querySelector('.database-preview-scroll').scrollTop>1000")
  await assert(`document.querySelector('.database-world').style.transform===${JSON.stringify(previewView)}`)
  await assert("document.querySelector('.database-preview tbody tr:last-child th').textContent==='49'")
  if (process.env.DATABASE_SCREENSHOT) {
    await evaluate("document.querySelector('.database-preview-scroll').scrollTop=0")
    const shot = await send('browsingContext.captureScreenshot', { context })
    await writeFile(process.env.DATABASE_SCREENSHOT.replace(/\.png$/, '-preview.png'), Buffer.from(shot.data, 'base64'))
  }
  await drag({x:4,y:4},0,0)
  await waitFor("!document.querySelector('.database-preview')")
  await assert("document.activeElement.classList.contains('database-table-eye') && document.body.style.overflow!== 'hidden'")
  await openPreview()
  await waitFor("document.querySelectorAll('.database-preview tbody tr').length===50")
  await evaluate('window.databaseTest.previewRows=3')
  await click('New sample')
  await waitFor("document.querySelectorAll('.database-preview tbody tr').length===3")
  await evaluate('window.databaseTest.previewRows=0')
  await click('New sample')
  await waitFor("document.querySelector('.database-preview').textContent.includes('no visible rows')")
  await evaluate('window.databaseTest.previewFail=true')
  await click('New sample')
  await waitFor("document.querySelector('.database-preview [role=\"alert\"]')?.textContent.includes('temporarily unavailable')")
  await evaluate('window.databaseTest.previewFail=false;window.databaseTest.previewRows=50')
  await click('New sample')
  await waitFor("document.querySelectorAll('.database-preview tbody tr').length===50")
  await send('input.performActions',{context,actions:[{type:'key',id:'keyboard',actions:[{type:'keyDown',value:'\uE00C'},{type:'keyUp',value:'\uE00C'}]}]})
  await waitFor("!document.querySelector('.database-preview')")
  await evaluate('window.databaseTest.previewDelay=2000')
  await openPreview()
  await waitFor("document.querySelector('.database-preview [role=\"status\"]')")
  await evaluate("document.querySelector('[aria-label=\"Close table preview\"]').click()")
  await waitFor('window.databaseTest.previewAborted && !document.querySelector(".database-preview")')
  await assert("window.databaseTest.errors.length===0 && window.databaseTest.requests.every(r=>r.method==='GET')")
  await evaluate('window.databaseTest.previewDelay=0')
  console.log('PASS: on-demand 50-row eye preview, ten visible rows, wheel scroll without canvas zoom, outside/Escape close, focus return, resample, null/redacted/text-safe values, empty/short/error cases and request abort.')
  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 844 } })
  await waitFor('document.documentElement.scrollWidth<=innerWidth+1')
  await assert("(()=>{const c=document.querySelector('.database-canvas').getBoundingClientRect(),s=document.querySelector('.database-search').getBoundingClientRect();return s.left>=c.left&&s.right<=c.right&&s.width>100})()")
  await assert("(()=>{const c=document.querySelector('.database-canvas').getBoundingClientRect(),h=document.querySelector('.database-heading').getBoundingClientRect(),s=document.querySelector('.database-search').getBoundingClientRect();return Math.abs(c.height-innerHeight)<1&&h.left>=c.left&&Math.abs(c.right-h.right-13)<2&&Math.abs(c.bottom-h.bottom-13)<2&&Math.abs(s.left-c.left-13)<2&&s.bottom<h.top})()")
  await search('public.game_fen_association')
  await assert("document.querySelector('.database-table.selected')?.dataset.tableId==='public.game_fen_association'")
  await evaluate("document.querySelector('.database-table.selected .database-table-eye').click()")
  await waitFor("document.querySelectorAll('.database-preview tbody tr').length===50")
  await assert("(()=>{const r=document.querySelector('.database-preview').getBoundingClientRect();return r.left>=0&&r.right<=innerWidth&&r.top>=0&&r.bottom<=innerHeight})()")
  await evaluate("document.querySelector('[aria-label=\"Close table preview\"]').click()")
  await load('?case=user')
  await waitFor("document.querySelector('main [role=\"alert\"]')?.textContent.includes('Superuser')")
  await assert("!window.databaseTest.requests.some(r=>r.path==='/database/schema')")
  await load('?case=error')
  await waitFor("document.querySelector('.database-error')")
  await assert("!document.querySelector('.database-canvas')")
  await load('?case=empty')
  await waitFor("document.querySelector('.database-empty')")
  await load('?case=storage')
  await waitFor("document.querySelector('.database-canvas')")
  await assert("document.querySelector('.database-canvas') && window.databaseTest.errors.length===0")
  console.log('PASS: read-only requests, stale/error recovery, mobile canvas/preview width, non-admin gating, empty schema and disabled storage.')
} finally {
  socket?.close(); browser.kill('SIGTERM')
  await Promise.race([once(browser, 'exit'), new Promise(resolve => setTimeout(resolve, 3000))])
  await rm(temporary, { recursive: true, force: true })
}
