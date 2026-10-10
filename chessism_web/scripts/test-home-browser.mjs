// Actual router and components; every API call is intercepted by the fixture.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-home-ui-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9254'], { stdio: ['ignore', 'pipe', 'pipe'] })
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
    if (pending) {
      requests.delete(response.id)
      response.type === 'error' ? pending.reject(new Error(JSON.stringify(response))) : pending.resolve(response.result)
    }
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const id = ++nextId; requests.set(id, { resolve, reject }); socket.send(JSON.stringify({ id, method, params }))
  })
  await send('session.new', { capabilities: {} })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  await send('browsingContext.setViewport', { context, viewport: { width: 1400, height: 1000 } })
  const load = initial => send('browsingContext.navigate', { context,
    url: (process.env.HOME_UI_TEST_URL || 'http://localhost:6789') + '/tests/fixtures/home.html?initial=' + encodeURIComponent(initial), wait: 'complete' })
  const evaluate = async expression => {
    const response = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (response.type === 'exception') throw new Error(JSON.stringify(response))
    return response.result.value
  }
  const waitFor = async expression => {
    for (let i=0; i<100; i++) {
      if (await evaluate(expression)) return
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw new Error('Timeout: ' + expression)
  }
  const assert = async expression => {
    if (await evaluate(expression)) return
    console.log(await evaluate(`JSON.stringify({width:innerWidth,scrollWidth:document.documentElement.scrollWidth,
      metrics:document.querySelectorAll('.metric-card').length,grid:document.querySelector('.metric-grid') && getComputedStyle(document.querySelector('.metric-grid')).gridTemplateColumns,
      errors:window.homeTest.unexpected,location:location.href})`))
    throw new Error('Failed: ' + expression)
  }
  const clickPlayer = name => evaluate(`document.querySelector('a.character-rating-row[href="/?player=${name}"]').click()`)
  const loaded = name => waitFor(`document.querySelector('.players-title-select')?.textContent.trim()==='${name}' && document.querySelector('.profile-item strong')?.textContent==='${name}' && !document.querySelector('.players-update-button')?.disabled && document.querySelector('.measure-daily-efficiency-point')`)

  await load('/')
  await waitFor("document.querySelectorAll('.character-rating-row:not(.loading)').length===50")
  await waitFor("getComputedStyle(document.querySelector('.home-overview')).gridTemplateColumns.split(' ').length===2")
  await assert("document.querySelector('.home-page main').children[0].classList.contains('home-overview') && document.querySelector('.home-overview').children[0].classList.contains('dashboard-panel') && document.querySelector('.home-overview').children[1].classList.contains('main-characters-carousel')")
  await assert("document.querySelectorAll('.metric-card').length===4 && getComputedStyle(document.querySelector('.metric-grid')).gridTemplateColumns.split(' ').length===2")
  await assert("!document.querySelector('.home-shell > .site-header') && !document.body.textContent.includes('Chess analytics workstation')")
  await assert("document.querySelector('.dashboard-panel').getBoundingClientRect().top===document.querySelector('.main-characters-carousel').getBoundingClientRect().top && document.querySelector('.dashboard-panel').getBoundingClientRect().right < document.querySelector('.main-characters-carousel').getBoundingClientRect().left")
  await assert("!document.querySelector('.status') && !document.querySelector('.mode-summary') && !document.querySelector('.players-top-grid')")
  await assert("!window.homeTest.requests.some(r=>r.path==='/status' || r.path==='/games/time_controls' || (r.path.startsWith('/players/') && r.path.split('/').length===3))")
  await assert("document.querySelector('.rail-nav a[href=\"/system_status\"]').textContent.includes('System Status')")
  if (process.env.HOME_SCREENSHOT) {
    await evaluate("document.fonts.ready.then(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))))")
    const result = await send('browsingContext.captureScreenshot', {context})
    await writeFile(process.env.HOME_SCREENSHOT, Buffer.from(result.data, 'base64'))
  }

  await evaluate("Array.from(document.querySelectorAll('.mode-tab')).find(b=>b.textContent==='blitz').click()")
  await waitFor("document.querySelector('.mode-tab.active')?.textContent==='blitz' && !document.querySelector('.character-rating-row.loading')")
  await evaluate("Array.from(document.querySelectorAll('.character-pagination button')).find(b=>b.textContent.trim()==='Next').click()")
  await waitFor("document.querySelector('.character-pagination span')?.textContent==='2 / 2'")
  await clickPlayer('player51')
  await loaded('player51')
  await assert("location.pathname==='/' && location.search==='?player=player51' && document.querySelectorAll('main').length===1 && document.querySelectorAll('.left-rail').length===1")
  await assert("document.querySelector('.character-rating-row.selected')?.getAttribute('aria-current')==='true' && window.homeTest.scrolls.at(-1)==='selected-player'")
  await assert("document.querySelector('.mode-tab.active').textContent==='blitz' && document.querySelector('.character-pagination span').textContent==='2 / 2'")
  await clickPlayer('player52')
  await loaded('player52')
  await assert("document.querySelectorAll('.players-top-grid').length===1 && !document.querySelector('.players-top-grid').textContent.includes('player51')")
  await waitFor("document.querySelector('[aria-label=\"Rating chart\"] .graph-empty-label')?.textContent==='No rating observations.'")
  await assert("document.querySelectorAll('[aria-label=\"Rating chart\"] .daily-year-band').length===0")
  await evaluate('history.back()')
  await loaded('player51')
  await evaluate('history.forward()')
  await loaded('player52')
  await assert("document.querySelector('.character-pagination span').textContent==='2 / 2' && document.querySelector('.mode-tab.active').textContent==='blitz'")
  await assert("window.homeTest.requests.filter(r=>r.path==='/games/generalities').length===1 && window.homeTest.requests.filter(r=>r.path==='/players/main_characters/top').length===2")

  await evaluate("document.querySelector('button[aria-label=\"Next player: magnuscarlsen\"]').click()")
  await loaded('magnuscarlsen')
  await assert("location.search==='?player=magnuscarlsen' && document.querySelector('.character-pagination span').textContent==='2 / 2'")
  await evaluate("document.querySelector('.players-title-select').click()")
  await waitFor("Array.from(document.querySelectorAll('[role=menuitem]')).some(b=>b.textContent==='hikaru')")
  await evaluate("Array.from(document.querySelectorAll('[role=menuitem]')).find(b=>b.textContent==='hikaru').click()")
  await loaded('hikaru')
  await assert("!document.querySelector('[role=menu]')")
  console.log('PASS: title removed, summary beside Main Characters, inline player workspace, preserved rankings and browser/hero navigation.')

  // Ratings uses the same calendar-band interaction as accuracy, without year pills.
  await waitFor("document.querySelectorAll('.behavior-rating-line').length===2")
  await evaluate("window.ratingChart=()=>document.querySelector('[aria-label=\"Rating chart\"]'); window.beforeRatingRequests=window.homeTest.requests.filter(r=>r.path.includes('/behavioural/')).length; window.ratingChart().scrollIntoView({block:'center'})")
  await assert("document.querySelectorAll('.behavior-rating-years button').length===1 && document.querySelector('.behavior-rating-years button').textContent==='All' && window.ratingChart().querySelectorAll('.daily-year-band').length===3")
  const yearGeometry = JSON.parse(await evaluate(`JSON.stringify((() => {
    const rect=window.ratingChart().querySelector('[data-year="2024"]').getBoundingClientRect();
    return {x:Math.round(rect.x+rect.width/2),y:Math.round(rect.y+rect.height/2)};
  })())`))
  await send('input.performActions', {context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[
    {type:'pointerMove',x:yearGeometry.x,y:yearGeometry.y},
  ]}]})
  await waitFor("window.ratingChart().querySelector('.daily-year-band.highlighted')?.dataset.year==='2024' && window.ratingChart().querySelector('.daily-year-label')?.textContent==='2024'")
  if (process.env.RATING_SCREENSHOT) {
    const result = await send('browsingContext.captureScreenshot', {context})
    await writeFile(process.env.RATING_SCREENSHOT, Buffer.from(result.data, 'base64'))
  }
  await send('input.performActions', {context,actions:[{type:'pointer',id:'mouse',parameters:{pointerType:'mouse'},actions:[
    {type:'pointerDown',button:0},{type:'pointerUp',button:0},
  ]}]})
  await waitFor("window.ratingChart().querySelector('.daily-year-hint').textContent.includes('Showing 2024.')")
  await assert("window.ratingChart().querySelectorAll('.daily-year-band').length===0 && document.querySelector('.behavior-rating-years button').getAttribute('aria-pressed')==='false'")
  await assert("Array.from(document.querySelectorAll('.behavior-rating-line')).every(line=>line.points.numberOfItems===2) && window.ratingChart().querySelector('.graph-axis-label').textContent==='2024-01-01'")
  await evaluate("document.querySelector('.behavior-rating-years button').click()")
  await waitFor("window.ratingChart().querySelectorAll('.daily-year-band').length===3")
  await evaluate("window.ratingChart().querySelector('[data-year=\"2025\"]').focus()")
  await waitFor("window.ratingChart().querySelector('.daily-year-band.highlighted')?.dataset.year==='2025'")
  await evaluate("window.ratingChart().querySelector('[data-year=\"2025\"]').dispatchEvent(new KeyboardEvent('keydown',{key:'Enter',bubbles:true}))")
  await waitFor("window.ratingChart().querySelector('.daily-year-hint').textContent.includes('Showing 2025.')")
  await assert("document.querySelectorAll('.behavior-rating-line').length===1 && document.querySelector('.behavior-rating-line').points.numberOfItems===1 && document.querySelector('.behavior-rating-point') && document.activeElement===document.querySelector('.behavior-rating-years button')")
  await evaluate("document.querySelector('.behavior-rating-years button').click()")
  await waitFor("window.ratingChart().querySelectorAll('.daily-year-band').length===3")
  await evaluate("window.ratingChart().querySelector('.behavior-mode-controls button').click()")
  await waitFor("document.querySelectorAll('.behavior-rating-line').length===1")
  await evaluate("window.ratingChart().querySelector('[data-year=\"2024\"]').dispatchEvent(new KeyboardEvent('keydown',{key:' ',bubbles:true}))")
  await waitFor("window.ratingChart().querySelector('.daily-year-hint').textContent.includes('Showing 2024.')")
  await assert("document.querySelectorAll('.behavior-rating-line').length===1 && window.ratingChart().querySelector('.behavior-mode-controls button').getAttribute('aria-pressed')==='false'")
  await evaluate("document.querySelector('.behavior-rating-years button').click()")
  await waitFor("window.ratingChart().querySelectorAll('.daily-year-band').length===3")
  await assert("document.querySelectorAll('.behavior-rating-line').length===1 && window.homeTest.requests.filter(r=>r.path.includes('/behavioural/')).length===window.beforeRatingRequests")
  console.log('PASS: Ratings hover/click and keyboard year zoom, All reset, empty/single-day years and preserved game-type filters; no new API calls.')

  await evaluate("document.querySelector('.rail-nav a[href=\"/system_status\"]').click()")
  await waitFor("document.querySelector('.system-status-page .status-card .value')?.textContent==='Active'")
  await assert("document.querySelector('main').children.length===3 && !document.querySelector('.dashboard-panel') && !document.querySelector('.players-top-grid')")
  await assert("document.querySelector('.rail-nav a[href=\"/system_status\"]').classList.contains('active')")
  await waitFor("window.homeTest.requests.filter(r=>r.path==='/status').length>=2")
  await waitFor("document.querySelector('[data-cloud-activity]')?.textContent==='Idle'")
  await assert("document.querySelector('[data-cloud-controller]').textContent==='Online' && !document.querySelector('.cloud-status-panel progress')")
  await assert("document.querySelectorAll('.cloud-status-panel .status-card').length===5 && document.querySelector('[data-cloud-cpus] .value').textContent==='0'")
  await assert("document.querySelector('.cloud-status-panel').nextElementSibling.classList.contains('cloud-bill') && document.querySelector('#cloud-bill-toggle').textContent==='gcloud bill' && document.querySelector('#cloud-bill-content').hidden")
  await assert("!window.homeTest.requests.some(r=>r.path==='/analysis/cloud/billing')")
  await evaluate("document.querySelector('#cloud-bill-toggle').click()")
  await waitFor("document.querySelectorAll('.cloud-bill-totals article').length===3")
  await assert("document.querySelector('#cloud-bill-toggle').getAttribute('aria-expanded')==='true' && document.querySelector('.cloud-bill-totals article:last-child strong').textContent.includes('10.00')")
  await assert("document.querySelector('.cloud-bill').textContent.includes('Compute Engine') && document.querySelector('.cloud-bill').textContent.includes('not a final invoice')")
  await new Promise(resolve=>setTimeout(resolve,400))
  await assert("window.homeTest.requests.filter(r=>r.path==='/analysis/cloud/billing').length===1")
  await evaluate("document.querySelector('.cloud-bill select').value='2026-09'; document.querySelector('.cloud-bill select').dispatchEvent(new Event('change',{bubbles:true}))")
  await assert("document.querySelector('.cloud-bill-empty').textContent.includes('does not confirm zero') && !document.querySelector('.cloud-bill-totals')")
  await evaluate("document.querySelector('.cloud-bill select').value='2026-10'; document.querySelector('.cloud-bill select').dispatchEvent(new Event('change',{bubbles:true})); window.homeTest.failBilling=true; document.querySelector('.cloud-bill-actions button').click()")
  await waitFor("document.querySelector('.cloud-bill [role=alert]')?.textContent.includes('showing the previous snapshot')")
  await assert("document.querySelector('.cloud-bill-totals article:last-child strong').textContent.includes('10.00')")
  await evaluate("window.homeTest.failBilling=false; document.querySelector('.cloud-bill-actions button').click()")
  await waitFor("!document.querySelector('.cloud-bill [role=alert]') && !document.querySelector('.cloud-bill-actions button').disabled")
  if (process.env.BILLING_SCREENSHOT) {
    await evaluate("document.querySelector('.cloud-bill').scrollIntoView({block:'start'})")
    const shot = await send('browsingContext.captureScreenshot', {context})
    await writeFile(process.env.BILLING_SCREENSHOT, Buffer.from(shot.data,'base64'))
  }
  await send('browsingContext.setViewport', {context,viewport:{width:390,height:844}})
  await assert("document.documentElement.scrollWidth<=innerWidth")
  await send('browsingContext.setViewport', {context,viewport:{width:1400,height:1000}})
  await evaluate("document.querySelector('#cloud-bill-toggle').click(); window.scrollTo(0,0)")
  const billCount = await evaluate("window.homeTest.requests.filter(r=>r.path==='/analysis/cloud/billing').length")
  await new Promise(resolve=>setTimeout(resolve,400))
  await assert(`document.querySelector('#cloud-bill-content').hidden && window.homeTest.requests.filter(r=>r.path==='/analysis/cloud/billing').length===${billCount}`)
  console.log('PASS: full-width billing disclosure, lazy read, totals/services, empty month, stale data on error, mobile layout, and no background billing polling.')
  await evaluate(`window.homeTest.cloudUsage={
    'test-cloud':{phases:[{phase:'running',run_id:'run-one',started_at:new Date(Date.now()-3600000).toISOString()}]},
    'prior-cloud':{phases:[...['finalizing','cleaning','log_planning','log_cleaning','refreshing'].map((phase,i)=>({
      phase,run_id:'prior-root',outcome:'complete',started_at:new Date(Date.now()-86400000+i*60000).toISOString()
    })),{phase:'global_summaries',outcome:'complete',run_id:null,finished_at:new Date(Date.now()-86400000+300000).toISOString()}]}
  }; window.homeTest.cloud={controller_online:true,cloud_busy:true,blocking_job_id:'test-cloud',jobs:[{
    id:'test-cloud',status:'running',target:2000000,imported:500000,
    selection:{backend:'batch_spot',mode:'games',n_vms:2},combined_selection:{players:['hikaru','magnuscarlsen']},
    sequence:{automatic:true,times:2,frozen:true,reserved:2000000,requested:2000000,phase:'running',cloud_status:'RUNNING',cloud_completed:1},
    runs:[{id:'run-one',status:'finalizing',cloud_state:'SUCCEEDED',positions:1000000,imported:500000},
      {id:'run-two',status:'running',cloud_state:'RUNNING',positions:1000000,imported:0,
       vm_statuses:[{index:0,positions:500000,state:'RUNNING',preemptions:1,application_failures:0},
         {index:1,positions:500000,state:'RUNNING',preemptions:0,application_failures:0}]}]
  },{id:'prior-cloud',status:'complete',target:2000000,selection:{backend:'batch_spot'},sequence:{automatic:true},runs:[{id:'prior-root',status:'complete'}]}]}`)
  await waitFor("document.querySelector('[data-cloud-machines] .value')?.textContent==='2 VMs'")
  await assert("document.querySelector('[data-cloud-cpus] .value').textContent==='32' && document.querySelector('[data-cloud-machines]').textContent.includes('16 GiB per VM')")
  await assert("document.querySelector('.cloud-status-panel progress').value===500000 && document.querySelector('.cloud-status-fen-count').textContent==='500,000 / 2,000,000'")
  await assert("document.querySelector('.cloud-status-activity.queue-active progress') && !document.querySelector('.cloud-status-panel button, .cloud-status-panel form, .cloud-status-panel details, .cloud-status-panel a')")
  await assert("getComputedStyle(document.querySelector('.cloud-status-panel .status-grid')).gridTemplateColumns===getComputedStyle(document.querySelector('#status .status-grid')).gridTemplateColumns && getComputedStyle(document.querySelector('.cloud-status-activity')).gridRow==='1 / 3'")
  await assert("!document.querySelector('.cloud-status-panel').textContent.includes('test-cloud') && !document.querySelector('.cloud-status-panel').textContent.includes('Combined games')")
  await waitFor("document.querySelector('.cloud-status-eta')?.dataset.etaState==='estimating'")
  await assert("document.querySelector('.cloud-status-eta-label strong').textContent==='~3h 6m' && !document.querySelector('.cloud-status-eta-track') && !document.querySelector('.cloud-status-eta progress, .cloud-status-eta [role=progressbar]')")
  await evaluate("window.homeTest.clockOffsetMs+=35000; window.homeTest.cloud.jobs[0].imported=500500")
  await waitFor("document.querySelector('.cloud-status-panel progress').value===500500")
  await evaluate("window.homeTest.clockOffsetMs+=35000; window.homeTest.cloud.jobs[0].imported=501000")
  await waitFor("document.querySelector('.cloud-status-eta')?.dataset.etaState==='estimating'")
  await assert("document.querySelector('.cloud-status-eta-label strong').textContent.startsWith('~') && !document.querySelector('.cloud-status-eta-track')")
  await assert("document.querySelector('.cloud-status-eta').getBoundingClientRect().top > document.querySelector('.cloud-status-panel progress').getBoundingClientRect().bottom")
  if (process.env.CLOUD_STATUS_SCREENSHOT) {
    const result = await send('browsingContext.captureScreenshot', {context})
    await writeFile(process.env.CLOUD_STATUS_SCREENSHOT, Buffer.from(result.data,'base64'))
  }
  await send('browsingContext.setViewport', {context,viewport:{width:390,height:844}})
  await assert("document.documentElement.scrollWidth<=innerWidth")
  await send('browsingContext.setViewport', {context,viewport:{width:1400,height:1000}})
  await evaluate("window.homeTest.cloudUsage['test-cloud'].phases.push({phase:'cleaning',run_id:'run-one',started_at:new Date(Date.now()).toISOString()}); window.homeTest.cloud.jobs[0].imported=2000000; window.homeTest.cloud.jobs[0].sequence.phase='cleaning'; window.homeTest.cloud.jobs[0].sequence.cloud_status='SUCCEEDED'")
  await waitFor("document.querySelector('[data-cloud-activity]').textContent==='Cleaning'")
  await assert("document.querySelector('.cloud-status-panel progress').value===2000000 && document.querySelector('.cloud-status-activity.queue-active')")
  await waitFor("document.querySelector('.cloud-status-eta-label strong').textContent==='~4 min'")
  await assert("!document.querySelector('.cloud-status-eta-track') && window.homeTest.requests.filter(r=>r.path==='/analysis/cloud/prior-cloud/usage').length===1")
  await evaluate("window.homeTest.failCloudStatus=true")
  await waitFor("document.querySelector('[data-cloud-activity]')?.textContent==='Stale'")
  await assert("document.querySelector('[data-cloud-controller]').textContent==='Unknown' && document.querySelector('.cloud-status-panel progress').value===2000000 && document.querySelector('.cloud-status-activity.queue-stale')")
  await assert("document.querySelector('.cloud-status-eta-label strong').textContent==='Unavailable'")
  await evaluate("window.homeTest.failCloudStatus=false; window.homeTest.cloud.controller_online=false")
  await waitFor("document.querySelector('[data-cloud-controller]').textContent==='Offline'")
  await assert("document.querySelector('.cloud-status-panel').textContent.includes('Google work may continue')")
  await evaluate("window.homeTest.cloud.controller_online=true; window.homeTest.cloud.jobs[0].status='complete'; window.homeTest.cloud.jobs[0].sequence.phase='complete'; window.homeTest.cloud.jobs[0].runs.forEach(r=>r.status='complete'); window.homeTest.cloud.cloud_busy=false; window.homeTest.cloud.blocking_job_id=null")
  await waitFor("document.querySelector('.cloud-status-activity.queue-idle')")
  await assert("document.querySelector('[data-cloud-cpus] .value').textContent==='0' && !document.querySelector('.cloud-status-panel progress')")
  await evaluate("window.homeTest.cloud.jobs=[{id:'run-cloud',status:'running',target:1000,imported:500,selection:{backend:'cloud_run',mode:'all',n_cpus:16},runs:[{id:'run',status:'running',cloud_state:'RUNNING'}]}]; window.homeTest.cloud.cloud_busy=true")
  await waitFor("document.querySelector('[data-cloud-cpus] .value')?.textContent==='16'")
  await assert("document.querySelector('[data-cloud-machines] h3').textContent==='Cloud Run' && document.querySelector('[data-cloud-machines] .value').textContent==='Serverless'")
  await waitFor("document.querySelector('.cloud-status-eta-label strong').textContent==='Not enough history'")
  console.log('PASS: numeric-only ETA includes whole-process timings, works immediately from saved progress, and continues through cleanup; unknown/stale states never reuse a misleading countdown.')
  console.log('PASS: compact local-style GCloud grid; CPU/VM specs; colored progress square; idle, cleanup, stale, offline and Cloud Run states; responsive and read-only.')
  await evaluate("window.homeTest.failStatus=true")
  await waitFor("document.querySelector('.status-banner')?.textContent.includes('Degraded: showing last known values.')")
  await evaluate("document.querySelector('.rail-brand').click()")
  await waitFor("document.querySelector('.dashboard-panel') && !document.querySelector('.status')")
  const statusCount = await evaluate("window.homeTest.requests.filter(r=>r.path==='/status').length")
  const cloudCount = await evaluate("window.homeTest.requests.filter(r=>r.path==='/analysis/cloud').length")
  const usageCount = await evaluate("window.homeTest.requests.filter(r=>r.path.endsWith('/usage')).length")
  await new Promise(resolve => setTimeout(resolve,600))
  await assert(`window.homeTest.requests.filter(r=>r.path==='/status').length===${statusCount}`)
  await assert(`window.homeTest.requests.filter(r=>r.path==='/analysis/cloud').length===${cloudCount}`)
  await assert(`window.homeTest.requests.filter(r=>r.path.endsWith('/usage')).length===${usageCount}`)
  console.log('PASS: dedicated System Status page, degraded state, both polls stop on navigation.')

  await load('/players?player=hikaru')
  await loaded('hikaru')
  await assert("location.pathname==='/' && location.search==='?player=hikaru' && document.querySelector('.dashboard-panel') && document.querySelector('.character-rating-row.selected')")
  await assert("window.homeTest.scrolls.length===0")
  await load('/?player=magnuscarlsen')
  await loaded('magnuscarlsen')
  await waitFor("document.querySelectorAll('[aria-label=\"Rating chart\"] .daily-year-band').length===3")
  await assert("document.querySelector('.behavior-rating-years button').getAttribute('aria-pressed')==='true'")
  await evaluate("window.homeTest.slowPlayer='hikaru'")
  await clickPlayer('hikaru')
  await waitFor("window.homeTest.requests.some(r=>r.path==='/players/hikaru')")
  await clickPlayer('magnuscarlsen')
  await loaded('magnuscarlsen')
  await waitFor("window.homeTest.aborted.includes('hikaru')")
  await assert("!window.homeTest.requests.some(r=>r.path.includes('/hikaru/analysis/'))")
  await new Promise(resolve => setTimeout(resolve,2200))
  await assert("document.querySelector('.profile-item strong').textContent==='magnuscarlsen'")
  console.log('PASS: legacy bookmarks, reload-equivalent deep links and cancelled stale player requests.')

  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 850 } })
  await assert("getComputedStyle(document.querySelector('.players-profile-layout')).gridTemplateColumns.split(' ').length===1")
  await assert("getComputedStyle(document.querySelector('.metric-grid')).gridTemplateColumns.split(' ').length===1")
  await assert("getComputedStyle(document.querySelector('.home-overview')).gridTemplateColumns.split(' ').length===1")
  await assert("document.documentElement.scrollWidth<=window.innerWidth+1")
  await assert("window.homeTest.unexpected.length===0 && window.homeTest.requests.every(r=>r.method==='GET')")
  await evaluate("document.querySelector('.rail-nav a[href=\"/system_status\"]').click()")
  await waitFor("document.querySelector('.system-status-page .status-grid')")
  await assert("getComputedStyle(document.querySelector('.status-grid')).gridTemplateColumns.split(' ').length===1 && document.documentElement.scrollWidth<=window.innerWidth+1")
  console.log('PASS: mobile homepage/player/status layouts; no production requests or write operations.')
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser,'exit'); browser.kill('SIGTERM'); await stopped
  }
  await rm(temporary,{recursive:true,force:true})
}
