import { writeFile } from 'node:fs/promises'
import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9314, async ({ send, context, evaluate, assert, waitFor }) => {
  const click = square => evaluate(`document.querySelector('[data-square="${square}"]').click()`)
  const bar = "document.querySelector('.live-score-bar')"
  const fill = "document.querySelector('.live-score-white')"
  const frame = () => evaluate('new Promise(resolve=>requestAnimationFrame(()=>resolve(true)))')
  const settled = () => waitFor(`${fill}.getAnimations().length===0`)
  await send('browsingContext.setViewport', { context, viewport: { width: 1400, height: 1000 } })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/live-analysis.html?controlled', wait: 'complete' })
  await waitFor("document.querySelector('.live-stockfish-toggle') && document.querySelector('.live-board-shell').getBoundingClientRect().width>540")
  await assert("!document.querySelector('.site-header') && !document.body.textContent.includes('Chess analytics workstation') && document.querySelector('.live-analysis-toolbar h1').textContent==='Chessism'")
  await assert("(()=>{const p=document.querySelector('.live-analysis-panel').getBoundingClientRect(),b=document.querySelector('.live-board-shell').getBoundingClientRect();return Math.abs(p.height-innerHeight)<1&&b.width>800&&Math.abs(b.width-b.height)<1&&b.top>=0&&b.bottom<=innerHeight&&document.documentElement.scrollWidth<=innerWidth})()")
  await assert("(()=>{const b=document.querySelector('.live-stockfish-toggle');return b.getAttribute('aria-pressed')==='false'&&b.getAttribute('aria-label')==='Start engine'&&!b.disabled&&b.textContent.trim()===''&&b.querySelector('svg')&&Number(getComputedStyle(b).opacity)<.6})()")
  // The board works while analysis is off, without starting an engine.
  await click('e2'); await click('e4')
  await waitFor("document.querySelector('.live-analysis-footer').textContent.includes('Black to move')")
  await assert('liveTest.pending.length===0 && liveTest.workers.length===0')
  await evaluate("document.querySelector('.live-stockfish-toggle').click()")
  await waitFor('liveTest.pending.length===1')
  await waitFor("getComputedStyle(document.querySelector('.live-stockfish-toggle')).opacity==='1'")
  await assert("document.querySelector('.live-stockfish-toggle').getAttribute('aria-label')==='Stop engine' && getComputedStyle(document.querySelector('.live-stockfish-toggle')).boxShadow!=='none' && getComputedStyle(document.querySelector('.live-stockfish-toggle svg')).color==='rgb(63, 221, 143)'")
  await evaluate('liveTest.publish(400)')
  await waitFor(`${bar}.querySelector('span').textContent==='+4.00'`)
  await settled()
  const previous = await evaluate(`${bar}.style.getPropertyValue('--white-score-fraction')`)
  await click('e7'); await click('e5')
  await waitFor('liveTest.pending.length===2')
  await assert(`${bar}.style.getPropertyValue('--white-score-fraction')===${JSON.stringify(previous)} && ${bar}.querySelector('span').textContent==='+4.00' && ${bar}.title.includes('Previous position')`)
  await assert("document.querySelector('.live-analysis-footer').textContent.includes('Best --')")
  await evaluate('liveTest.publish(-900, 0)')
  await assert(`${bar}.querySelector('span').textContent==='+4.00'`)
  // Confirm intermediate frames, not just a CSS duration declaration.
  await evaluate(`window.scoreSamples=[];window.sampling=true;function sample(){scoreSamples.push(new DOMMatrixReadOnly(getComputedStyle(${fill}).transform).m22);if(sampling)requestAnimationFrame(sample)}sample();liveTest.publish(-300)`)
  await waitFor(`${bar}.querySelector('span').textContent==='-3.00'`)
  await frame(); await settled()
  await evaluate('window.sampling=false')
  await assert('scoreSamples.length>=3 && scoreSamples.some(v=>v>.3&&v<.7) && scoreSamples.every((v,i,a)=>!i||v<=a[i-1]+.01)')
  await assert(`${bar}.title==='Evaluation from White’s perspective'`)
  if (process.env.LIVE_LAYOUT_SCREENSHOT) {
    const shot = await send('browsingContext.captureScreenshot', { context })
    await writeFile(process.env.LIVE_LAYOUT_SCREENSHOT, Buffer.from(shot.data, 'base64'))
  }
  await evaluate("document.querySelector('.live-stockfish-toggle').click()")
  await click('g1'); await click('f3')
  await waitFor("document.querySelector('.live-analysis-footer').textContent.includes('Black to move')")
  await assert(`${bar}.querySelector('span').textContent==='-3.00' && liveTest.pending.length===2 && liveTest.workers.length===0`)
  const squareCenter = async square => JSON.parse(await evaluate(`JSON.stringify((()=>{const r=document.querySelector('[data-square="${square}"]').getBoundingClientRect();return{x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())`))
  const from = await squareCenter('b8'), to = await squareCenter('c6')
  await evaluate("window.dragEvents=[];['dragstart','dragover','drop','dragend'].forEach(type=>document.addEventListener(type,e=>{dragEvents.push(type);if(type==='dragstart')window.dragTransfer=e.dataTransfer},true))")
  await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [
    { type: 'pointerMove', ...from }, { type: 'pointerDown', button: 0 }, { type: 'pause', duration: 80 },
    { type: 'pointerMove', x: from.x + 10, y: from.y + 10, duration: 80 },
    { type: 'pointerMove', ...to, duration: 200 }, { type: 'pointerUp', button: 0 },
  ] }] })
  // Firefox BiDi can leave the native drag open after pointerUp. Complete the
  // DOM drop in that case; this still exercises react-chessboard's real handler.
  await evaluate(`(()=>{if(dragEvents.includes('drop'))return;const from=document.querySelector('[data-square=b8] [data-piece]'),to=document.querySelector('[data-square=c6]');to.dispatchEvent(new DragEvent('drop',{bubbles:true,cancelable:true,dataTransfer:dragTransfer,clientX:${to.x},clientY:${to.y}}));from?.dispatchEvent(new DragEvent('dragend',{bubbles:true,dataTransfer:dragTransfer}))})()`)
  await waitFor("document.querySelector('.live-analysis-footer').textContent.includes('White to move')")
  await assert('liveTest.pending.length===2 && liveTest.workers.length===0')
  console.log('PASS: full-page square board, in-panel title, dimmed/green icon, playable with engine off, held evaluation, no stale PV or late-score updates, and real smooth animation frames.')
  for (const [width, height] of [[900, 600], [390, 844], [320, 640]]) {
    await send('browsingContext.setViewport', { context, viewport: { width, height } })
    await frame(); await frame()
    try {
      await waitFor(`(()=>{const s=document.querySelector('.live-analysis-stage').getBoundingClientRect(),b=document.querySelector('.live-board-shell').getBoundingClientRect();return b.width>100&&Math.abs(b.width-b.height)<1&&b.left>=s.left&&b.right<=s.right+1&&b.top>=s.top&&b.bottom<=s.bottom+1&&document.documentElement.scrollWidth<=innerWidth})()`)
    } catch (error) {
      console.error(await evaluate(`JSON.stringify({width:innerWidth,overflow:document.documentElement.scrollWidth,stage:document.querySelector('.live-analysis-stage').getBoundingClientRect(),board:document.querySelector('.live-board-shell').getBoundingClientRect()})`))
      throw error
    }
    if (width < 720) {
      await settled()
      await assert(`(()=>{const m=new DOMMatrixReadOnly(getComputedStyle(${fill}).transform);return m.m22===1&&m.m11>0&&m.m11<1})()`)
    }
  }
  await assert('liveTest.requests.length===0 && liveTest.errors.length===0')
  console.log('PASS: board fits shorter desktop and mobile layouts without horizontal overflow; all requests blocked.')
}, { reducedMotion: false })

await browserFixture(9314, async ({ send, context, assert, waitFor }) => {
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/live-analysis.html?controlled', wait: 'complete' })
  await waitFor("document.querySelector('.live-score-white')")
  await assert("getComputedStyle(document.querySelector('.live-score-white')).transitionDuration==='0s'")
  console.log('PASS: reduced-motion preference disables evaluation animation.')
}, { reducedMotion: true })
