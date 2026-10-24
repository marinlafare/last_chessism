import { browserFixture } from './browser-fixture.mjs'
import { writeFile } from 'node:fs/promises'

const start = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1'
const e4 = 'rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1'
const e5 = 'rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq - 0 2'
const capture = ['4k3/5p2/8/8/2B5/8/8/4K3 w - - 0 1', '4k3/5B2/8/8/8/8/8/4K3 b - - 0 1']
const castle = ['4k3/8/8/8/8/8/8/4K2R w K - 0 1', '4k3/8/8/8/8/8/8/5RK1 b - - 1 1']
const ep = ['4k3/8/8/3pP3/8/8/8/4K3 w - d6 0 1', '4k3/8/3P4/8/8/8/8/4K3 b - - 0 1']
const promotion = ['1r2k3/P7/8/8/8/8/8/4K3 w - - 0 1', '1N2k3/8/8/8/8/8/8/4K3 b - - 0 1']

await browserFixture(9309, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('browsingContext.setViewport', { context, viewport:{width:1000,height:800} })
  await send('browsingContext.navigate', {context,url:'http://localhost:6789/tests/fixtures/boardMotion.html',wait:'complete'})
  const piece = square => `document.querySelector('.game-review-board [data-square="${square}"] .game-review-piece-skin')`
  const current = square => `boardMotionTest.animations.filter(a=>a.node===${piece(square)}).at(-1)`
  const frame = () => evaluate(`new Promise(resolve=>requestAnimationFrame(()=>resolve(true)))`)
  const set = state => evaluate(`boardMotionTest.set(${JSON.stringify(state)})`)
  const moving = square => waitFor(`${piece(square)} && ${current(square)}`)
  const clearRecords = () => evaluate(`boardMotionTest.animations=[]`)
  const finish = async () => {
    await evaluate(`boardMotionTest.animations.forEach(a=>{if(a.animation.playState!=='idle')a.animation.finish()})`)
    await waitFor(`document.querySelector('.game-review-board').getAnimations({subtree:true}).length===0`)
  }
  const reset = async (position, id) => {
    await set({id,position,ply:0,orientation:'white',animationDuration:200,width:560})
    await waitFor(`!document.querySelector('.game-review-board').getAnimations({subtree:true}).length`)
    await frame(); await frame(); await clearRecords()
  }
  await waitFor(`document.querySelectorAll('.game-review-piece-skin').length===32`)
  await evaluate(`boardMotionTest.pause=true`)
  await set({position:e4,ply:1}); await moving('e4')
  await assert(`${current('e4')}.options.duration===200 && ${current('e4')}.options.easing==='cubic-bezier(0.65, 0, 0.35, 1)'`)
  await assert(`Math.abs(${piece('e4')}.getBoundingClientRect().top-document.querySelector('[data-square="e2"]').getBoundingClientRect().top)<1`)
  await evaluate(`${current('e4')}.animation.currentTime=100`); await frame()
  await assert(`Math.abs(${piece('e4')}.getBoundingClientRect().top-document.querySelector('[data-square="e3"]').getBoundingClientRect().top)<1`)
  await assert(`(()=>{const node=${piece('e4')},r=node.getBoundingClientRect();return document.elementFromPoint(r.left+r.width/2,r.top+r.height/2).closest('.game-review-piece-skin')===node})()`)
  if (process.env.BOARD_MOTION_SCREENSHOT) {
    const shot = await send('browsingContext.captureScreenshot',{context})
    await writeFile(process.env.BOARD_MOTION_SCREENSHOT,Buffer.from(shot.data,'base64'))
  }
  // A quick reversal starts from where the piece is visible, not e4 or e2.
  await evaluate(`boardMotionTest.midY=${piece('e4')}.getBoundingClientRect().top`)
  await set({position:start,ply:0}); await moving('e2')
  await assert(`Math.abs(${piece('e2')}.getBoundingClientRect().top-boardMotionTest.midY)<1`)
  await finish()
  await assert(`${piece('e2')}.style.willChange==='' && ${piece('e2')}.style.zIndex===''`)
  // A second move does not snap an unrelated piece that is still moving.
  await clearRecords(); await set({position:e4,ply:1}); await moving('e4')
  await evaluate(`${current('e4')}.animation.currentTime=80;boardMotionTest.first=${current('e4')}`); await frame()
  await set({position:e5,ply:2}); await moving('e5')
  await assert(`boardMotionTest.first.animation.playState==='paused' && boardMotionTest.first.node===${piece('e4')}`)
  await finish()
  // Captures fade the captured piece; undo restores it without duplicate pieces.
  await reset(capture[0],'capture'); await set({position:capture[1],ply:1}); await moving('f7')
  await assert(`${current('f7')}.animation.playState==='paused' && Math.abs(${piece('f7')}.getBoundingClientRect().left-document.querySelector('[data-square="c4"]').getBoundingClientRect().left)<1`)
  await assert(`document.querySelectorAll('.game-review-capture-layer > *').length===1 && document.querySelector('.game-review-capture-layer [data-piece="bP"]')`)
  await assert(`boardMotionTest.animations.some(a=>a.frames[1].opacity===0 && a.options.duration===120)`)
  await evaluate(`${current('f7')}.animation.currentTime=100`); await frame()
  await set({position:capture[0],ply:0}); await moving('c4')
  await assert(`document.querySelectorAll('.game-review-capture-layer > *').length===0 && ${piece('f7')}.querySelector('[data-piece="bP"]')`)
  await assert(`${current('f7')}.frames[0].opacity===0`)
  await finish()
  await reset(castle[0],'castle'); await set({position:castle[1],ply:1}); await moving('g1'); await moving('f1')
  await assert(`boardMotionTest.animations.length===2`); await finish()
  await reset(ep[0],'ep'); await set({position:ep[1],ply:1}); await moving('d6')
  await assert(`document.querySelector('.game-review-capture-layer [data-piece="bP"]')`); await finish()
  await reset(promotion[0],'promotion'); await set({position:promotion[1],ply:1}); await moving('b8')
  await assert(`${current('b8')}.animation.playState==='paused'`)
  await assert(`${piece('b8')}.querySelector('[data-piece="wN"]') && document.querySelector('.game-review-capture-layer [data-piece="bR"]')`); await finish()
  // Long jumps do not send unrelated pieces flying across the board.
  await reset(start,'jump'); await set({position:e5,ply:20}); await waitFor(`${piece('e5')}`)
  await assert(`boardMotionTest.animations.length===0`)
  // Flip, resize and reduced motion cancel active movement immediately.
  await reset(start,'flip'); await set({position:e4,ply:1}); await moving('e4')
  await set({orientation:'black'}); await waitFor(`boardMotionTest.animations.every(a=>a.animation.playState==='idle')`)
  await assert(`document.querySelector('[data-square="a1"]').getBoundingClientRect().top<document.querySelector('[data-square="a8"]').getBoundingClientRect().top`)
  await reset(start,'resize'); await set({position:e4,ply:1}); await moving('e4')
  await set({width:320}); await waitFor(`document.querySelector('.game-review-board-renderer').getBoundingClientRect().width===320`)
  await waitFor(`boardMotionTest.animations.every(a=>a.animation.playState==='idle')`)
  await set({animationDuration:0,position:start,ply:0}); await waitFor(`${piece('e2')}`); await clearRecords()
  await set({position:e4,ply:1}); await waitFor(`${piece('e4')}`)
  await assert(`boardMotionTest.animations.length===0`)
  // Measure real, unpaused animation frames, not just configured durations.
  await reset(start,'natural'); await evaluate(`boardMotionTest.pause=false`)
  await set({position:e4,ply:1}); await moving('e4')
  await evaluate(`new Promise(resolve=>{const samples=[];function sample(){samples.push(${piece('e4')}.getBoundingClientRect().top);if(${current('e4')}.animation.playState==='finished'){boardMotionTest.samples=samples;resolve(true)}else requestAnimationFrame(sample)}requestAnimationFrame(sample)})`)
  await assert(`boardMotionTest.samples.length>=3 && boardMotionTest.samples.every((y,i,a)=>!i||y<=a[i-1]+0.1)`)
  // Release animations and overlays when the viewer closes.
  await evaluate(`boardMotionTest.pause=true`); await set({position:start,ply:0}); await moving('e2')
  await evaluate(`boardMotionTest.mount(false)`); await waitFor(`!document.querySelector('.game-review-board')`)
  await assert(`boardMotionTest.animations.every(a=>['idle','finished'].includes(a.animation.playState)) && !document.getAnimations().length`)
  await assert(`!boardMotionTest.errors.length && !boardMotionTest.requests.length`)
  console.log('PASS: eased 200 ms motion, live-position reversal, rapid navigation, captures/undo, castling, en passant, promotion, jumps, flip/resize, reduced motion, real frames and unmount cleanup. Mock boards only.')
}, {reducedMotion:false})

await browserFixture(9310, async ({send,context,evaluate,assert,waitFor}) => {
  await send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/gameReview.html',wait:'complete'})
  await waitFor(`document.querySelector('.game-review-board [data-square="e2"] [data-piece="wP"]')`)
  await evaluate(`document.querySelector('.game-review-moves button:first-child').click()`)
  await waitFor(`document.querySelector('.game-review-board [data-square="e4"] [data-piece="wP"]')`)
  await assert(`matchMedia('(prefers-reduced-motion: reduce)').matches && !document.querySelector('.game-review-board').getAnimations({subtree:true}).length`)
  await assert(`gameReviewTest.requests.length===1 && !gameReviewTest.errors.length && !gameReviewTest.unexpected.length`)
  console.log('PASS: actual game viewer honors the OS reduced-motion setting without refetching game data.')
}, {reducedMotion:true})
