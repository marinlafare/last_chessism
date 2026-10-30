import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9297,async ({send,context,evaluate,assert,waitFor})=>{
  await send('browsingContext.setViewport',{context,viewport:{width:1440,height:1000}})
  await send('script.addPreloadScript',{functionDeclaration:`()=>{
    window.repeatAudio={ticks:0,animations:[]}; const animate=Element.prototype.animate;
    Element.prototype.animate=function(...args){const a=animate.apply(this,args);
      if(this.classList.contains('game-review-piece-skin')){a.pause();repeatAudio.animations.push(a)}return a};
    window.AudioContext=class {
      state='running';currentTime=0;
      createGain(){return {gain:{setValueAtTime(){},linearRampToValueAtTime(){},exponentialRampToValueAtTime(){}},connect(){},disconnect(){}}}
      createOscillator(){return {frequency:{setValueAtTime(){}},connect(){},disconnect(){},
        start(){repeatAudio.ticks++},stop(at){if(at===undefined)this.onended?.()}}}
      close(){this.state='closed';return Promise.resolve()}
    }
  }`})
  await send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/gameReview.html',wait:'complete'})
  await waitFor(`document.querySelector('.game-review-board [data-square="e2"]')`)
  await evaluate('new Promise(resolve=>setTimeout(resolve,250))')
  const key=async(key,repeat=true)=>{
    await evaluate(`window.dispatchEvent(new KeyboardEvent('keydown',{key:${JSON.stringify(key)},repeat:${repeat},bubbles:true,cancelable:true}))`)
    await evaluate('new Promise(resolve=>setTimeout(resolve,60))')
  }
  const at=ply=>`document.querySelector('.game-review-moves button:nth-child(${ply})')?.getAttribute('aria-current')==='step'`
  await key('ArrowRight',false);await waitFor(at(1))
  await key('ArrowRight');await waitFor(at(2))
  await assert('repeatAudio.ticks===1 && gameReviewTest.sounds===0')
  await key('ArrowRight');await waitFor(at(3))
  await key('ArrowLeft');await waitFor(at(2))
  await assert('repeatAudio.ticks===3 && gameReviewTest.sounds===0')
  await evaluate(`repeatAudio.animations.forEach(a=>{if(a.playState==='paused')a.finish()})`)
  await evaluate('new Promise(resolve=>setTimeout(resolve,100))')
  await assert('gameReviewTest.sounds===0')
  // Separate rapid presses (repeat=false), direction changes and pointer
  // navigation also replace a superseded normal sound with one immediate tick.
  await key('ArrowRight',false);await waitFor(at(3))
  await key('ArrowRight',false);await waitFor(at(4))
  await key('ArrowLeft',false);await waitFor(at(3))
  await assert('repeatAudio.ticks===5 && gameReviewTest.sounds===0')
  await evaluate(`document.querySelector('.game-review-moves button:nth-child(4)').click()`)
  await waitFor(at(4))
  await assert('repeatAudio.ticks===6 && gameReviewTest.sounds===0')
  const finishMotion=()=>evaluate(`repeatAudio.animations.forEach(a=>{if(a.playState==='paused')a.finish()})`)
  await finishMotion();await waitFor('gameReviewTest.sounds===1')
  // Once a move completes normally, the next single press must not add a tick.
  await key('ArrowRight',false);await waitFor(at(5))
  await finishMotion();await waitFor('gameReviewTest.sounds===2')
  await key('ArrowLeft',false);await waitFor(at(4))
  await finishMotion();await waitFor('gameReviewTest.sounds===3')
  await assert('repeatAudio.ticks===6')
  await evaluate(`document.querySelector('[data-style-id="gameReview.soundToggle"]').click()`)
  await key('ArrowRight');await waitFor(at(5))
  await assert('repeatAudio.ticks===6')
  await evaluate(`document.querySelector('[data-style-id="gameReview.soundToggle"]').click()`)
  for(let i=0;i<5;i++)await key('ArrowLeft')
  await evaluate('repeatAudio.atStart=repeatAudio.ticks')
  await key('ArrowLeft');await assert('repeatAudio.ticks===repeatAudio.atStart')
  await evaluate(`document.querySelector('[aria-label="Close game viewer"]').click()`)
  await key('ArrowRight');await assert('repeatAudio.ticks===repeatAudio.atStart')
  await assert('gameReviewTest.errors.length===0 && gameReviewTest.unexpected.length===0')
  console.log('PASS: held arrows, rapid separate presses, direction changes and mouse navigation click when normal sound is interrupted; settled moves, mute, bounds and close remain correct')
},{reducedMotion:false})
