import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9298, async ({send,context,evaluate,assert,waitFor}) => {
  await send('browsingContext.setViewport',{context,viewport:{width:1440,height:1000}})
  await send('script.addPreloadScript',{functionDeclaration:`()=>{
    window.reviewAnimations=[];const animate=Element.prototype.animate;
    Element.prototype.animate=function(...args){const a=animate.apply(this,args);
      if(this.classList.contains('game-review-piece-skin')){a.pause();reviewAnimations.push(a)}return a}
  }`})
  await send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/gameReview.html',wait:'complete'})
  await waitFor("document.querySelector('.game-review-board [data-square=\"e2\"]')")
  await evaluate('new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))')
  await evaluate('new Promise(resolve=>setTimeout(resolve,250))')
  await evaluate(`document.querySelector('.game-review-moves button').click()`)
  await evaluate('new Promise(resolve=>setTimeout(resolve,100))')
  await waitFor('reviewAnimations.length > 0')
  await assert('gameReviewTest.sounds === 0')
  await evaluate('reviewAnimations.forEach(a=>a.finish())')
  await waitFor('gameReviewTest.sounds === 1')
  await assert('gameReviewTest.soundDuringMotion === false')
  await assert(`document.querySelectorAll('.game-review-moves [aria-current="step"]').length === 1`)
  await assert(`getComputedStyle(document.querySelector('.game-review-moves [aria-current="step"]')).boxShadow !== 'none'`)
  await evaluate(`reviewAnimations=[];document.querySelector('.game-review-moves button:nth-child(2)').click()`)
  await waitFor('reviewAnimations.length > 0')
  await assert('gameReviewTest.sounds === 1')
  await evaluate('reviewAnimations.forEach(a=>a.finish())')
  await waitFor('gameReviewTest.sounds === 2')
  await assert('gameReviewTest.soundDuringMotion === false')
  await evaluate(`reviewAnimations=[];document.querySelector('.game-review-moves button:nth-child(3)').click()`)
  await waitFor('reviewAnimations.length > 0')
  await evaluate(`document.querySelector('[aria-label="Close game viewer"]').click()`)
  await evaluate('new Promise(resolve=>setTimeout(resolve,500))')
  await assert('gameReviewTest.sounds === 2 && gameReviewTest.errors.length === 0 && gameReviewTest.unexpected.length === 0')
  console.log('PASS: sound follows completed motion, current move is highlighted, closing cancels pending audio; mocked data only')
}, {reducedMotion:false})
