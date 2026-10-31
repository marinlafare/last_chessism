import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9331, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1000 } })
  await send('script.addPreloadScript', { functionDeclaration: `()=>{
    window.badgeAnimations=[];window.badgeAnimationStart=0;const original=Element.prototype.animate;
    Element.prototype.animate=function(...args){const animation=original.apply(this,args);
      if(this.classList.contains('game-review-piece-skin')){animation.pause();badgeAnimations.push(animation)}return animation}
  }` })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/gameReview.html', wait: 'complete' })
  await waitFor(`document.querySelectorAll('.game-review-square').length===64`)
  await evaluate(`new Promise(resolve=>setTimeout(resolve,250))`)
  const badge = `document.querySelector('.game-review-quality-badge')`
  const clickMove = n => evaluate(`badgeAnimationStart=badgeAnimations.length;document.querySelector('.game-review-moves button:nth-child(${n})').click()`)
  const moving = () => waitFor('badgeAnimations.length>badgeAnimationStart')
  const finish = () => evaluate(`badgeAnimations.forEach(animation=>{if(animation.playState!=='idle')animation.finish()})`)
  const check = async (square, symbol, label) => {
    await waitFor(`${badge}?.textContent===${JSON.stringify(symbol)}`)
    await assert(`${badge}.title===${JSON.stringify(label)} && ${badge}.getAttribute('aria-label')===${JSON.stringify(`${label} move on ${square}`)}`)
    await assert(`document.querySelectorAll('.game-review-quality-badge').length===1`)
    await assert(`(()=>{const b=${badge}.getBoundingClientRect(),s=document.querySelector('.game-review-board [data-square="${square}"]').getBoundingClientRect();return b.left>=s.left && b.top>=s.top && b.right<=s.right+1 && b.bottom<=s.bottom+1 && Math.abs(b.width-b.height)<1})()`)
    await assert(`getComputedStyle(${badge}).borderRadius==='50%'`)
  }
  await assert(`!${badge}`)
  for (const [n, square, symbol, label] of [[1,'e4','!','Best'],[2,'e5','?!','Inaccuracy'],[3,'f3','?','Mistake'],[4,'c6','??','Blunder'],[5,'c4','✓','Good']]) {
    await clickMove(n)
    await moving()
    await assert(`!${badge}`) // No badge ahead of the animated piece.
    await finish(); await check(square, symbol, label)
  }
  await evaluate(`document.querySelector('[aria-label="Flip board"]').click()`)
  await check('c4', '✓', 'Good')
  await send('browsingContext.setViewport', { context, viewport: { width: 820, height: 900 } })
  await evaluate(`new Promise(resolve=>setTimeout(resolve,250))`)
  await check('c4', '✓', 'Good')
  await clickMove(6); await moving(); await finish()
  await assert(`!${badge}`) // Unassessed moves have no invented classification.
  await clickMove(5); await moving(); await finish(); await check('c4','✓','Good')
  await clickMove(4); await moving()
  await clickMove(3); await moving(); await finish(); await check('f3','?','Mistake')
  await evaluate(`badgeAnimationStart=badgeAnimations.length;window.dispatchEvent(new KeyboardEvent('keydown',{key:'ArrowLeft',bubbles:true}))`)
  await moving(); await finish(); await check('e5','?!','Inaccuracy')
  await evaluate(`document.querySelector('[aria-label="Close game viewer"]').click()`)
  await assert(`!${badge} && gameReviewTest.errors.length===0 && gameReviewTest.unexpected.length===0 && gameReviewTest.requests.length===1`)
  console.log('PASS: all five badges, post-animation placement, flip/resize, unassessed moves, backward/rapid/keyboard navigation, no extra requests.')
}, { reducedMotion: false })
