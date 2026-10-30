import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9306, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('script.addPreloadScript', { functionDeclaration: `() => {
    window.gameReviewKeyboardFixture = true;
    window.gameReviewStyles = '.game-review-moves { max-height: 50px; }';
  }` })
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1000 } })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/gameReview.html', wait: 'complete' })
  const key = value => send('input.performActions', { context, actions: [{ type: 'key', id: 'keyboard',
    actions: [{ type: 'keyDown', value }, { type: 'keyUp', value }] }] })
  const at = ply => `document.querySelector('.game-review-moves button:nth-child(${ply})')?.getAttribute('aria-current') === 'step'`
  await waitFor("document.getElementById('review-opener')")
  await evaluate("document.getElementById('review-opener').focus({preventScroll:true})")
  await key('\uE007') // Enter opens from an input that would otherwise consume arrows.
  await waitFor("document.querySelector('.game-review-modal') && typeof gameReviewTest.finishLoading === 'function'")
  await assert("document.activeElement === document.querySelector('.game-review-modal')")
  // The popup is active even while loading, without a pointer click.
  await key('\uE014')
  await assert('!gameReviewTest.backgroundKeys')
  await evaluate('gameReviewTest.finishLoading()')
  await waitFor("document.querySelectorAll('.game-review-moves button').length === 6")
  await assert("document.activeElement === document.querySelector('.game-review-modal')")
  await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse',
    parameters: { pointerType: 'mouse' }, actions: [{ type: 'pointerMove', origin: 'viewport', x: 1, y: 1 }] }] })
  await assert("!document.elementFromPoint(1, 1).closest('.game-review-modal')")
  await key('\uE014'); await waitFor(at(1)) // Right, pointer still outside.
  await key('\uE014'); await waitFor(at(2))
  await key('\uE012'); await waitFor(at(1)) // Left.
  for (let ply = 2; ply <= 6; ply++) { await key('\uE014'); await waitFor(at(ply)) }
  await waitFor("document.querySelector('.game-review-moves').scrollTop > 0")
  await assert(`(() => {
    const list = document.querySelector('.game-review-moves').getBoundingClientRect();
    const row = document.querySelector('.game-review-moves [aria-current="step"]').getBoundingClientRect();
    return row.top >= list.top - 1 && row.bottom <= list.bottom + 1;
  })()`)
  await assert('!gameReviewTest.backgroundKeys && scrollY === 0')
  // Tab stays in the active review, while controls keep their own keyboard behavior.
  await evaluate("document.querySelector('.game-review-moves button:last-child').focus()")
  await key('\uE004')
  await assert("document.activeElement.getAttribute('data-style-id') === 'gameReview.fullscreen'")
  await evaluate("document.querySelector('[aria-label=\"Board sound set\"]').focus()")
  await key('\uE015')
  await assert("document.querySelector('[aria-label=\"Board sound set\"]').getAttribute('aria-expanded') === 'true'")
  await key('\uE012'); await assert(at(6))
  await key('\uE00C') // First Escape closes only the sound menu.
  await assert("document.querySelector('.game-review-modal') && document.querySelector('.game-review-sound-list').hidden")
  await key('\uE00C')
  await waitFor("!document.querySelector('.game-review-modal')")
  await assert("document.activeElement === document.getElementById('review-opener') && !document.body.style.overflow")
  await key('\uE014'); await assert('gameReviewTest.backgroundKeys === 1')
  // Reopening claims focus again; loading/closing never leaves a stale shortcut.
  await key('\uE007')
  await waitFor("document.activeElement === document.querySelector('.game-review-modal')")
  await evaluate('gameReviewTest.finishLoading()')
  await waitFor("document.querySelectorAll('.game-review-moves button').length === 6")
  await key('\uE014'); await waitFor(at(1))
  await key('\uE00C'); await waitFor("!document.querySelector('.game-review-modal')")
  await assert('gameReviewTest.errors.length === 0 && gameReviewTest.unexpected.length === 0')
  console.log('PASS: review focuses immediately, real arrows work without clicking and with pointer outside, moves scroll, Tab stays in review, sound selector keeps its keys, close restores focus and reopening works. Mock data only.')
})
