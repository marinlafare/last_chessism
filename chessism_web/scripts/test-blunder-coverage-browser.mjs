import { browserFixture } from './browser-fixture.mjs'
await browserFixture(9278, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/gameReview.html', wait: 'complete' })
  await waitFor("document.querySelector('[data-style-id=\"gameReview.summary.blunders.value\"]')?.textContent === '1'")
  await evaluate("window.gameReviewTest.coverage = { white: { assessed_moves: 10, unassessed_moves: 2, complete: false, detected_blunders: 0 } }; window.gameReviewTest.select({ gameId: 2, playerName: 'WhitePlayer' })")
  await waitFor("document.querySelector('[data-style-id=\"gameReview.summary.blunders.value\"]')?.textContent === '0 detected · partial'")
  await assert("document.querySelector('[data-style-id=\"gameReview.summary.blunders.value\"]').title === '10 moves assessed; 2 unassessed.'")
  await evaluate("window.gameReviewTest.pending = true; window.gameReviewTest.select({ gameId: 1, playerName: 'WhitePlayer' })")
  await waitFor("document.querySelector('[data-style-id=\"gameReview.summary.blunders.value\"]')?.textContent === 'Updating…'")
  await assert("!window.gameReviewTest.unexpected.length && !window.gameReviewTest.errors.length")
  console.log('PASS: partial blunder counts are labeled, coverage is exposed, stale counts show updating; all API requests mocked')
})
