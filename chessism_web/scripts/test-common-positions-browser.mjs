// Actual Positions page; the existing fixture intercepts every API request.
import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9346, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('browsingContext.navigate', { context, wait: 'complete',
    url: 'http://localhost:6789/tests/fixtures/scoredPositions.html?count=0' })
  await waitFor(`document.querySelector('.common-positions-toggle')`)
  await evaluate(`window.commonTest = {
    panel: document.querySelector('.common-positions-panel'),
    button: document.querySelector('.common-positions-toggle'),
    content: document.querySelector('.common-positions-content'),
  }`)
  await assert(`commonTest.panel.nextElementSibling.matches('.scored-rating-advantage-panel')`)
  await assert(`commonTest.button.textContent.trim() === 'Neo4j test'
    && commonTest.button.getAttribute('aria-expanded') === 'false' && commonTest.content.hidden`)
  await assert(`commonTest.button.getAttribute('aria-controls') === commonTest.content.id
    && commonTest.content.getAttribute('aria-labelledby') === commonTest.button.id`)
  for (const width of [1440, 390]) {
    await send('browsingContext.setViewport', { context, viewport: { width, height: 1000 } })
    await assert(`Math.abs(commonTest.button.getBoundingClientRect().width - commonTest.panel.clientWidth) < 1`)
    await assert(`Math.abs(commonTest.panel.getBoundingClientRect().width - commonTest.panel.parentElement.clientWidth) < 1`)
  }
  await evaluate(`commonTest.button.click()`)
  await waitFor(`!commonTest.content.hidden && commonTest.button.getAttribute('aria-expanded') === 'true'`)
  await waitFor(`commonTest.content.textContent.includes('No collapsed map has been published yet.')`)
  await assert(`commonTest.content.querySelector('.ply-map') && commonTest.content.getBoundingClientRect().height >= 100`)
  await evaluate(`commonTest.button.focus()`)
  await send('input.performActions', { context, actions: [{ type: 'key', id: 'keyboard', actions: [
    { type: 'keyDown', value: '\uE007' }, { type: 'keyUp', value: '\uE007' },
  ] }] })
  await waitFor(`commonTest.content.hidden && commonTest.button.getAttribute('aria-expanded') === 'false'`)
  await assert(`new Set([commonTest.panel, commonTest.button, commonTest.content].map(el => el.dataset.styleId)).size === 3`)
  await assert(`!window.scoredTest.requests.some(r => /advantage_by_rating|common_positions/.test(r.pathname))`)
  await assert(`window.scoredTest.requests.some(r=>r.pathname==='/graph-explorer/ply-map')`)
  console.log('PASS: full-width disclosure above Advantage by rating, lazy graph explorer, click/Enter, desktop/mobile, independent style targets, only mocked reads.')
})
