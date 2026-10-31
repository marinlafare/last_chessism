// Real local-analysis UI with mocked requests. No jobs are submitted.
import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9312, async ({ send, context, evaluate, assert, waitFor }) => {
  for (const [width, height] of [[1400, 1000], [390, 844]]) {
    await send('browsingContext.setViewport', { context, viewport: { width, height } })
    await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/positions.html', wait: 'complete' })
    await waitFor("document.querySelector('#analysis-local-toggle')")
    await evaluate("document.querySelector('#analysis-local-toggle').click()")
    await waitFor("document.querySelector('.local-analyze-button')?.getBoundingClientRect().width>0")
    const centered = `(()=>{const b=document.querySelector('.local-analyze-button').getBoundingClientRect(),p=document.querySelector('.local-selection-form').getBoundingClientRect();return Math.abs(b.left+b.width/2-p.left-p.width/2)<1&&b.width<=p.width})()`
    await assert(centered)
    await evaluate("(()=>{const n=document.querySelector('select[aria-label=\"Local FEN selection\"]');n.value='player';n.dispatchEvent(new Event('change',{bubbles:true}))})()")
    await waitFor("document.querySelector('.local-selection-form input[type=text]')")
    await assert(centered)
    await assert("!document.querySelector('.site-footer') && !document.body.textContent.includes('Local Chessism stack') && !document.querySelector('a[href=\"/api/docs\"]')")
    await assert("document.documentElement.scrollWidth<=innerWidth && !positionsTest.errors.length && !positionsTest.requests.some(r=>r.method==='POST')")
  }
  console.log('PASS: centered Analyze locally button in all/player modes on desktop/mobile, no footer, no analysis submissions. Mock data only.')
})
