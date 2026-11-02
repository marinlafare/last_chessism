import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9381, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/cudaTerminalTitle.html', wait: 'complete' })
  await waitFor('window.cudaTitleTest && document.querySelector(".cuda-terminal-title")')
  await evaluate("cudaTitleTest.title='Calculating repeated positions on RTX 4060';cudaTitleTest.render()")
  await waitFor("document.querySelector('.cuda-terminal-title').dataset.typing === 'true' && document.querySelector('.cuda-terminal-text').textContent.length > 0")
  await assert("document.querySelector('.cuda-terminal-text').textContent.length < cudaTitleTest.title.length && document.querySelector('[role=status]').textContent === cudaTitleTest.title")
  await assert("getComputedStyle(document.querySelector('.cuda-terminal-line .cuda-terminal-cursor')).animationName === 'none'")
  const height = await evaluate("document.querySelector('.cuda-terminal-title').getBoundingClientRect().height")
  await waitFor("document.querySelector('.cuda-terminal-text').textContent === cudaTitleTest.title")
  await assert(`document.querySelector('.cuda-terminal-title').getBoundingClientRect().height === ${height}`)
  await assert("getComputedStyle(document.querySelector('.cuda-terminal-line .cuda-terminal-cursor')).animationName === 'cuda-terminal-blink'")
  await assert("getComputedStyle(document.querySelector('.cuda-terminal-line .cuda-terminal-cursor')).animationDuration === '3s'")
  await assert("(()=>{const style=getComputedStyle(document.querySelector('.cuda-terminal-line .cuda-terminal-cursor'));return parseFloat(style.height)>parseFloat(style.width) && style.backgroundColor === style.color})()")
  await evaluate('cudaTitleTest.render()')
  await assert("document.querySelector('.cuda-terminal-title').dataset.typing === 'false'")
  await evaluate("cudaTitleTest.title='Preparing behavioral and ending inputs';cudaTitleTest.render()")
  await waitFor("document.querySelector('.cuda-terminal-title').dataset.typing === 'true'")
  await evaluate("cudaTitleTest.title='Publishing salience';cudaTitleTest.render()")
  await waitFor("document.querySelector('.cuda-terminal-text').textContent === 'Publishing salience'")
  await assert("document.querySelectorAll('[role=status]').length === 1 && document.querySelector('.cuda-terminal-title').dataset.title === 'Publishing salience'")
  await evaluate("cudaTitleTest.active=false;cudaTitleTest.title='Complete';cudaTitleTest.render()")
  await waitFor("document.querySelector('.cuda-terminal-text').textContent === 'Complete'")
  await assert("getComputedStyle(document.querySelector('.cuda-terminal-line .cuda-terminal-cursor')).animationName === 'none'")
  console.log('PASS: step typing, solid/blinking block cursor, stable height, stable polling, rapid step replacement, accessible complete title, terminal cursor stops')
}, { reducedMotion: false })

await browserFixture(9382, async ({ send, context, assert, evaluate, waitFor }) => {
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/cudaTerminalTitle.html', wait: 'complete' })
  await waitFor('window.cudaTitleTest && document.querySelector(".cuda-terminal-title")')
  await assert("matchMedia('(prefers-reduced-motion: reduce)').matches && document.querySelector('.cuda-terminal-text').textContent === cudaTitleTest.title")
  await evaluate("cudaTitleTest.title='Finalizing salience';cudaTitleTest.render()")
  await waitFor("document.querySelector('.cuda-terminal-text').textContent === 'Finalizing salience'")
  await assert("document.querySelector('.cuda-terminal-title').dataset.typing === 'false' && getComputedStyle(document.querySelector('.cuda-terminal-line .cuda-terminal-cursor')).animationName === 'none'")
  console.log('PASS: reduced-motion preference shows the whole title immediately without blinking')
}, { reducedMotion: true })

await browserFixture(9383, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/positions.html', wait: 'complete' })
  await waitFor("document.querySelector('.positions-main > .cuda-summary-panel') && document.querySelector('#analysis-gcloud-toggle')")
  for (const width of [1280, 390]) {
    await send('browsingContext.setViewport', { context, viewport: { width, height: 900 } })
    await assert(`(()=>{const cuda=document.querySelector('.cuda-summary-panel'), cloud=cuda.previousElementSibling;
      return !cuda.nextElementSibling && cloud.contains(document.querySelector('#analysis-gcloud-toggle')) &&
        Math.abs(cuda.getBoundingClientRect().top-cloud.getBoundingClientRect().bottom-3*parseFloat(getComputedStyle(document.documentElement).fontSize))<1})()`)
  }
  await evaluate("document.querySelector('#analysis-gcloud-toggle').click()")
  await assert("document.querySelector('.cuda-summary-panel').previousElementSibling.contains(document.querySelector('#analysis-gcloud-toggle'))")
  await assert("!positionsTest.requests.some(r=>r.method==='POST')")
  console.log('PASS: CUDA follows GCloud with a 3rem gap at desktop/mobile widths; no jobs submitted')
})
