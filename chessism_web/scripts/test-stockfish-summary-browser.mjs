import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9251,async({send,context,evaluate,assert,waitFor})=>{
  await send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/stockfishSummary.html',wait:'complete'})
  const toggle=`document.querySelector('[data-style-id="player.measures.toggle"]')`
  const accuracy=`document.querySelector('[data-style-id="player.accuracy.toggle"]')`
  const score=`document.querySelector('.player-score-distribution')`
  await waitFor(toggle)
  await assert('summaryTest.requests.length===0')
  await evaluate(`${toggle}.click()`)
  await waitFor(`${score}?.textContent.includes('Summary queued') && ${accuracy}`)
  await assert(`${accuracy}.disabled && ${accuracy}.querySelector('.player-analysis-disclosure-spinner')`)
  await assert(`!${score}.querySelector('svg') && summaryTest.unexpected.length===0`)
  await evaluate('summaryTest.pending=false;summaryTest.ready=true')
  await waitFor(`${score}.querySelector('svg') && !${accuracy}.disabled`)
  await assert(`${score}.textContent.includes('Mate in 2') && ${score}.textContent.includes('Distance unavailable')`)
  await assert(`document.querySelector('[data-style-id="player.scores.mates.for"]').textContent==='2'`)
  await evaluate('summaryTest.stale=true')
  await waitFor(`${score}.textContent.includes('last complete summary')`)
  await assert(`${score}.querySelector('svg') && !summaryTest.requests.some(p=>p.includes('quality-calendar')||p.includes('refresh=true')||p.includes('daily-'))`)
  await evaluate(`${toggle}.click()`)
  await evaluate('new Promise(resolve=>setTimeout(resolve,400))')
  const count=await evaluate('summaryTest.requests.length')
  await evaluate('new Promise(resolve=>setTimeout(resolve,600))')
  await assert(`summaryTest.requests.length===${count}`)
  await evaluate(`${toggle}.click();summaryTest.stale=false`)
  await waitFor(`!${score}.textContent.includes('last complete summary') && summaryTest.requests.length>${count}`)
  await assert('summaryTest.unexpected.length===0')
  console.log('PASS: queued/ready/stale mate counters, Accuracy spinner, read-only readiness, no calendar/salience fetch, hidden polling cancellation and reopen.')
})
