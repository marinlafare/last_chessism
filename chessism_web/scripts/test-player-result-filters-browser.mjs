// Real chart clicks and game explorer with synthetic, deliberately delayed API responses.
import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9306, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('script.addPreloadScript', { functionDeclaration: `()=>{window.chartResultFiltersTest=true;window.fixtureErrors=[];window.addEventListener('error',e=>fixtureErrors.push(e.message));window.addEventListener('unhandledrejection',e=>fixtureErrors.push(String(e.reason)))}` })
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1100 } })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/chartLift.html', wait: 'complete' })
  const node = selector => `document.querySelector(${JSON.stringify(selector)})`
  const click = selector => evaluate(`${node(selector)}.click()`)
  const key = value => send('input.performActions', { context, actions: [{ type: 'key', id: 'keyboard', actions: [{ type: 'keyDown', value }, { type: 'keyUp', value }] }] })
  const pointerClick = async (selector, fraction = .2) => {
    await evaluate(`${node(selector)}.scrollIntoView({block:'center',behavior:'instant'})`)
    const coords = JSON.parse(await evaluate(`JSON.stringify((()=>{const r=${node(selector)}.getBoundingClientRect();return{x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height*${fraction})}})())`))
    await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [
      { type: 'pointerMove', origin: 'viewport', ...coords }, { type: 'pointerDown', button: 0 }, { type: 'pointerUp', button: 0 },
    ] }] })
  }
  const requests = `chartLiftTest.requests.filter(r=>r.path.endsWith('/explore-games'))`
  const last = `${requests}.at(-1).body`
  const rows = `document.querySelectorAll('[data-game-key]')`
  const filter = result => `[data-style-id="gameExplorer.results.${result}"]`
  const ready = result => waitFor(`${rows}.length===30 && [...${rows}].every(row=>row.dataset.gameKey.startsWith('${result || 'all'}-'))`)
  await waitFor(`document.querySelector('[data-style-id="player.activityHours.toggle"]')`)
  await click('[data-style-id="player.activityHours.toggle"]')
  await waitFor(`document.querySelectorAll('.behavior-hour-mark').length===72`)
  // Keep only blitz selected: result clicks must not override the chart's modes.
  await click('[data-style-id="player.hours.modes"]:first-child')
  await click('[data-style-id="player.hours.modes"]:last-child')
  for (const result of ['win', 'draw', 'loss']) {
    const mark = `.behavior-hour-mark:has(> .behavior-bar-${result})`
    const before = await evaluate(`${requests}.length`)
    await pointerClick(`${mark} > .behavior-hour-mark-hit`, result === 'loss' ? .12 : .2)
    await ready(result)
    await assert(`${requests}.length===${before + 1} && ${last}.scope==='hour' && ${last}.hour===0 && ${last}.result==='${result}'`)
    await assert(`JSON.stringify(${last}.modes)==='["blitz"]' && ${last}.analyzed_only===false`)
    await assert(`!document.querySelector('.game-explorer-result-filters')`)
    await click('[aria-label="Close game explorer"]')
    await evaluate(`${node(mark)}.focus({preventScroll:true})`)
    await key(result === 'draw' ? ' ' : '\uE007')
    await ready(result)
    try { await assert(`${requests}.length===${before + 2} && ${last}.result==='${result}'`) }
    catch (error) { throw Error(error.message + '\n' + await evaluate(`JSON.stringify({requests:${requests},active:document.activeElement?.className})`)) }
    await click('[aria-label="Close game explorer"]')
  }
  // Whole-hour keyboard access still includes all results.
  await evaluate(`${node('.behavior-bar-hitbox')}.focus()`)
  await key('\uE007'); await ready(null)
  await assert(`${last}.result===null`)
  await click('[aria-label="Close game explorer"]')
  await click('[data-style-id="player.activityDays.toggle"]')
  await click('[data-style-id="player.days.modes"]:nth-child(2)') // bullet + rapid
  await click('.behavior-weekday-curtain')
  await waitFor(`!${node('.behavior-weekday-hour')}.disabled`)
  await pointerClick('.behavior-weekday-hour')
  await ready(null)
  await assert(`document.querySelectorAll('.game-explorer-result-filters button').length===3`)
  await assert(`!document.querySelector('.game-explorer-result-filters [aria-pressed="true"]')`)
  for (const result of ['win', 'draw', 'loss']) {
    await pointerClick(filter(result)); await ready(result)
    await assert(`${last}.scope==='weekday_hour' && ${last}.weekday===1 && ${last}.hour===0 && ${last}.result==='${result}'`)
    await assert(`JSON.stringify(${last}.modes)==='["bullet","rapid"]' && ${last}.analyzed_only===false`)
    await assert(`${node(filter(result))}.getAttribute('aria-pressed')==='true' && document.activeElement===${node(filter(result))}`)
    await assert(`document.querySelectorAll('.game-explorer-result-filters [aria-pressed="true"]').length===1`)
  }
  // Scroll pagination retains the result filter and keyset cursor.
  await evaluate(`${node('.game-explorer-list')}.scrollTop=100000`)
  await waitFor(`${rows}.length===35`)
  await assert(`${last}.result==='loss' && ${last}.cursor==='loss-page-2' && JSON.stringify(${last}.modes)==='["bullet","rapid"]'`)
  await pointerClick(filter('loss')); await ready(null)
  await assert(`${last}.result===null && !${last}.cursor && ${node('.game-explorer-list')}.scrollTop===0`)
  // A delayed first page cannot replace a newer filter's rows.
  await evaluate(`chartLiftTest.holdResult='draw';chartLiftTest.release=null`)
  await pointerClick(filter('draw')); await waitFor(`typeof chartLiftTest.release==='function'`)
  await pointerClick(filter('win')); await ready('win')
  await evaluate(`chartLiftTest.holdResult=null;chartLiftTest.release();new Promise(resolve=>setTimeout(resolve,100))`)
  await ready('win')
  // A delayed pagination page also cannot leak rows into a newly selected result.
  await evaluate(`chartLiftTest.holdPage=true;chartLiftTest.release=null;${node('.game-explorer-list')}.scrollTop=100000`)
  await waitFor(`typeof chartLiftTest.release==='function'`)
  await pointerClick(filter('loss')); await ready('loss')
  await evaluate(`chartLiftTest.holdPage=false;chartLiftTest.release();new Promise(resolve=>setTimeout(resolve,100))`)
  await ready('loss')
  await assert(`!${last}.cursor && ${node('.game-explorer-list')}.scrollTop===0`)
  await assert(`chartLiftTest.unexpected.length===0 && fixtureErrors.length===0 && !document.querySelector('[role="alert"]')`)
  console.log('PASS: Hours W/D/L mouse and keyboard clicks; Days result toggles, inherited modes, filtered pagination, reset and stale-response isolation. Mock data only.')
})
