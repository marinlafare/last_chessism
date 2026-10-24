// Real charts and pointer events. Synthetic API data only, including popup clicks.
import { readFile } from 'node:fs/promises'
import { browserFixture } from './browser-fixture.mjs'
import { appearanceCss } from '../src/appearance/styleCore.js'

const read = name => readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url), 'utf8').then(JSON.parse)
const [base, scopes, charts, interactions] = await Promise.all([
  'appearance_registry', 'appearance_scopes', 'appearance_charts', 'appearance_chart_interactions',
].map(read))
const targets = { ...scopes.common_targets, ...charts.targets, ...scopes.scopes.home.targets }
for (const group of interactions.groups) for (const id of group.targets) if (targets[id]) {
  targets[id] = { ...targets[id], chartInteraction: group.families.flatMap(family => interactions.families[family]),
    properties: [...targets[id].properties, 'barHoverColor', 'barHoverBorder'] }
}
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: { home: { targets } } }
const css = appearanceCss(registry, ['chart.outcomes', 'player.scores.chart', 'player.accuracyDistribution.chart', 'player.days.row']
  .map(target => ({ scope: 'home', target, property: 'barHoverColor', value: '#123456' })))

for (const reducedMotion of [false, true]) await browserFixture(reducedMotion ? 9305 : 9304, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('script.addPreloadScript', { functionDeclaration: `()=>{window.chartLiftStyles=${JSON.stringify(css)};window.chartLiftErrors=[];window.addEventListener('error',e=>window.chartLiftErrors.push(e.message));window.addEventListener('unhandledrejection',e=>window.chartLiftErrors.push(String(e.reason)))}` })
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1100 } })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/chartLift.html', wait: 'complete' })
  const node = selector => `document.querySelector(${JSON.stringify(selector)})`
  const neutral = element => `['none','matrix(1, 0, 0, 1, 0, 0)'].includes(getComputedStyle(${element}).transform)`
  const lifted = selector => reducedMotion ? neutral(node(selector)) : `getComputedStyle(${node(selector)}).transform==='matrix(1, 0, 0, 1, 0, -4)'`
  const click = selector => evaluate(`${node(selector)}.click()`)
  const move = point => send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' },
    actions: [{ type: 'pointerMove', origin: 'viewport', ...point }] }] })
  const hover = async (selector, fraction = .25) => {
    await evaluate(`${node(selector)}.scrollIntoView({block:'center',behavior:'instant'})`)
    const point = JSON.parse(await evaluate(`JSON.stringify((()=>{const r=${node(selector)}.getBoundingClientRect();return{x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height*${fraction})}})())`))
    await move(point)
    return point
  }
  const key = value => send('input.performActions', { context, actions: [{ type: 'key', id: 'keyboard', actions: [{ type: 'keyDown', value }, { type: 'keyUp', value }] }] })
  const keyboardFocus = async selector => {
    await evaluate(`${node(selector)}.focus()`)
    await key('\uE004')
    await send('input.performActions', { context, actions: [{ type: 'key', id: 'keyboard', actions: [
      { type: 'keyDown', value: '\uE008' }, { type: 'keyDown', value: '\uE004' },
      { type: 'keyUp', value: '\uE004' }, { type: 'keyUp', value: '\uE008' },
    ] }] })
    await assert(`document.activeElement===${node(selector)} && ${node(selector)}.matches(':focus-visible')`)
  }
  const pointerClick = () => send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [{ type: 'pointerDown', button: 0 }, { type: 'pointerUp', button: 0 }] }] })
  try { await waitFor(`document.querySelectorAll('.score-distribution-column').length===13 && document.querySelectorAll('.blunder-distribution-column').length===7`) }
  catch (error) { throw Error(error.message + '\n' + await evaluate(`JSON.stringify({errors:window.chartLiftErrors,body:document.body.innerText,requests:window.chartLiftTest?.requests})`)) }
  await waitFor(`getComputedStyle(document.documentElement).getPropertyValue('--surface-2').trim()==='#080808'`)
  await assert(`matchMedia('(prefers-reduced-motion: reduce)').matches===${reducedMotion}`)
  await click('[data-style-id="player.activityHours.toggle"]')
  await waitFor(`document.querySelectorAll('.behavior-hour-mark').length===72`)
  for (const result of ['win', 'loss', 'draw']) {
    const bar = `.behavior-hours-chart .behavior-bar-${result}`, hit = `.behavior-hour-mark:has(> .behavior-bar-${result}) > .behavior-hour-mark-hit`
    await hover(hit, result === 'loss' ? .12 : .2)
    await waitFor(lifted(bar))
    await assert(`getComputedStyle(${node(bar)}).fill==='rgb(18, 52, 86)'`)
    for (const other of ['win', 'loss', 'draw'].filter(item => item !== result)) await waitFor(neutral(node(`.behavior-hours-chart .behavior-bar-${other}`)))
  }
  const hourAxis = '.behavior-hours-chart .hourly-axis-tick'
  const axisBefore = await evaluate(`JSON.stringify(${node(hourAxis)}.getBoundingClientRect().toJSON())`)
  await evaluate(`window.hourHoverSamples=[]; window.hourHoverUntil=performance.now()+400; const sample=()=>{chartLiftTest.hourSamples=(chartLiftTest.hourSamples||[]).concat(getComputedStyle(document.querySelector('.behavior-hours-chart .behavior-bar-draw')).transform);if(performance.now()<window.hourHoverUntil)requestAnimationFrame(sample)}; requestAnimationFrame(sample)`)
  await evaluate(`new Promise(resolve=>setTimeout(resolve,450))`)
  if (!reducedMotion) await assert(`chartLiftTest.hourSamples.length>5 && chartLiftTest.hourSamples.every(value=>value==='matrix(1, 0, 0, 1, 0, -4)')`)
  await assert(`JSON.stringify(${node(hourAxis)}.getBoundingClientRect().toJSON())===${JSON.stringify(axisBefore)}`)
  await pointerClick()
  await waitFor(`document.querySelector('.game-explorer-modal')`)
  await assert(`chartLiftTest.requests.some(r=>r.body?.scope==='hour'&&r.body.hour===0)`)
  await click('[aria-label="Close game explorer"]')
  await click('[data-style-id="player.activityDays.toggle"]')
  await click('.behavior-weekday-curtain')
  await waitFor(`!${node('.behavior-weekday-hour')}.disabled`)
  const day = '.behavior-weekday-hour', square = `${day} > i`
  const opacity = await evaluate(`getComputedStyle(${node(square)}).opacity`)
  await hover(day, .97) // Near the base: the fixed hit area must prevent flicker.
  await waitFor(lifted(square))
  await assert(`getComputedStyle(${node(square)}).opacity===${JSON.stringify(opacity)}`)
  await assert(`getComputedStyle(${node('.behavior-weekday-stage')}).overflow==='visible'`)
  await assert(neutral(node('.behavior-weekday-hour:nth-child(2) > i')))
  await pointerClick()
  await waitFor(`document.querySelector('.game-explorer-modal')`)
  await assert(`chartLiftTest.requests.some(r=>r.body?.scope==='weekday_hour'&&r.body.hour===0)`)
  await click('[aria-label="Close game explorer"]')
  for (const family of ['score', 'blunder']) {
    const column = `.${family}-distribution-column`, hit = `.${family}-distribution-hit`, bar = `.${family}-distribution-bar`
    await hover(hit, .99)
    await waitFor(lifted(bar))
    await assert(`getComputedStyle(${node(bar)}).fill==='rgb(18, 52, 86)'`)
    await evaluate(`new Promise(resolve=>setTimeout(resolve,250))`)
    await assert(lifted(bar))
    await assert(neutral(`[...document.querySelectorAll(${JSON.stringify(bar)})][1]`))
    await pointerClick()
    await waitFor(`document.querySelector('${family === 'score' ? '.score-positions-modal' : '.game-explorer-modal'}')`)
    await click(family === 'score' ? '[aria-label="Close position list"]' : '[aria-label="Close game explorer"]')
    await move({ x: 1, y: 1 })
    await keyboardFocus(column)
    await waitFor(lifted(bar))
    await key('\uE007')
    await waitFor(`document.querySelector('${family === 'score' ? '.score-positions-modal' : '.game-explorer-modal'}')`)
    await click(family === 'score' ? '[aria-label="Close position list"]' : '[aria-label="Close game explorer"]')
    await evaluate(`${node(column)}.blur()`)
    // Very short and empty bins retain their hit areas/disabled behavior.
    const tinyHit = `[...document.querySelectorAll('${hit}')].at(-2)`
    await assert(`${tinyHit}.height.baseVal.value>=14`)
    await evaluate(`window.tinyMark=${tinyHit}.parentElement.querySelector('${bar}'); ${tinyHit}.scrollIntoView({block:'center',behavior:'instant'})`)
    const tinyPoint=JSON.parse(await evaluate(`JSON.stringify((()=>{const r=${tinyHit}.getBoundingClientRect();return{x:Math.round(r.left+r.width/2),y:Math.round(r.bottom-2)}})())`))
    await move(tinyPoint)
    await waitFor(reducedMotion ? neutral('window.tinyMark') : `getComputedStyle(window.tinyMark).transform==='matrix(1, 0, 0, 1, 0, -4)'`)
    await assert(`document.querySelector('${column}[aria-disabled="true"]')?.querySelector('${hit}')===null`)
  }
  await assert(`chartLiftTest.unexpected.length===0 && !document.querySelector('[role="alert"]')`)
  console.log(`PASS: ${reducedMotion ? 'reduced motion' : 'animated lift'}; independent Hours bars, day bins, both Stockfish histograms, saved hover colors, stable targets, popup clicks and keyboard navigation.`)
}, { reducedMotion })
