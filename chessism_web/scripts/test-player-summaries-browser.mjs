import { readFile } from 'node:fs/promises'
import { browserFixture } from './browser-fixture.mjs'

const read = name => readFile(new URL(`../../chessism_api/${name}`, import.meta.url), 'utf8').then(JSON.parse)
const [base, scopes, charts] = await Promise.all(['appearance_registry.json', 'appearance_scopes.json', 'appearance_charts.json'].map(read))
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: {} }
for (const [scope, spec] of Object.entries(scopes.scopes)) registry.scopes[scope] = {
  ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets, ...(scope === base.scope ? base.targets : {}) },
}
await browserFixture(9315, async ({ send, context, evaluate, waitFor, assert }) => {
  await send('script.addPreloadScript', { functionDeclaration: `() => { window.playerSummaryRegistry = ${JSON.stringify(registry)} }` })
  await send('browsingContext.setViewport', { context, viewport: { width: 1400, height: 1000 } })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/player-summaries.html', wait: 'complete' })
  await waitFor(`document.querySelector('[data-style-id="player.analysis.toggle"]:not(:disabled)')`)
  await assert(`playerSummaryTest.requests.some(row=>row.path.endsWith('/latest')) && !playerSummaryTest.requests.some(row=>row.path.includes('/recent'))`)
  await evaluate(`document.querySelector('[data-style-id="player.analysis.toggle"]').click()`)
  await waitFor(`document.querySelector('[data-style-id="player.activityDays.toggle"]:not(:disabled)')`)
  await assert(`!playerSummaryTest.requests.some(row=>row.path.endsWith('/ratings'))`)
  await evaluate(`document.querySelector('[data-style-id="player.activityDays.toggle"]').click()`)
  try { await waitFor(`document.querySelector('.behavior-weekday-hour i[data-balance="positive"]')`) }
  catch (error) { console.log(await evaluate('JSON.stringify({errors:playerSummaryTest.errors,requests:playerSummaryTest.requests,text:document.body.innerText})')); throw error }
  await assert(`getComputedStyle(document.querySelector('i[data-balance="empty"]')).backgroundColor === 'rgba(0, 0, 0, 0)'`)
  await assert(`getComputedStyle(document.querySelector('i[data-balance="empty"]')).visibility === 'hidden'`)
  await assert(`[...document.querySelectorAll('i[data-balance="positive"]')].some(el=>el.style.getPropertyValue('--balance-intensity')==='100%')`)
  await assert(`[...document.querySelectorAll('.behavior-weekday-row:first-child .behavior-weekday-hour i')].slice(0,6).map(el=>Number(getComputedStyle(el).opacity)).join(',') === '0.5,0.25,1,0.5,0.5,0'`)
  await assert(`[...document.querySelectorAll('.behavior-weekday-row:nth-child(2) .behavior-weekday-hour i')].slice(0,2).map(el=>Number(getComputedStyle(el).opacity)).join(',') === '0.5,1'`)
  await assert(`[...document.querySelectorAll('.behavior-weekday-wide-bar i')].every(el=>getComputedStyle(el).opacity==='1')`)
  await evaluate(`document.querySelector('.behavior-weekday-curtain').click()`)
  await waitFor(`document.querySelector('.behavior-weekday-row.expanded .behavior-weekday-hour:not(:disabled)')`)
  await evaluate(`window.dayMode=[...document.querySelectorAll('[data-style-id="player.days.modes"]')].find(el=>el.textContent==='blitz');dayMode.click()`)
  await waitFor(`[...document.querySelectorAll('.behavior-weekday-hour i')].every(el=>getComputedStyle(el).opacity==='0')`)
  await evaluate(`dayMode.click()`)
  await waitFor(`document.querySelector('.behavior-weekday-hour i[data-balance="positive"]')`)
  await evaluate(`document.querySelector('.appearance-account-controls button').click()`)
  await evaluate(`window.sample=document.querySelector('i[data-balance="positive"]');window.beforeColor=getComputedStyle(sample).backgroundColor;document.querySelector('[data-style-id="player.days.palette"]').dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,clientX:700,clientY:300}))`)
  await waitFor(`document.querySelector('#appearance-chartColor1')`)
  await assert(`!document.querySelector('[aria-label="Positive balance base color opacity"]')`)
  await evaluate(`const field=document.querySelector('#appearance-chartColor1');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(field,'#2255ff');field.dispatchEvent(new Event('input',{bubbles:true}));field.dispatchEvent(new Event('change',{bubbles:true}))`)
  await waitFor(`getComputedStyle(sample).backgroundColor !== beforeColor`)
  await assert(`getComputedStyle(sample).opacity === '0.25'`)
  await evaluate(`document.querySelector('.appearance-account-controls button').click();document.querySelector('[data-style-id="player.rating.toggle"]').click()`)
  await waitFor(`playerSummaryTest.requests.some(row=>row.path.endsWith('/ratings'))`)
  await waitFor(`!document.querySelector('[data-style-id="player.rating.toggle"]').disabled`)
  await evaluate(`document.querySelector('#choose-shell').click()`)
  await waitFor(`document.body.innerText.includes('does not have an eligible main-page profile')`)
  await assert(`!document.querySelector('[data-style-id="player.hero"]')`)
  await assert(`!playerSummaryTest.requests.some(row=>row.method==='POST' && row.path.endsWith('/games'))`)
  await assert(`!playerSummaryTest.requests.some(row=>row.path.includes('/shell/') && !row.path.endsWith('/hero'))`)
  await assert(`playerSummaryTest.errors.length===0 && playerSummaryTest.unexpected.length===0`)
  console.log('PASS mocked browser: proportional per-day bin opacity, filter rescaling, transparent empty bins, independently editable balance colors, lazy ratings, and eligible profiles only.')
})
