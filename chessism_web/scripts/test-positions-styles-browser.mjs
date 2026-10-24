// Mocked running job, appearance saves and registry updates. No live API access.
import { readFile } from 'node:fs/promises'
import { browserFixture } from './browser-fixture.mjs'
const read = name => readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url), 'utf8').then(JSON.parse)
const [base, scopes, charts] = await Promise.all(['appearance_registry', 'appearance_scopes', 'appearance_charts'].map(read))
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: Object.fromEntries(
  Object.entries(scopes.scopes).map(([id, spec]) => [id, { ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets } }])) }

await browserFixture(9298, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('script.addPreloadScript', { functionDeclaration: `() => {
    window.cloudStyleRegistry=${JSON.stringify(registry)}; window.prompt=()=> 'Test';
  }` })
  await send('browsingContext.setViewport', { context, viewport: { width: 1280, height: 1000 } })
  const navigate = () => send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/cloudAnalysis.html?appearance=1&independentStyles=1', wait: 'complete' })
  const node = id => `document.querySelector('[data-style-id="${id}"]')`
  const open = async id => {
    await evaluate(`${node(id)}.dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:300,clientY:250}))`)
    await waitFor(`document.querySelector('.appearance-property-group')?.dataset.appearanceTarget==='${id}'`)
  }
  const set = async (property, value) => {
    await evaluate(`(()=>{const n=document.querySelector('#appearance-${property}');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(n,'${value}');n.dispatchEvent(new Event('input',{bubbles:true}));n.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  }
  const enable = async () => {
    await waitFor(`document.querySelector('.appearance-account-controls button')?.disabled===false`)
    await evaluate(`document.querySelector('.appearance-account-controls button').click()`)
  }
  await navigate(); await enable()
  await evaluate(`document.querySelector('#analysis-gcloud-toggle').click()`)
  await waitFor(`${node('cloud.job.title')}`)
  await assert(`${node('cloud.heading')}.tagName==='H2' && ${node('cloud.job.title')}.tagName==='H3'`)
  // Every heading has its own rounding; the button also exposes its heading box.
  await open('cloud.toggle')
  await assert(`!!document.querySelector('#appearance-radius') && !!document.querySelector('#appearance-related-radius')`)
  await open('cloud.heading'); await set('radius', 18); await set('background', '#123456')
  await assert(`getComputedStyle(${node('cloud.heading')}).borderRadius==='18px' && getComputedStyle(${node('cloud.job.title')}).backgroundColor!=='rgb(18, 52, 86)'`)
  await open('cloud.job.title'); await set('radius', 7); await set('background', '#abcdef')
  await assert(`getComputedStyle(${node('cloud.job.title')}).borderRadius==='7px' && getComputedStyle(${node('cloud.heading')}).backgroundColor==='rgb(18, 52, 86)'`)
  // Simulate a stale registry then an API deployment while a draft is dirty.
  await evaluate(`cloudStyleRegistry.universalProperties=cloudStyleRegistry.universalProperties.filter(x=>x!=='radius');
    for(const spec of Object.values(cloudStyleRegistry.scopes.positions.targets))spec.properties=spec.properties.filter(x=>x!=='radius');
    window.dispatchEvent(new Event('focus'))`)
  await waitFor(`!document.querySelector('#appearance-radius')`)
  await evaluate(`window.cloudStyleRegistry=${JSON.stringify(registry)};window.dispatchEvent(new Event('focus'))`)
  await waitFor(`!!document.querySelector('#appearance-radius')`)
  await assert(`getComputedStyle(${node('cloud.job.title')}).borderRadius==='7px' && getComputedStyle(${node('cloud.heading')}).borderRadius==='18px'`)
  await evaluate(`document.querySelector('[aria-label="Close appearance popup"]').click();[...document.querySelectorAll('button')].find(n=>n.textContent==='Save styles').click()`)
  await waitFor(`!document.querySelector('.appearance-account-controls').textContent.includes('*')`)
  await navigate(); await waitFor(`${node('cloud.heading')} && getComputedStyle(${node('cloud.heading')}).borderRadius==='18px'`)
  await assert(`getComputedStyle(${node('cloud.job.title')}).borderRadius==='7px'`)
  await assert(`cloudTest.job.status==='running' && !cloudTest.requests.some(r=>r.method==='POST' && !r.path.endsWith('/appearance/save'))`)
  console.log('PASS: separate GCloud/progress headings, button and heading radii, independent edits, dirty-registry refresh, save/reload; running job unchanged.')
})
