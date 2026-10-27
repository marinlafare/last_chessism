// Every API response is mocked by profileStylesFixture; no live player/cloud data.
import { readFile } from 'node:fs/promises'
import { browserFixture } from './browser-fixture.mjs'

const read = name => readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url), 'utf8').then(JSON.parse)
const [base, scopes, charts] = await Promise.all(['appearance_registry', 'appearance_scopes', 'appearance_charts'].map(read))
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: Object.fromEntries(
  Object.entries(scopes.scopes).map(([scope, spec]) => [scope, { ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets } }])) }

await browserFixture(9303, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('script.addPreloadScript', { functionDeclaration: `()=>{window.profileStylesRegistry=${JSON.stringify(registry)}}` })
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1000 } })
  const load = () => send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/profileStyles.html', wait: 'complete' })
  const node = target => `document.querySelector('[data-style-id="${target}"]')`
  const photo = node('player.photo'), popup = node('player.profilePopup')
  const key = async value => send('input.performActions', { context, actions: [{ type: 'key', id: 'keyboard', actions: [
    { type: 'keyDown', value }, { type: 'keyUp', value },
  ] }] })
  const openEditor = async target => {
    await evaluate(`${node(target)}.dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:300,clientY:250}))`)
    await waitFor(`document.querySelector('.appearance-property-group[data-appearance-target="${target}"]')`)
  }
  const set = (property, value) => evaluate(`(()=>{const input=document.querySelector('#appearance-${property}');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,${JSON.stringify(value)});input.dispatchEvent(new Event('input',{bubbles:true}));input.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  const closeEditor = () => evaluate(`document.querySelector('[aria-label="Close appearance popup"]').click()`)
  const hover = async target => {
    const point = JSON.parse(await evaluate(`JSON.stringify((()=>{const r=${node(target)}.getBoundingClientRect();return{x:Math.round(r.left+r.width/2),y:Math.round(r.top+12)}})())`))
    await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [{ type: 'pointerMove', origin: 'viewport', ...point }] }] })
  }
  await load()
  await waitFor(`${photo} && ${node('player.metadata.last-play.value')}?.textContent==='OCT 23 2026'`)
  await waitFor(`getComputedStyle(${photo}).backgroundColor==='rgb(8, 8, 8)'`)
  await assert(`${photo}.querySelector('img[data-default-avatar="true"]').title.includes('default image')`)
  await evaluate(`window.profilePhotoRequests=window.profileStylesTest.requests.length; ${photo}.focus()`)
  await key('\uE007') // Enter opens the real button.
  await waitFor(`${popup}?.getAttribute('aria-modal')==='true'`)
  await assert(`${popup}.contains(document.activeElement)`)
  await assert(`document.body.style.overflow==='hidden'`)
  for (const [field, value] of Object.entries({ player: 'example', name: 'Example Player', country: 'MX', title: 'GM',
    location: 'Mexico City', followers: '0', verified: 'No', streamer: 'Yes', games: '1,000', 'scored-positions': '7,000', 'rapid-games': '0' })) {
    await assert(`document.querySelector('[data-profile-field="${field}"] dd').textContent===${JSON.stringify(value)}`)
  }
  await assert(`document.querySelector('[data-profile-field="last-play"] dd').textContent.includes('08:00 AM UTC')`)
  await assert(`window.profileStylesTest.requests.length===window.profilePhotoRequests`)
  // Tab cannot leave the popup; close returns focus without scrolling the page.
  await evaluate(`document.querySelector('[data-profile-field="twitch"] a').focus()`)
  await key('\uE004')
  await assert(`document.activeElement===${node('player.profilePopup.close')}`)
  await key('\uE00C') // Escape
  await waitFor(`!${popup}`)
  await assert(`document.activeElement===${photo} && document.body.style.overflow===''`)
  await evaluate(`document.querySelector('.appearance-account-controls > button').click()`)
  for (const target of ['player.photo', 'player.profile', 'player.hero']) {
    await openEditor(target)
    for (const property of ['background', 'hoverBackground', 'borderColor', 'hoverBorderColor', 'radius']) await assert(`document.querySelector('#appearance-${property}')`)
    await set('background', target === 'player.photo' ? '#123456' : '#102030')
    await set('hoverBackground', target === 'player.photo' ? '#654321' : '#304050')
    await closeEditor()
  }
  await hover('player.photo')
  await waitFor(`getComputedStyle(${photo}).backgroundColor==='rgb(101, 67, 33)' && getComputedStyle(${photo}).transform==='matrix(1, 0, 0, 1, 0, -4)'`)
  await assert(`getComputedStyle(${node('player.profile')}).backgroundColor==='rgb(48, 64, 80)'`)
  await evaluate(`${photo}.focus(); ${photo}.click()`)
  await waitFor(`${popup}`)
  await openEditor('player.profilePopup')
  await set('background', '#234567'); await closeEditor()
  await assert(`getComputedStyle(${popup}).backgroundColor==='rgb(35, 69, 103)'`)
  await evaluate(`${node('player.profilePopup.close')}.click()`)
  await waitFor(`!${popup}`)
  // Pinned photo is independently editable, without copying the large card.
  await evaluate(`document.querySelector('main').style.minHeight='3600px';window.scrollTo({top:1500,behavior:'instant'})`)
  await waitFor(`${node('player.photo.pinned')}`)
  await openEditor('player.photo.pinned')
  await set('borderColor', '#abcdef'); await set('borderWidth', 3); await set('radius', 14)
  await set('hoverBackground', '#132435'); await closeEditor()
  await assert(`getComputedStyle(${node('player.photo.pinned')}).borderColor==='rgb(171, 205, 239)' && getComputedStyle(${node('player.photo.pinned')}).borderRadius==='14px'`)
  await assert(`getComputedStyle(${photo}).borderColor!=='rgb(171, 205, 239)'`)
  await evaluate(`window.scrollTo({top:0,behavior:'instant'});document.querySelector('main').style.minHeight=''`)
  await waitFor(`!${node('player.photo.pinned')}`)
  await evaluate(`[...document.querySelectorAll('button')].find(n=>n.textContent==='Save styles').click()`)
  await waitFor(`localStorage.getItem('profile-styles-fixture')!==null`)
  await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [{ type: 'pointerMove', origin: 'viewport', x: 1, y: 1 }] }] })
  await load()
  await waitFor(`${photo} && getComputedStyle(${photo}).backgroundColor==='rgb(18, 52, 86)'`)
  await assert(`JSON.parse(localStorage.getItem('profile-styles-fixture')).overrides.some(r=>r.target==='player.photo'&&r.property==='hoverBackground'&&r.value.startsWith('#654321'))`)
  await assert(`JSON.parse(localStorage.getItem('profile-styles-fixture')).overrides.some(r=>r.target==='player.photo.pinned'&&r.property==='radius'&&r.value===14)`)
  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 700 } })
  await evaluate(`${photo}.focus(); ${photo}.click()`)
  await waitFor(`${popup}`)
  await assert(`getComputedStyle(${popup}).backgroundColor==='rgb(35, 69, 103)'`)
  await assert(`${popup}.getBoundingClientRect().left>=0 && ${popup}.getBoundingClientRect().right<=innerWidth && ${popup}.scrollWidth<=${popup}.clientWidth`)
  await evaluate(`${node('player.profilePopup.backdrop')}.click()`)
  await waitFor(`!${popup}`)
  await assert(`document.activeElement===${photo}`)
  console.log('PASS: profile popup, no additional fetches, default photo, keyboard/focus/Escape/backdrop, mobile, hover lift, independent hero/photo styles and save/reload; all API requests mocked.')
})
