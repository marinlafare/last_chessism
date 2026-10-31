// Real appearance editor and viewer; style saves and game responses are mocked.
import { readFile } from 'node:fs/promises'
import { browserFixture } from './browser-fixture.mjs'
const read = name => readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url), 'utf8').then(JSON.parse)
const [base, scopes, charts] = await Promise.all(['appearance_registry','appearance_scopes','appearance_charts'].map(read))
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: Object.fromEntries(
  Object.entries(scopes.scopes).map(([id, spec]) => [id, { ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets } }])) }

await browserFixture(9308, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('script.addPreloadScript', { functionDeclaration: `()=>{window.gameReviewRegistry=${JSON.stringify(registry)}}` })
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1000 } })
  const load = () => send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/gameReview.html', wait: 'complete' })
  const node = target => `document.querySelector('[data-style-id="${target}"]')`
  const color = (target, prop = 'color') => `getComputedStyle(${node(target)}).${prop}`
  const click = target => evaluate(`${node(target)}.click()`)
  const close = () => evaluate(`document.querySelector('[aria-label="Close appearance popup"]').click()`)
  const edit = async target => {
    await evaluate(`${node(target)}.dispatchEvent(new MouseEvent('contextmenu',{bubbles:true,cancelable:true,clientX:300,clientY:250}))`)
    await waitFor(`document.querySelector('.appearance-property-group[data-appearance-target="${target}"]')`)
  }
  const set = (prop, value) => evaluate(`(()=>{const input=document.querySelector('#appearance-${prop}');Object.getOwnPropertyDescriptor(input.tagName==='SELECT'?HTMLSelectElement.prototype:HTMLInputElement.prototype,'value').set.call(input,${JSON.stringify(String(value))});input.dispatchEvent(new Event('input',{bubbles:true}));input.dispatchEvent(new Event('change',{bubbles:true}))})()`)
  const hover = async target => {
    const point = JSON.parse(await evaluate(`JSON.stringify((()=>{const r=${node(target)}.getBoundingClientRect();return{x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())`))
    await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [{ type: 'pointerMove', origin: 'viewport', ...point }] }] })
  }
  await load()
  await waitFor(`document.querySelectorAll('.game-review-square').length===64`)
  await waitFor(`getComputedStyle(document.documentElement).getPropertyValue('--surface-2').trim()==='#080808'`)
  for (const [kind, symbol, rgb] of [['best','!','21, 199, 128'],['inaccuracy','?!','201, 168, 76'],['mistake','?','219, 139, 69'],['blunder','??','213, 92, 92']]) {
    const target = `gameReview.move.marker.${kind}`
    await assert(`${node(target)}.textContent===${JSON.stringify(symbol)} && ${color(target)}==='rgb(${rgb})'`)
    await assert(`(()=>{const marker=${node(target)},row=marker.closest('button'),m=marker.getBoundingClientRect(),r=row.getBoundingClientRect(),n=row.querySelector('[data-style-id="gameReview.move.number"]').getBoundingClientRect();return marker.parentElement===row && Math.abs(m.left-r.left-parseFloat(getComputedStyle(row).borderLeftWidth))<1 && m.right<=n.left && Math.abs((m.top+m.bottom-r.top-r.bottom)/2)<1})()`, `${kind} symbol hugs the left rim without overlapping the move number`)
  }
  await assert(`!document.querySelector('.game-review-move-marker') && !document.querySelector('.game-review-moves button.good .game-review-move-annotation') && !document.querySelector('.game-review-moves button:last-child .game-review-move-annotation')`)
  await assert(`${color('gameReview.title')}==='rgb(171, 193, 35)' && ${color('gameReview.panel','backgroundColor')}==='rgb(16, 32, 48)'`)
  // The sound dropdown follows the selected name, not the longest option.
  for (const sound of ['nes','futuristic','piano']) {
    await evaluate(`document.querySelector('[aria-label="Board sound set"]').click()`)
    await evaluate(`document.querySelector('[data-sound-set="${sound}"]').click()`)
    await waitFor(`${node('gameReview.soundSet')}.dataset.selectedLabel===document.querySelector('.game-review-sound-option[aria-selected="true"]').textContent`)
    await assert(`(()=>{const field=${node('gameReview.soundSet')},s=getComputedStyle(field),c=document.createElement('canvas').getContext('2d');c.font=s.font;const expected=c.measureText(field.dataset.selectedLabel).width+parseFloat(s.fontSize)+5+parseFloat(s.paddingLeft)+parseFloat(s.paddingRight)+parseFloat(s.borderLeftWidth)+parseFloat(s.borderRightWidth);return Math.abs(field.getBoundingClientRect().width-expected)<2})()`)
    await evaluate(`gameReviewTest['${sound}Width']=${node('gameReview.soundSet')}.getBoundingClientRect().width`)
  }
  await assert(`gameReviewTest.nesWidth<gameReviewTest.pianoWidth && gameReviewTest.pianoWidth<gameReviewTest.futuristicWidth && gameReviewTest.pianoWidth<100`)
  await assert(`(()=>{const field=${node('gameReview.soundSet')},s=getComputedStyle(field);return field.offsetHeight<=parseFloat(s.lineHeight)+parseFloat(s.paddingTop)+parseFloat(s.paddingBottom)+parseFloat(s.borderTopWidth)+parseFloat(s.borderBottomWidth)+1 && field.offsetHeight<28})()`)
  await evaluate(`document.querySelector('.appearance-account-controls > button').click()`)
  await edit('gameReview.soundSet'); await set('fontSize',18); await close()
  await assert(`${node('gameReview.soundSet')}.getBoundingClientRect().width>gameReviewTest.pianoWidth && getComputedStyle(document.querySelector('[aria-label="Board sound set"]')).fontSize==='18px'`)
  await edit('gameReview.soundSet'); await set('fontSize',10); await close()
  // Check the disabled bar before selecting an evaluated move.
  await edit('gameReview.evaluation.disabledLine'); await set('background', '#654321'); await close()
  await assert(`${color('gameReview.evaluation.disabledLine','backgroundColor')}==='rgb(101, 67, 33)'`)
  await evaluate(`document.querySelector('.game-review-moves button:nth-child(2)').click()`)
  await waitFor(`${node('gameReview.current.accuracy')} && ${node('gameReview.current.engineLine')}`)
  await assert(`[...document.querySelectorAll('.game-review-backdrop [data-style-id]')].every(el=>el.dataset.styleId.startsWith('gameReview.') && gameReviewRegistry.scopes.home.targets[el.dataset.styleId])`)
  await assert(`new Set([...document.querySelectorAll('.game-review-backdrop [data-style-id]')].map(el=>el.dataset.styleId)).size>=70`)
  await waitFor(`${node('gameReview.board.badge.inaccuracy')}`)
  await edit('gameReview.board.badge.inaccuracy')
  for (const prop of ['background','hoverBackground','color','hoverColor','borderColor','hoverBorderColor','borderWidth','radius','padding','fontSize']) await assert(`document.querySelector('#appearance-${prop}')`)
  await set('background','#123456'); await set('hoverBackground','#654321'); await set('borderColor','#abcdef'); await close()
  await assert(`${color('gameReview.board.badge.inaccuracy','backgroundColor')}==='rgb(18, 52, 86)' && ${color('gameReview.move.marker.inaccuracy')}==='rgb(201, 168, 76)'`)
  await hover('gameReview.board.badge.inaccuracy')
  await assert(`${color('gameReview.board.badge.inaccuracy','backgroundColor')}==='rgb(101, 67, 33)'`)
  // All major surfaces expose normal/hover background, border, radius and spacing.
  for (const target of ['gameReview.panel','gameReview.header','gameReview.body','gameReview.boardColumn','gameReview.boardHeader',
    'gameReview.boardFooter','gameReview.stage','gameReview.analysis','gameReview.summary','gameReview.current','gameReview.moves',
    'gameReview.clock.white','gameReview.clock.black','gameReview.soundSet','gameReview.soundToggle','gameReview.fullscreen','gameReview.close',
    'gameReview.chart.container','gameReview.chart.plot']) {
    await edit(target)
    for (const property of ['background','hoverBackground','borderColor','hoverBorderColor','radius','padding','margin','marginLeft','marginRight','marginTop','marginBottom']) await assert(`document.querySelector('#appearance-${property}')`, target)
    await close()
  }
  await edit('gameReview.title'); await set('color','#654321'); await set('textAlign','left'); await close()
  await edit('gameReview.chart.whiteArea'); await set('color','#abcdef'); await close()
  await assert(`getComputedStyle(${node('gameReview.chart.whiteArea')}).fill==='rgb(171, 205, 239)' && getComputedStyle(${node('gameReview.chart.blackArea')}).fill!=='rgb(171, 205, 239)'`)
  await edit('gameReview.chart.line'); await set('color','#2468ac'); await close()
  await assert(`getComputedStyle(${node('gameReview.chart.line')}).stroke==='rgb(36, 104, 172)' && getComputedStyle(${node('gameReview.chart.whiteArea')}).fill==='rgb(171, 205, 239)'`)
  await edit('gameReview.chart.currentPoint'); await set('color','#fefefe'); await set('stroke','#112233'); await close()
  await assert(`getComputedStyle(${node('gameReview.chart.currentPoint')}).fill==='rgb(254, 254, 254)' && getComputedStyle(${node('gameReview.chart.currentPoint')}).stroke==='rgb(17, 34, 51)' && getComputedStyle(${node('gameReview.chart.line')}).stroke==='rgb(36, 104, 172)'`)
  await edit('gameReview.chart.plot')
  await set('reviewChartFocusColor','#cc8844'); await set('reviewChartFocusWidth',3); await set('reviewChartFocusOffset',1); await close()
  await evaluate(`${node('gameReview.chart.plot')}.focus()`)
  await assert(`${color('gameReview.chart.plot','outlineColor')}==='rgb(204, 136, 68)' && ${color('gameReview.chart.plot','outlineWidth')}==='3px' && ${color('gameReview.chart.plot','outlineOffset')}==='1px'`)
  await evaluate(`${node('gameReview.close')}.focus()`)
  await assert(`${color('gameReview.chart.plot','outlineStyle')}==='none'`)
  await assert(`${color('gameReview.title')}==='rgb(101, 67, 33)' && ${color('gameReview.title','textAlign')}==='left' && getComputedStyle(document.querySelector('#outside-title')).color==='rgb(171, 193, 35)'`)
  await edit('gameReview.clock.white.remaining'); await set('color','#33cc66'); await close()
  await assert(`${color('gameReview.clock.white.remaining')}==='rgb(51, 204, 102)' && ${color('gameReview.clock.black.remaining')}==='rgb(18, 58, 188)' && getComputedStyle(document.querySelector('#outside-value')).color==='rgb(18, 58, 188)'`)
  await edit('gameReview.date.year'); await set('background','#123456'); await close()
  await assert(`${color('gameReview.date.year','backgroundColor')}==='rgb(18, 52, 86)' && ${color('gameReview.date.month','backgroundColor')}!=='rgb(18, 52, 86)'`)
  await edit('gameReview.board.square.light'); await set('background','#abcdef'); await set('hoverBackground','#fedcba'); await close()
  await hover('gameReview.board.square.light')
  await assert(`${color('gameReview.board.square.light','backgroundColor')}==='rgb(254, 220, 186)' && ${color('gameReview.board.square.dark','backgroundColor')}==='rgb(181, 136, 99)'`)
  await edit('gameReview.board.rank'); await set('color','#123456'); await close()
  await assert(`${color('gameReview.board.rank')}==='rgb(18, 52, 86)' && ${color('gameReview.board.file')}!=='rgb(18, 52, 86)'`)
  await edit('gameReview.board.piece.white'); await set('reviewPieceOpacity',.7); await close()
  await assert(`${color('gameReview.board.piece.white','opacity')}==='0.7' && ${color('gameReview.board.piece.black','opacity')}==='1'`)
  await edit('gameReview.board'); await set('padding',8); await set('borderWidth',2); await set('radius',12); await close()
  await waitFor(`(()=>{const b=${node('gameReview.board')}.getBoundingClientRect(),s=document.querySelector('.game-review-board [data-square="a8"]').getBoundingClientRect();return Math.abs(s.left-b.left-10)<1})()`)
  await assert(`getComputedStyle(${node('gameReview.board')}).borderRadius==='12px'`)
  await edit('gameReview.evaluation.white'); await set('background','#999999'); await close()
  await assert(`${color('gameReview.evaluation.white','backgroundColor')}==='rgb(153, 153, 153)' && ${color('gameReview.evaluation.black','backgroundColor')}==='rgb(20, 25, 30)'`)
  await edit('gameReview.move'); await set('reviewSelectedBackground','#234567'); await set('reviewSelectedBorder','#987654'); await close()
  await assert(`getComputedStyle(document.querySelector('.game-review-moves button.active')).backgroundColor==='rgb(35, 69, 103)' && getComputedStyle(document.querySelector('.game-review-moves button:first-child')).backgroundColor!=='rgb(35, 69, 103)'`)
  await edit('gameReview.move.notation'); await set('reviewSelectedColor','#55aa88'); await close()
  await assert(`getComputedStyle(document.querySelector('.game-review-moves button.active strong')).color==='rgb(85, 170, 136)'`)
  await edit('gameReview.move.marker.best'); await set('color','#abcdef'); await set('hoverColor','#fedcba'); await close()
  await assert(`${color('gameReview.move.marker.best')}==='rgb(171, 205, 239)' && ${color('gameReview.move.marker.inaccuracy')}==='rgb(201, 168, 76)'`)
  await hover('gameReview.move.marker.best')
  await assert(`${color('gameReview.move.marker.best')}==='rgb(254, 220, 186)'`)
  for (const kind of ['inaccuracy','mistake','blunder']) {
    const target = `gameReview.move.marker.${kind}`
    await edit(target); await set('color','#aabbcc'); await set('hoverColor','#ccbbaa'); await close()
    await assert(`${color(target)}==='rgb(170, 187, 204)'`)
    await hover(target); await assert(`${color(target)}==='rgb(204, 187, 170)'`)
  }
  await edit('gameReview.soundToggle'); await set('reviewMutedColor','#cc8844'); await close()
  await click('gameReview.soundToggle')
  await assert(`${color('gameReview.soundToggle')}==='rgb(204, 136, 68)'`)
  await click('gameReview.flip')
  await assert(`document.querySelector('.game-review-board-header [data-style-id="gameReview.clock.white.remaining"]')`)
  await assert(`${color('gameReview.clock.white.remaining')}==='rgb(51, 204, 102)'`)
  await evaluate(`[...document.querySelectorAll('.appearance-account-controls button')].find(n=>n.textContent==='Save styles').click()`)
  await waitFor(`localStorage.getItem('game-review-styles-fixture') && !document.querySelector('.appearance-account-controls > button').textContent.includes('*')`)
  await load(); await waitFor(`${node('gameReview.title')} && document.querySelectorAll('.game-review-square').length===64`)
  await evaluate(`${node('gameReview.chart.plot')}.focus()`)
  await assert(`${color('gameReview.chart.plot','outlineColor')}==='rgb(204, 136, 68)' && ${color('gameReview.chart.plot','outlineWidth')}==='3px' && ${color('gameReview.chart.plot','outlineOffset')}==='1px'`)
  await assert(`${color('gameReview.title')}==='rgb(101, 67, 33)' && ${color('gameReview.clock.white.remaining')}==='rgb(51, 204, 102)'`)
  await assert(`${color('gameReview.evaluation.disabledLine','backgroundColor')}==='rgb(101, 67, 33)'`)
  await assert(`${node('gameReview.move.marker.best')}.textContent==='!' && ${color('gameReview.move.marker.best')}==='rgb(171, 205, 239)'`)
  for (const kind of ['inaccuracy','mistake','blunder']) await assert(`${color(`gameReview.move.marker.${kind}`)}==='rgb(170, 187, 204)'`)
  await assert(`getComputedStyle(document.querySelector('#outside-title')).color==='rgb(171, 193, 35)' && getComputedStyle(document.querySelector('#outside-panel')).backgroundColor==='rgb(16, 32, 48)'`)
  await assert(`gameReviewTest.errors.length===0 && gameReviewTest.unexpected.length===0`)
  console.log('PASS: real edit popup, independent surfaces/text/states/board styles, inherited preset preservation, save/reload, isolation from unrelated page elements. Mock data only.')
})
