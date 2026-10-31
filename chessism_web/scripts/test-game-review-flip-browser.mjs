// Actual chessboard, clocks and review summary; no live game data or audio.
import { readFile } from 'node:fs/promises'
import { browserFixture } from './browser-fixture.mjs'
import { appearanceCss } from '../src/appearance/styleCore.js'

const read = name => readFile(new URL(`../../chessism_api/${name}.json`, import.meta.url), 'utf8').then(JSON.parse)
const [base, scopes] = await Promise.all(['appearance_registry', 'appearance_scopes'].map(read))
const registry = { ...base, scopes: { home: { targets: scopes.common_targets } } }
const css = appearanceCss(registry, [
  { scope: 'home', target: 'gameReview.flip', property: 'background', value: '#123456' },
  { scope: 'home', target: 'gameReview.flip', property: 'hoverBackground', value: '#654321' },
  { scope: 'home', target: 'gameReview.flip', property: 'radius', value: 8 },
  { scope: 'home', target: 'gameReview.player.white', property: 'color', value: '#abcdef' },
  { scope: 'home', target: 'gameReview.player.black', property: 'color', value: '#fedcba' },
])

await browserFixture(9307, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('script.addPreloadScript', { functionDeclaration: `()=>{window.gameReviewStyles=${JSON.stringify(css)}}` })
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1000 } })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/gameReview.html', wait: 'complete' })
  const node = selector => `document.querySelector(${JSON.stringify(selector)})`
  const flip = '[data-style-id="gameReview.flip"]', footer = '.game-review-board-footer'
  const click = selector => evaluate(`${node(selector)}.click()`)
  const key = value => send('input.performActions', { context, actions: [{ type: 'key', id: 'keyboard', actions: [{ type: 'keyDown', value }, { type: 'keyUp', value }] }] })
  const bottomColor = color => `Boolean(${node('.game-review-board [data-square="a1"]')} && ${node('.game-review-board [data-square="a8"]')} && (${node('.game-review-board [data-square="a1"]')}.getBoundingClientRect().top > ${node('.game-review-board [data-square="a8"]')}.getBoundingClientRect().top)===${color === 'white'})`
  await waitFor(`document.querySelector('.game-review-board [data-square="a1"]')`)
  await waitFor(`getComputedStyle(document.documentElement).getPropertyValue('--surface-2').trim()==='#080808'`)
  await assert(bottomColor('white'))
  await assert(`document.querySelectorAll('${footer} .game-review-player-name').length===2`)
  for (const color of ['white', 'black']) await assert(`${node(`${footer} .game-review-player-name.${color}`)}.textContent.includes('${color === 'white' ? 'WhitePlayer' : 'BlackPlayer'}')`)
  await assert(`getComputedStyle(${node(flip)}).backgroundColor==='rgb(18, 52, 86)' && getComputedStyle(${node(flip)}).borderRadius==='8px'`)
  await assert(`getComputedStyle(${node('.game-review-player-name.white')}).color==='rgb(171, 205, 239)'`)
  await assert(`getComputedStyle(${node('.game-review-player-name.black')}).color==='rgb(254, 220, 186)'`)
  await click('.game-review-moves button:nth-child(2)')
  await waitFor(`document.querySelector('.game-review-board [data-square="e5"] [data-piece="bP"]')`)
  await evaluate(`gameReviewTest.summary=document.querySelector('.game-review-summary').textContent;gameReviewTest.playedSounds=gameReviewTest.sounds`)
  await assert(`${node('.game-review-eval')}.getAttribute('aria-label')==='Position evaluation: +1.00'`)
  await assert(`${node(`${footer} .game-review-clock.white strong`)}.textContent==='2:55'`)
  const point = JSON.parse(await evaluate(`JSON.stringify((()=>{const r=${node(flip)}.getBoundingClientRect();return{x:Math.round(r.left+r.width/2),y:Math.round(r.top+r.height/2)}})())`))
  await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [
    { type: 'pointerMove', origin: 'viewport', ...point }, { type: 'pointerDown', button: 0 }, { type: 'pointerUp', button: 0 },
  ] }] })
  await waitFor(bottomColor('black'))
  await assert(`getComputedStyle(${node(flip)}).backgroundColor==='rgb(101, 67, 33)'`)
  await assert(`${node(flip)}.getAttribute('aria-pressed')==='true' && ${node(flip)}.title.includes('White at the bottom')`)
  await assert(`${node('.game-review-board-header .game-review-clock.white strong')}.textContent==='2:55'`)
  await assert(`${node(`${footer} .game-review-clock.black strong`)}.textContent==='2:58'`)
  await assert(`${node(`${footer} [data-board-side="bottom"]`)}.textContent.includes('BlackPlayer')`)
  await assert(`${node('.game-review-eval')}.getAttribute('aria-label')==='Position evaluation: -1.00'`)
  await assert(`${node('.game-review-summary')}.textContent===gameReviewTest.summary && gameReviewTest.summary.includes('win')`)
  await assert(`${node('.game-review-moves button:nth-child(2)')}.classList.contains('active') && gameReviewTest.requests.length===1 && gameReviewTest.sounds===gameReviewTest.playedSounds`)
  await evaluate(`${node(flip)}.focus()`); await key('\uE007')
  await waitFor(bottomColor('white'))
  await key(' '); await waitFor(bottomColor('black'))
  await key('\uE012') // Left still navigates one ply, independent of orientation.
  await waitFor(`${node('.game-review-moves button:first-child')}.classList.contains('active')`)
  await assert(`${node('.game-review-board-header .game-review-clock.white strong')}.textContent==='2:55' && ${node(`${footer} .game-review-clock.black strong`)}.textContent==='3:00'`)
  await click('[data-style-id="gameReview.fullscreen"]')
  await waitFor(`${node('.game-review-modal')}.classList.contains('maximized')`)
  await assert(bottomColor('black'))
  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 750 } })
  await waitFor(`${node('.game-review-board')}.getBoundingClientRect().width<390`)
  await assert(`(()=>{const b=${node('.game-review-board')}.getBoundingClientRect(),f=${node(footer)}.getBoundingClientRect();return f.top>=b.bottom-1&&f.right<=innerWidth&&f.left>=0&&${node(footer)}.scrollWidth<=${node(footer)}.clientWidth})()`)
  await click(flip); await waitFor(bottomColor('white'))
  // A new game resets the flip; the selected player's identity is independent.
  await evaluate(`gameReviewTest.select({gameId:2,playerName:'BlackPlayer'})`)
  await waitFor(`gameReviewTest.requests.length===2 && document.querySelector('.game-review-clock.black')`)
  await waitFor(bottomColor('black'))
  await assert(`${node('.game-review-summary')}.textContent.includes('loss') && ${node('.game-review-summary')}.textContent.includes('58.00')`)
  await click(flip); await waitFor(bottomColor('white'))
  await assert(`${node('.game-review-summary')}.textContent.includes('loss') && gameReviewTest.requests.length===2`)
  await click('[aria-label="Close game viewer"]')
  await waitFor(`!document.querySelector('.game-review-modal')`)
  await evaluate(`gameReviewTest.select({gameId:2,playerName:'BlackPlayer'})`)
  await waitFor(`gameReviewTest.requests.length===3 && document.querySelector('.game-review-board [data-square="a1"]')`)
  await waitFor(bottomColor('black'))
  await assert(`gameReviewTest.errors.length===0 && gameReviewTest.unexpected.length===0`)
  console.log('PASS: pointer/keyboard board flip, both names, clocks, scores, unchanged player results/moves, no flip fetch/audio, reset, fullscreen, mobile and independent styles. Mock games only.')
})
