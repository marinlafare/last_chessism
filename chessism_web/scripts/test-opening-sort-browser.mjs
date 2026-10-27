import { browserFixture } from './browser-fixture.mjs'

const kind = process.argv[2] || 'openings'
if (!['openings', 'opponents'].includes(kind)) throw Error('Choose openings or opponents')

await browserFixture(kind === 'openings' ? 9316 : 9317, async ({ send, context, evaluate, waitFor, assert }) => {
  await send('browsingContext.setViewport', { context, viewport: { width: 1400, height: 1000 } })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/catalog-keyboard.html?sorting=1', wait: 'complete' })
  await evaluate(`document.querySelector('[data-style-id="player.${kind}.toggle"]').click()`)
  const idle = `document.querySelector('.is-${kind}')?.getAttribute('aria-busy')==='false'`
  const select = async target => evaluate(`document.querySelector('[data-style-id="player.${kind}.${target}"]').click()`)
  const expected = (outcome, bin, search = '') => `catalogRegression.catalogRows
    .filter(row=>row.games>=2 && (${JSON.stringify(outcome)}==='games' ||
      (row[${JSON.stringify(outcome)}]*100>=${bin * 10}*row.games && (${bin}===9 || row[${JSON.stringify(outcome)}]*100<${(bin + 1) * 10}*row.games))))
    .sort((a,b)=>(${JSON.stringify(outcome)}==='games'?0:b[${JSON.stringify(outcome)}]/b.games-a[${JSON.stringify(outcome)}]/a.games)
      || b.games-a.games || (a.key<b.key?-1:a.key>b.key?1:0))
    .filter(row=>row.key.includes(${JSON.stringify(search)}))`
  const checkRows = async (outcome, bin = 9, search = '') => {
    await waitFor(idle)
    await assert(`(() => {
      const expected=${expected(outcome, bin, search)};
      const rows=[...document.querySelectorAll('.is-${kind} .player-game-catalog-row')];
      return rows.length>0 && rows.every((row,i)=>row.dataset.catalogKey===expected[i].key);
    })()`)
    await assert(`document.querySelector('.is-${kind} .player-game-catalog-totals').textContent.trim()===
      (${expected(outcome, bin, search)}).length.toLocaleString('en-US')+' ${kind} · '+
      (${expected(outcome, bin, search)}).reduce((sum,row)=>sum+row.games,0).toLocaleString('en-US')+' games'`)
    if (kind === 'opponents') await assert(`(() => {
      const ratings=[...document.querySelectorAll('.is-opponents .player-game-catalog-row:first-child .player-game-catalog-rating')];
      const mode=document.querySelector('[aria-label="Opponent game mode"]').value;
      return ratings.length===(mode==='all'?3:1) && ratings.some(el=>el.textContent.includes('2502'));
    })()`)
  }
  await checkRows('games')
  await assert(`!document.querySelector('.player-game-catalog-bins')`)
  const leftColumn = `(() => {
    const buttons=[...document.querySelectorAll('.player-game-catalog-sort button')].map(el=>el.getBoundingClientRect());
    const body=document.querySelector('.is-${kind} .player-game-catalog-body').getBoundingClientRect();
    const list=document.querySelector('.is-${kind} .player-game-catalog-rows').getBoundingClientRect();
    return buttons.length===4 && Math.abs(buttons[0].left-body.left)<1 && buttons.every((b,i)=>Math.abs(b.left-buttons[0].left)<1 && b.right<=list.left && (!i || b.top>=buttons[i-1].bottom));
  })()`
  const binRow = `(() => {
    const buttons=[...document.querySelectorAll('.player-game-catalog-bin')];
    const rects=buttons.map(el=>el.getBoundingClientRect());
    return buttons.length===10 && buttons.every((el,i)=>el.dataset.styleId==='player.${kind}.bin.'+(9-i)) &&
      rects.every((r,i)=>Math.abs(r.width-r.height)<1 && Math.abs(r.top-rects[0].top)<1 && (!i || r.left>=rects[i-1].right));
  })()`
  await assert(leftColumn)
  for (const outcome of ['wins', 'losses', 'draws']) {
    // Returning to By games clears the previous bin; an outcome starts at 90–100%.
    await select('sort.games')
    await checkRows('games')
    await select(`sort.${outcome}`)
    await checkRows(outcome)
    await assert(`document.querySelector('[data-style-id="player.${kind}.bin.9"]').getAttribute('aria-pressed')==='true'`)
    await assert(binRow)
    for (let bin=9; bin>=0; bin--) {
      await select(`bin.${bin}`)
      await checkRows(outcome, bin)
      await assert(`document.querySelector('[data-style-id="player.${kind}.bin.${bin}"]').getAttribute('aria-pressed')==='true'`)
      await assert(`catalogRegression.catalogPages.some(row=>row.sort_by==='${outcome}' && row.ratio_bin==='${bin}' && row.page==='1')`)
    }
  }
  await select('sort.wins')
  await select('bin.9')
  await checkRows('wins')
  await evaluate(`document.querySelector('.is-${kind} .player-game-catalog-rows').scrollTop=100000`)
  await waitFor(`document.querySelectorAll('.is-${kind} .player-game-catalog-row').length>=20`)
  await checkRows('wins')
  await assert(`catalogRegression.catalogPages.some(row=>row.sort_by==='wins' && row.ratio_bin==='9' && row.page==='2')`)
  await select('bin.0')
  await checkRows('wins', 0)
  await assert(`document.querySelector('.is-${kind} .player-game-catalog-rows').scrollTop===0`)
  // A slow loss response must not replace the next draw/bin selection.
  await select('sort.losses')
  await evaluate(`new Promise(resolve=>setTimeout(resolve,25))`)
  await select('sort.draws')
  await select('bin.5')
  await checkRows('draws', 5)
  await evaluate(`new Promise(resolve=>setTimeout(resolve,250))`)
  await checkRows('draws', 5)
  await evaluate(`const mode=document.querySelector('[aria-label="${kind === 'openings' ? 'Opening' : 'Opponent'} game mode"]');mode.value='blitz';mode.dispatchEvent(new Event('change',{bubbles:true}));if ('${kind}'==='openings') document.querySelectorAll('[data-style-id="player.${kind}.side"]')[1].click()`)
  await waitFor(`catalogRegression.catalogPages.some(row=>row.sort_by==='draws' && row.ratio_bin==='5' && row.mode==='blitz' && row.color==='${kind === 'openings' ? 'black' : 'all'}' && row.page==='1')`)
  const search = value => evaluate(`(() => {const input=document.querySelector('[aria-label="Search ${kind}"]');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(input,${JSON.stringify(value)});input.dispatchEvent(new Event('input',{bubbles:true}));})()`)
  await search('draws-')
  await waitFor(`catalogRegression.catalogPages.some(row=>row.search==='draws-' && row.ratio_bin==='5')`)
  await checkRows('draws', 5, 'draws-')
  await send('browsingContext.setViewport', { context, viewport: { width: 650, height: 900 } })
  await assert(leftColumn)
  await assert(binRow)
  await search('missing-opening')
  await waitFor(`catalogRegression.catalogPages.some(row=>row.search==='missing-opening')`)
  await waitFor(idle)
  await assert(`document.querySelectorAll('.is-${kind} .player-game-catalog-row').length===0 && document.querySelectorAll('.player-game-catalog-bin').length===10`)
  await search('')
  await waitFor(idle)
  await select('sort.games')
  await checkRows('games')
  await assert(`!document.querySelector('.player-game-catalog-bins') && !('ratio_bin' in catalogRegression.catalogPages.at(-1))`)
  await assert(`catalogRegression.errors.length===0 && catalogRegression.unexpected.length===0`)
  console.log(`PASS ${kind} outcome bins: all 30 selections, descending square layout, percentage order within bins, totals, pagination reset, stale responses, search, mode/color filters, empty results and By games reset.`)
})
