// Actual homepage/router/player components, with every API request mocked.
import {browserFixture} from './browser-fixture.mjs'

await browserFixture(9301,async({send,context,evaluate,assert,waitFor})=>{
  await send('browsingContext.setViewport',{context,viewport:{width:1440,height:1000}})
  await send('browsingContext.navigate',{context,url:'http://localhost:6789/tests/fixtures/home.html?initial=%2F',wait:'complete'})
  const hero=`document.querySelector('#selected-player')`
  const loaded=name=>waitFor(`document.querySelector('.players-title-select')?.textContent.trim()==='${name}' && !document.querySelector('.players-update-button')?.disabled`)
  const hiddenButtons=`[...document.querySelectorAll('.home-overview-restore')].length===2 && [...document.querySelectorAll('.home-overview-restore')].every(n=>n.getBoundingClientRect().bottom<=0)`
  await waitFor(`document.querySelector('a.character-rating-row[href="/?player=hikaru"]') && getComputedStyle(document.querySelector('.home-overview')).gridTemplateColumns.split(' ').length===2`)
  await waitFor(`getComputedStyle(document.documentElement).getPropertyValue('--surface-2').trim()==='#080808'`)
  await evaluate(`document.fonts.ready`)
  await evaluate(`new Promise(resolve=>setTimeout(resolve,250))`)
  await evaluate(`document.querySelector('a.character-rating-row[href="/?player=hikaru"]').click()`)
  await loaded('hikaru')
  await waitFor(`Math.abs(${hero}.getBoundingClientRect().top-18)<2`)
  await assert(hiddenButtons)
  const switchAndCheck=async(direction,name)=>{
    await evaluate(`(()=>{homeTest.slowPlayer='${name}';homeTest.frames=[];homeTest.recordScroll=true;homeTest.scrollCallsBefore=homeTest.scrolls.length;homeTest.initialHeroTop=${hero}.getBoundingClientRect().top;const sample=()=>{if(!homeTest.recordScroll)return;homeTest.frames.push({top:${hero}.getBoundingClientRect().top,buttons:[...document.querySelectorAll('.home-overview-restore')].map(n=>n.getBoundingClientRect().bottom)});requestAnimationFrame(sample)};requestAnimationFrame(sample);document.querySelector('.players-title-arrow[aria-label^="${direction} player"]').click()})()`)
    await waitFor(`document.querySelector('.players-title-select')?.textContent.includes('Loading')`)
    await assert(hiddenButtons)
    await loaded(name)
    await evaluate(`new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(()=>{homeTest.recordScroll=false;resolve()})))`)
    const result=JSON.parse(await evaluate(`JSON.stringify({frames:homeTest.frames.length,first:homeTest.frames[0]?.top,last:homeTest.frames.at(-1)?.top,maxShift:Math.max(...homeTest.frames.map(f=>Math.abs(f.top-homeTest.initialHeroTop))),maxButtonBottom:Math.max(...homeTest.frames.flatMap(f=>f.buttons)),newScrollCalls:homeTest.scrolls.length-homeTest.scrollCallsBefore})`))
    console.log(direction,name,result)
    await assert(`homeTest.frames.length>5 && homeTest.frames.every(f=>Math.abs(f.top-homeTest.initialHeroTop)<2 && f.buttons.every(bottom=>bottom<=0))`)
    await assert(`homeTest.scrolls.length===homeTest.scrollCallsBefore`)
    await assert(hiddenButtons)
  }
  await switchAndCheck('Next','magnuscarlsen')
  await switchAndCheck('Previous','hikaru')
  // Reopening an overview and selecting a player there must still scroll down.
  await evaluate(`homeTest.slowPlayer='';window.scrollTo({top:0,behavior:'instant'})`)
  await waitFor(`document.querySelector('.home-overview-restore').getBoundingClientRect().top>=0`)
  await evaluate(`document.querySelector('[aria-controls="home-rankings-content"]').click()`)
  await waitFor(`!document.querySelector('#home-rankings-content').hidden`)
  await evaluate(`document.querySelector('a.character-rating-row[href="/?player=player3"]').click()`)
  await loaded('player3')
  await waitFor(`Math.abs(${hero}.getBoundingClientRect().top-18)<2`)
  await assert(hiddenButtons)
  console.log('PASS: first selection aligns the hero; next/previous keep every loading frame steady; overview buttons stay above the viewport and remain reachable by scrolling up.')
})
