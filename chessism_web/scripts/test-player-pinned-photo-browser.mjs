// Real homepage and profile components; every API request is mocked by home.html.
import { browserFixture } from './browser-fixture.mjs'

await browserFixture(9311, async ({ send, context, evaluate, assert, waitFor }) => {
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1000 } })
  await send('browsingContext.navigate', { context,
    url: 'http://localhost:6789/tests/fixtures/home.html?initial=%2F%3Fplayer%3Dhikaru', wait: 'complete' })
  const card = `document.querySelector('[data-style-id="player.photo"]')`
  const slot = `document.querySelector('.players-profile-photo-anchor')`
  const pin = `document.querySelector('.player-profile-photo-pin')`
  const button = `document.querySelector('[data-style-id="player.photo.pinned"]')`
  const workspace = `document.querySelector('.players-starting-point')`
  const popup = `document.querySelector('.player-profile-popup')`
  const key = value => send('input.performActions', { context, actions: [{ type: 'key', id: 'keyboard', actions: [
    { type: 'keyDown', value }, { type: 'keyUp', value },
  ] }] })
  const loaded = name => waitFor(`document.querySelector('.players-title-select')?.textContent.trim()==='${name}' && ${card} && !${card}.disabled`)
  const extend = () => evaluate(`${workspace}.style.minHeight='3600px'`)
  const scroll = top => evaluate(`window.scrollTo({top:${top},behavior:'instant'})`)
  await loaded('hikaru')
  await waitFor(`getComputedStyle(${card}).backgroundColor==='rgb(8, 8, 8)'`)
  await evaluate(`document.fonts.ready`)
  await scroll(0)
  await waitFor(`!${pin}`)
  await extend() // Stand in for long, expanded analysis without querying live data.
  await evaluate(`window.photoLayout={height:${card}.offsetHeight,width:${slot}.offsetWidth,left:${slot}.getBoundingClientRect().left}`)
  await assert(`getComputedStyle(${slot}).visibility==='visible'`)
  await scroll(`${slot}.getBoundingClientRect().top+scrollY-14`)
  await evaluate(`${card}.style.transform='translateY(-4px)';window.dispatchEvent(new Event('scroll'));new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))`)
  await assert(`!${pin}`) // The hover lift must not change the pinning threshold.
  await evaluate(`${card}.style.transform=''`)
  await scroll(`${slot}.getBoundingClientRect().top+scrollY-8`)
  await waitFor(`${pin} && ${pin}.getBoundingClientRect().top===12`)
  await assert(`${card}.offsetHeight===photoLayout.height && ${slot}.offsetWidth===photoLayout.width`)
  await assert(`Math.abs(${pin}.getBoundingClientRect().left-photoLayout.left)<1`)
  await assert(`getComputedStyle(${slot}).visibility==='hidden' && ${card}.getAttribute('aria-hidden')==='true'`)
  await assert(`${button}.querySelector('img').alt.includes('hikaru') && !${pin}.querySelector('dl')`)
  // Scrolling well past the hero must not dislodge the photo or pin the card.
  await scroll(2000)
  await waitFor(`${pin}?.getBoundingClientRect().top===12 && ${card}.getBoundingClientRect().bottom<0`)
  await assert(`${pin}.getBoundingClientRect().width===photoLayout.width`)
  await assert(`getComputedStyle(${card}).position!=='fixed'`)
  await evaluate(`window.photoScroll=scrollY;window.photoRequests=homeTest.requests.length;${button}.focus({preventScroll:true})`)
  await key('\uE007')
  await waitFor(`${popup}`)
  await assert(`${popup}.textContent.includes('hikaru') && document.body.style.overflow==='hidden'`)
  await assert(`homeTest.requests.length===photoRequests`)
  await key('\uE00C')
  await waitFor(`!${popup}`)
  await assert(`document.activeElement===${button} && scrollY===photoScroll`)
  // Reflow on mobile remains inside the viewport, without a second photo.
  await send('browsingContext.setViewport', { context, viewport: { width: 390, height: 700 } })
  await waitFor(`${pin}?.getBoundingClientRect().left>=12 && ${pin}.getBoundingClientRect().right<=document.documentElement.clientWidth-12`)
  await assert(`document.querySelectorAll('.player-profile-photo-pin').length===1`)
  await scroll(0)
  await waitFor(`!${pin} && getComputedStyle(${slot}).visibility==='visible'`)
  await assert(`!${card}.hasAttribute('aria-hidden') && ${card}.tabIndex===0`)
  await assert(`document.activeElement===${card}`)
  // Return to desktop, change player, then verify identity and teardown.
  await send('browsingContext.setViewport', { context, viewport: { width: 1440, height: 1000 } })
  await evaluate(`document.querySelector('.players-title-arrow[aria-label^="Next player"]').click()`)
  await loaded('magnuscarlsen')
  await extend()
  await scroll(2000)
  await waitFor(`${button}?.getAttribute('aria-label')==='Open full profile for magnuscarlsen'`)
  await assert(`${button}.querySelector('img').alt.includes('magnuscarlsen')`)
  await evaluate(`history.pushState(null,'','/');window.dispatchEvent(new PopStateEvent('popstate'))`)
  await waitFor(`!${workspace} && !${pin}`)
  await scroll(100)
  await assert(`!${pin} && homeTest.unexpected.length===0`)
  console.log('PASS: photo-only pinning, stable layout, deep scrolling, profile popup/focus, mobile resize, scroll-up restoration, player switching and unmount; APIs fully mocked.')
})
