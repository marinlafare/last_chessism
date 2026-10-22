// Regression for focus routing, shared mouse/keyboard selection and alignment.
// All API requests use fixture responses; this script never reads live DB data.
import { spawn } from 'node:child_process'
import { mkdtemp, mkdir, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { once } from 'node:events'

const readRegistry = name => readFile(new URL(`../../chessism_api/${name}`, import.meta.url), 'utf8').then(JSON.parse)
const [base, scopes, charts] = await Promise.all(['appearance_registry.json', 'appearance_scopes.json', 'appearance_charts.json'].map(readRegistry))
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: {} }
for (const [scope, spec] of Object.entries(scopes.scopes)) registry.scopes[scope] = {
  ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets, ...(scope === base.scope ? base.targets : {}) },
}
const temporary = await mkdtemp(path.join(tmpdir(), 'chessism-catalog-keys-'))
const profile = path.join(temporary, 'profile')
await mkdir(profile)
const browser = spawn('firefox', ['--headless', '--no-remote', '--profile', profile, '--remote-debugging-port', '9275'], { stdio: ['ignore', 'pipe', 'pipe'] })
let socket
try {
  const endpoint = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('Browser startup timeout')), 15000)
    const output = chunk => {
      const match = String(chunk).match(/WebDriver BiDi listening on (ws:\/\/[^\s]+)/)
      if (match) { clearTimeout(timer); resolve(match[1]) }
    }
    browser.stdout.on('data', output); browser.stderr.on('data', output)
    browser.once('error', error => { clearTimeout(timer); reject(error) })
  })
  socket = new WebSocket(endpoint.replace(/\/$/, '') + '/session')
  await new Promise((resolve, reject) => { socket.onopen = resolve; socket.onerror = reject })
  const pending = new Map(); let id = 0
  socket.onmessage = ({ data }) => {
    const message = JSON.parse(data), request = pending.get(message.id)
    if (!request) return
    pending.delete(message.id)
    message.type === 'error' ? request.reject(Error(JSON.stringify(message))) : request.resolve(message.result)
  }
  const send = (method, params) => new Promise((resolve, reject) => {
    const key = ++id; pending.set(key, { resolve, reject }); socket.send(JSON.stringify({ id: key, method, params }))
  })
  await send('session.new', { capabilities: {} })
  await send('script.addPreloadScript', { functionDeclaration: `() => { window.regressionRegistry = ${JSON.stringify(registry)} }` })
  const { context } = await send('browsingContext.create', { type: 'tab' })
  await send('browsingContext.setViewport', { context, viewport: { width: 1280, height: 900 } })
  await send('browsingContext.navigate', { context, url: 'http://localhost:6789/tests/fixtures/catalog-keyboard.html', wait: 'complete' })
  const evaluate = async expression => {
    const result = await send('script.evaluate', { expression, target: { context }, awaitPromise: true })
    if (result.type === 'exception') throw Error(JSON.stringify(result))
    return result.result.value
  }
  const waitFor = async expression => {
    for (let attempt = 0; attempt < 60; attempt++) {
      if (await evaluate(expression)) return
      await new Promise(resolve => setTimeout(resolve, 100))
    }
    throw Error(`Timeout: ${expression}; ${await evaluate('JSON.stringify({text:document.body.innerText,errors:window.catalogRegression?.errors,unexpected:window.catalogRegression?.unexpected})')}`)
  }
  const assert = async expression => { if (!await evaluate(expression)) throw Error(`Failed: ${expression}`) }
  const key = value => send('input.performActions', { context, actions: [{ type: 'key', id: 'keyboard', actions: [{ type: 'keyDown', value }, { type: 'keyUp', value }] }] })
  await waitFor("document.querySelector('[data-style-id=\"player.opponents.toggle\"]') !== null")
  for (const kind of ['opponents', 'openings']) {
    await evaluate(`window.kind = ${JSON.stringify(kind)}; window.toggle = document.querySelector('[data-style-id="player.' + kind + '.toggle"]'); toggle.focus({preventScroll:true}); toggle.click(); window.scrollTo(0,100)`)
    await waitFor("document.activeElement?.classList.contains('player-game-catalog-rows')")
    await evaluate("window.list = document.activeElement; window.pageTop = scrollY")
    await key('\uE015')
    await waitFor("list.querySelector('.is-highlighted')?.dataset.catalogKey === 'sample-0'")
    await key('\uE015'); await key('\uE013')
    await assert("scrollY === pageTop && list.querySelectorAll('.is-highlighted').length === 1 && document.activeElement.dataset.catalogKey === 'sample-0'")
    const coordinates = JSON.parse(await evaluate("JSON.stringify((() => {const r=list.querySelectorAll('.player-game-catalog-row')[3].getBoundingClientRect(); return {x:Math.round(r.x+40),y:Math.round(r.y+r.height/2)}})())"))
    await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [{ type: 'pointerMove', origin: 'viewport', ...coordinates }] }] })
    await waitFor("list.querySelector('.is-highlighted')?.dataset.catalogKey === 'sample-3'")
    await key('\uE013')
    await assert("list.querySelectorAll('.is-highlighted').length === 1 && list.querySelector('.is-highlighted').dataset.catalogKey === 'sample-2' && scrollY === pageTop")
    await key('\uE007')
    await waitFor("window.catalogRegression.selections.length > 0 && document.querySelector('[role=dialog]') !== null")
    await assert("catalogRegression.selections.at(-1)[kind === 'opponents' ? 'opponent' : 'opening'] === 'sample-2'")
    await evaluate("document.querySelector('[aria-label=\"Close game explorer\"]').click(); toggle.focus({preventScroll:true}); toggle.click()")
  }
  for (const alignment of ['left', 'center', 'right']) {
    await evaluate(`catalogRegression.align(${JSON.stringify(alignment)})`)
    await assert(`['player.compare.toggle','rating.toggle'].every(id => {const b=document.querySelector('[data-style-id="'+id+'"]');return getComputedStyle(b).textAlign === '${alignment}' && getComputedStyle(b.querySelector('span')).textAlign === '${alignment}' && b.querySelector('span').getBoundingClientRect().width > b.clientWidth * .8})`)
  }
  await evaluate("window.explorerOpener=document.getElementById('explore-fixture-games'); explorerOpener.focus({preventScroll:true}); explorerOpener.click(); window.pageTop=scrollY")
  await waitFor("document.activeElement?.classList.contains('game-explorer-list')")
  await evaluate('window.gamesList=document.activeElement')
  await key('\uE015'); await key('\uE015'); await key('\uE013')
  await assert("gamesList.querySelectorAll('.is-highlighted').length === 1 && document.activeElement.dataset.gameKey === 'fixture-game-0-white' && scrollY === pageTop")
  const gameCoordinates = JSON.parse(await evaluate("JSON.stringify((() => {const r=gamesList.querySelectorAll('[data-game-key]')[3].getBoundingClientRect(); return {x:Math.round(r.x+40),y:Math.round(r.y+r.height/2)}})())"))
  await send('input.performActions', { context, actions: [{ type: 'pointer', id: 'mouse', parameters: { pointerType: 'mouse' }, actions: [{ type: 'pointerMove', origin: 'viewport', ...gameCoordinates }] }] })
  await waitFor("gamesList.querySelector('.is-highlighted')?.dataset.gameKey === 'fixture-game-3-white'")
  await key('\uE013'); await key('\uE007')
  await assert("catalogRegression.openedGames.at(-1) === 'fixture-game-2' && gamesList.querySelectorAll('.is-highlighted').length === 1")
  await key('\uE010') // End scrolls this list, allowing the next page to load.
  await waitFor("gamesList.querySelectorAll('[data-game-key]').length === 35")
  await key('\uE015')
  await assert("document.activeElement.dataset.gameKey === 'fixture-game-30-white' && gamesList.scrollTop > 0 && scrollY === pageTop")
  await key('\uE010'); await key('\uE015')
  await assert("document.activeElement.dataset.gameKey === 'fixture-game-34-white' && catalogRegression.gamePages.length === 2")
  await key('\uE011'); await key('\uE007') // Home, Enter
  await assert("catalogRegression.openedGames.at(-1) === 'fixture-game-0' && scrollY === pageTop")
  await key('\uE00C') // Escape restores focus to the opener without scrolling.
  await waitFor("document.querySelector('.game-explorer-modal') === null")
  await assert('document.activeElement === explorerOpener && scrollY === pageTop')
  await assert('catalogRegression.unexpected.length === 0 && catalogRegression.errors.length === 0')
  console.log('PASS: opponents, openings and game explorer: real arrow/Enter keys, list focus, shared pointer selection, no page scrolling, game pagination and focus restoration; independent Compare/Rating alignment. Mock data only.')
  await send('session.end', {})
} finally {
  socket?.close()
  if (browser.pid && browser.exitCode === null && browser.signalCode === null) {
    const stopped = once(browser, 'exit'); browser.kill('SIGTERM'); await stopped
  }
  // This is only the isolated temporary browser profile created above.
  await rm(temporary, { recursive: true, force: true })
}
