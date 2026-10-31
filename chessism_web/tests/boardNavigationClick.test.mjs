import test from 'node:test'
import assert from 'node:assert/strict'
import { createBoardNavigationClick } from '../src/components/chess/boardNavigationClick.js'

function fixture({ suspended = false } = {}) {
  const state = { contexts: [], nodes: [] }
  class AudioContext {
    constructor() { this.state = suspended ? 'suspended' : 'running'; this.currentTime = 1; state.contexts.push(this) }
    resume() { return new Promise(resolve => { state.resume = () => { this.state = 'running'; resolve() } }) }
    close() { this.state = 'closed'; return Promise.resolve() }
    createGain() { return { gain: { setValueAtTime(){}, linearRampToValueAtTime(){}, exponentialRampToValueAtTime(){} }, connect(){}, disconnect(){} } }
    createOscillator() {
      const node = { frequency: {setValueAtTime(){}}, connect(){}, disconnect(){this.disconnected=true},
        start(at){this.started=at}, stop(at){this.stopped=at; if (at === undefined) this.onended?.()} }
      state.nodes.push(node); return node
    }
  }
  return {state, audio:createBoardNavigationClick({AudioContext})}
}

test('even rapid clicks play, replacing the previous voice without a queue', async () => {
  const {state,audio}=fixture()
  assert.equal(state.contexts.length,0)
  await audio.play()
  assert.equal(state.nodes.length,1)
  assert.ok(Math.abs(state.nodes[0].stopped-state.nodes[0].started-0.02)<1e-10)
  await audio.play()
  assert.equal(state.nodes.length,2)
  assert.equal(state.nodes[0].disconnected,true)
  await audio.play()
  assert.equal(state.nodes.length,3)
  assert.equal(state.nodes[1].disconnected,true)
  assert.equal(state.contexts.length,1)
  audio.dispose()
  assert.equal(state.contexts[0].state,'closed')
  await audio.play()
  assert.equal(state.nodes.length,3)
})

test('mute/close cancels a pending browser audio resume instead of playing late', async () => {
  for (const action of ['stop','dispose']) {
    const {state,audio}=fixture({suspended:true})
    const pending=audio.play()
    audio[action]();state.resume();await pending
    assert.equal(state.nodes.length,0)
  }
})

test('unavailable audio cannot break navigation', async () => {
  const audio=createBoardNavigationClick({AudioContext:null})
  await audio.play();audio.stop();audio.dispose()
})
