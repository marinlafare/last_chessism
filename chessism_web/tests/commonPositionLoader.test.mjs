import test from 'node:test'
import assert from 'node:assert/strict'
import { setImmediate as tick, setTimeout as wait } from 'node:timers/promises'
import { createCommonPositionLoader } from '../src/pages/scored-positions/commonPositionLoader.js'

test('prefetch is one page only; navigating reuses its in-flight request and cached result', async () => {
  const calls = []; let finish
  const loader = createCommonPositionLoader((cursor, options) => {
    calls.push({cursor, ...options})
    return new Promise(resolve => { finish = resolve })
  })
  loader.retain(['page-2'])
  loader.retain(['page-2']) // becomes visible while still loading
  assert.equal(calls.length, 1)
  assert.equal(calls[0].signal.aborted, false)
  finish({status:'complete', rows:[{rank:11}], next_cursor:'page-3'})
  await tick()
  assert.equal(calls.length, 1) // no recursive lookahead until visible page changes
  assert.equal(loader.get('page-2').rows[0].rank, 11)
  loader.retain([]); loader.retain(['page-2'])
  assert.equal(calls.length, 1)
  loader.stop()
})

test('pending prefetch polls quickly; canceled or hidden pages never publish late responses', async () => {
  const calls = []; let finish
  const loader = createCommonPositionLoader((cursor, options) => {
    calls.push({cursor, ...options})
    return calls.length === 1 ? Promise.resolve({status:'pending',rows:[]}) : new Promise(resolve => { finish=resolve })
  })
  loader.retain(['page-2'])
  await wait(600)
  assert.equal(calls.length, 2)
  loader.retain([])
  assert.equal(calls[1].signal.aborted, true)
  finish({status:'complete',rows:[{rank:11}]})
  await tick()
  assert.equal(loader.get('page-2'), undefined)
  await wait(600)
  assert.equal(calls.length, 2)
  loader.stop()
})

test('prefetch error is isolated, explicit retry recovers, and new snapshot uses fresh cache', async () => {
  const calls = []
  const loader = createCommonPositionLoader(async (cursor, options) => {
    calls.push({cursor, ...options})
    return {status: options.retry ? 'complete' : 'error', rows:[]}
  })
  loader.retain(['page-2']); await tick()
  assert.equal(loader.get('page-2').status, 'error')
  loader.retain(['page-2']); assert.equal(calls.length, 1)
  loader.retry('page-2'); await tick()
  assert.equal(calls.length, 2); assert.equal(calls[1].retry, true)
  assert.equal(loader.get('page-2').status, 'complete')
  loader.stop()
  const fresh = createCommonPositionLoader(async () => ({status:'changed'}))
  assert.equal(fresh.get('page-2'), undefined)
  fresh.retain(['page-2']); await tick()
  assert.equal(fresh.get('page-2').status, 'changed')
  fresh.stop()
})

test('bounded cache does not evict visible page while preparing lookahead', async () => {
  const loader = createCommonPositionLoader(async cursor => ({status:'complete',cursor}))
  for (let i=0; i<70; i++) { loader.retain(['visible', `page-${i}`]); await tick() }
  assert.equal(loader.get('visible').cursor, 'visible')
  assert.equal(loader.get('page-0'), undefined)
  loader.stop()
})
