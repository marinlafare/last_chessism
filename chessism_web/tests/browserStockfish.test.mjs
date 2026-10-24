import test from 'node:test'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { BrowserStockfish } from '../src/pages/live-analysis/browserStockfish.js'
import { parseAnalysisInfo } from '../src/pages/live-analysis/uciAnalysis.js'

const white = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1'
const black = 'rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1'
const info = (value = 35, pv = 1) => `info depth 12 multipv ${pv} score cp ${value} nodes 250000 pv e2e4 e7e5`
function setup(t, options = {}) {
  const workers = []
  const engine = new BrowserStockfish({ ...options, workerFactory: () => {
    const worker = { commands: [], terminated: false, postMessage(command) { this.commands.push(command) },
      terminate() { this.terminated = true }, emit(text) { this.onmessage({ data: text }) },
      ready() { this.emit('uciok'); this.emit('readyok') } }
    workers.push(worker)
    return worker
  } })
  t.after(() => engine.dispose())
  return { engine, workers }
}

test('parses multipv, white-relative centipawns/mates and terminal scores; ignores bounds and logs', () => {
  assert.equal(parseAnalysisInfo(info(), white).score, 35)
  assert.equal(parseAnalysisInfo(info(), black).score, -35)
  assert.deepEqual(parseAnalysisInfo(info(75, 2), white), { score: 75, scoreType: 'cp', mate: null, depth: 12, multipv: 2, nodes: 250000, pv: ['e2e4', 'e7e5'] })
  for (const [value, expected] of [[3, 9997], [-3, -9997], [0, -10000]]) {
    assert.equal(parseAnalysisInfo(`info depth 1 score mate ${value}`, white).score, expected)
    assert.equal(parseAnalysisInfo(`info depth 1 score mate ${value}`, black).score, -expected)
  }
  assert.equal(parseAnalysisInfo('info depth 0 score cp 0', white).score, 0)
  for (const value of ['info string score cp 50 depth 4', 'bestmove e2e4', 'info depth 5', 'info depth 4 score cp 35 lowerbound']) {
    assert.equal(parseAnalysisInfo(value, white), null)
  }
})

test('loads lazily, handshakes before searching, streams sorted lines and reuses an idle worker', async t => {
  const { engine, workers } = setup(t), updates = [], statuses = []
  assert.equal(workers.length, 0)
  const first = engine.analyze(white, { onUpdate: r => updates.push(r), onStatus: s => statuses.push(s) })
  const worker = workers[0]
  assert.deepEqual(worker.commands, ['uci'])
  worker.ready()
  assert.deepEqual(worker.commands, ['uci', 'setoption name Hash value 16', 'isready', 'setoption name MultiPV value 3', `position fen ${white}`, 'go nodes 250000'])
  worker.emit(info(10, 2)); worker.emit(info(35)); worker.emit('bestmove e2e4 ponder e7e5')
  assert.deepEqual((await first).analysis.map(l => l.multipv), [1, 2])
  assert.equal(updates.length, 2)
  assert.deepEqual(statuses, ['loading', 'analyzing'])
  const second = engine.analyze(black)
  assert.equal(workers.length, 1)
  worker.emit(info()); worker.emit('bestmove e7e5')
  assert.equal((await second).analysis[0].score, -35)
  assert.equal(engine.timer, null)
})

test('abort during boot or search terminates immediately and ignores late messages', async t => {
  const { engine, workers } = setup(t)
  for (const ready of [false, true]) {
    const controller = new AbortController(), updates = []
    const pending = engine.analyze(white, { signal: controller.signal, onUpdate: r => updates.push(r) })
    const rejected = assert.rejects(pending, { name: 'AbortError' })
    const worker = workers.at(-1)
    if (ready) worker.ready()
    controller.abort()
    await rejected
    assert.equal(worker.terminated, true)
    worker.emit('readyok'); worker.emit(info()); worker.emit('bestmove e2e4')
    assert.deepEqual(updates, [])
    assert.equal(engine.worker, null)
  }
})

test('new position supersedes a running search; disposal stops all work', async t => {
  const { engine, workers } = setup(t)
  const old = engine.analyze(white), rejected = assert.rejects(old, { name: 'AbortError' })
  workers[0].ready()
  const current = engine.analyze(black)
  await rejected
  workers[0].emit(info(999)); workers[0].emit('bestmove e2e4')
  workers[1].ready(); workers[1].emit(info()); workers[1].emit('bestmove e7e5')
  assert.equal((await current).fen, black)
  engine.dispose()
  assert.ok(workers.every(w => w.terminated))
  await assert.rejects(engine.analyze(white), { name: 'AbortError' })
})

test('worker failures, empty results and timeouts fail visibly and allow retry', async t => {
  const { engine, workers } = setup(t, { startupTimeout: 10, searchTimeout: 10 })
  await assert.rejects(engine.analyze(white), /too long to load/)
  assert.ok(workers[0].terminated)
  const timeout = engine.analyze(white)
  workers.at(-1).ready()
  await assert.rejects(timeout, /Analysis timed out/)
  const failed = engine.analyze(white)
  workers.at(-1).onerror({ preventDefault() {} })
  await assert.rejects(failed, /Could not load/)
  const empty = engine.analyze(white)
  workers.at(-1).ready(); workers.at(-1).emit('bestmove (none)')
  await assert.rejects(empty, /no evaluation/)
  const retry = engine.analyze(white)
  workers.at(-1).ready(); workers.at(-1).emit(info()); workers.at(-1).emit('bestmove e2e4')
  assert.equal((await retry).analysis[0].score, 35)
})

test('invalid commands, excess resource limits and pre-aborted requests never create a worker', async t => {
  const { engine, workers } = setup(t), controller = new AbortController()
  controller.abort()
  await assert.rejects(engine.analyze(white, { signal: controller.signal }), { name: 'AbortError' })
  await assert.rejects(engine.analyze(white + '\ngo infinite'), /Invalid analysis position/)
  await assert.rejects(engine.analyze(white, { nodes: 250001 }), /limits/)
  await assert.rejects(engine.analyze(white, { multipv: 4 }), /limits/)
  assert.equal(workers.length, 0)
})

test('vendored lite engine and rebuild materials match pinned checksums', async () => {
  for (const [file, expected] of Object.entries({
    'stockfish-19-lite-single.js': 'ccb130d7ff29408531c47d859a49f91d59d4f482d05a53eb1a3231a4f624c91a',
    'stockfish-19-lite-single.wasm': 'ee4f115bdb2d32cadf96cada104a05b5ec257d7ad6f57582190b49bc71f096de',
    'stockfish.js-v19.0.1-source.tar.gz': 'a18aa8a871f5265014b110b3e07804ff3637899d2e189be6ded2b99e2c17a10e',
    'nn-61e7af4bb97d.nnue': '61e7af4bb97d51eeeb25d322916f86513b5cd3a827ce189c98c6e31946f99e5b',
  })) {
    const bytes = await readFile(new URL(`../public/engines/stockfish-19.0.1/${file}`, import.meta.url))
    assert.equal(createHash('sha256').update(bytes).digest('hex'), expected, file)
  }
})
