import { analysisResult, parseAnalysisInfo } from './uciAnalysis.js'

export const ENGINE_ASSETS = '/engines/stockfish-19.0.1/'
export const ENGINE_SCRIPT = `${ENGINE_ASSETS}stockfish-19-lite-single.js`
const abortError = () => new DOMException('Analysis cancelled', 'AbortError')

// One single-threaded worker per Live page. Completed searches reuse it. Cancelling
// an active search terminates it: even a busy WASM engine cannot delay cancellation.
export class BrowserStockfish {
  constructor({ workerFactory = () => new Worker(ENGINE_SCRIPT), startupTimeout = 30000, searchTimeout = 15000 } = {}) {
    this.workerFactory = workerFactory
    this.startupTimeout = startupTimeout
    this.searchTimeout = searchTimeout
    this.worker = null
    this.request = null
    this.phase = 'off'
    this.timer = null
    this.disposed = false
  }

  analyze(fen, { signal, nodes = 250000, multipv = 3, onUpdate, onStatus } = {}) {
    if (this.disposed || signal?.aborted) return Promise.reject(abortError())
    if (typeof fen !== 'string' || /[\r\n]/.test(fen) || !/^[^\s]+ [wb] /.test(fen)) {
      return Promise.reject(new Error('Invalid analysis position'))
    }
    if (!Number.isInteger(nodes) || nodes < 1 || nodes > 250000 || !Number.isInteger(multipv) || multipv < 1 || multipv > 3) {
      return Promise.reject(new Error('Invalid browser analysis limits'))
    }
    if (this.request) this.fail(abortError())
    return new Promise((resolve, reject) => {
      const request = { fen, nodes, multipv, signal, onUpdate, onStatus, resolve, reject, lines: new Map() }
      request.abort = () => { if (this.request === request) this.fail(abortError()) }
      this.request = request
      signal?.addEventListener('abort', request.abort, { once: true })
      try {
        if (this.worker) { this.search(); return }
        onStatus?.('loading')
        const worker = this.workerFactory()
        this.worker = worker
        this.phase = 'uci'
        worker.onmessage = event => {
          if (this.worker !== worker) return
          for (const line of String(event.data).split(/\r?\n/)) this.message(line.trim())
        }
        worker.onerror = event => {
          event.preventDefault?.()
          if (this.worker === worker) this.fail(new Error('Could not load Stockfish Lite. Switch it off and on to retry.'))
        }
        worker.onmessageerror = () => {
          if (this.worker === worker) this.fail(new Error('The browser engine returned an unreadable result.'))
        }
        this.deadline(this.startupTimeout, 'Stockfish Lite took too long to load. Switch it off and on to retry.')
        worker.postMessage('uci')
      } catch (error) {
        this.fail(error instanceof Error ? error : new Error('Browser analysis is unavailable'))
      }
    })
  }

  deadline(milliseconds, message) {
    clearTimeout(this.timer)
    this.timer = setTimeout(() => this.fail(new Error(message)), milliseconds)
  }

  message(text) {
    const request = this.request
    if (!request) return
    if (this.phase === 'uci' && text === 'uciok') {
      this.phase = 'ready'
      this.worker.postMessage('setoption name Hash value 16')
      this.worker.postMessage('isready')
    } else if (this.phase === 'ready' && text === 'readyok') {
      this.search()
    } else if (this.phase === 'search') {
      const line = parseAnalysisInfo(text, request.fen)
      if (line && line.multipv <= request.multipv) {
        request.lines.set(line.multipv, line)
        request.onUpdate?.(analysisResult(request.fen, request.lines))
      }
      if (text.startsWith('bestmove ')) {
        if (!request.lines.size) { this.fail(new Error('Stockfish returned no evaluation for this position.')); return }
        const result = analysisResult(request.fen, request.lines)
        this.settle()
        this.phase = 'idle'
        request.resolve(result)
      }
    }
  }

  search() {
    const request = this.request
    this.phase = 'search'
    request.onStatus?.('analyzing')
    this.deadline(this.searchTimeout, 'Analysis timed out on this device. Switch Stockfish off and on to retry.')
    this.worker.postMessage(`setoption name MultiPV value ${request.multipv}`)
    this.worker.postMessage(`position fen ${request.fen}`)
    this.worker.postMessage(`go nodes ${request.nodes}`)
  }

  settle() {
    clearTimeout(this.timer)
    this.timer = null
    this.request?.signal?.removeEventListener('abort', this.request.abort)
    this.request = null
  }

  fail(error) {
    const request = this.request
    this.settle()
    this.worker?.terminate()
    this.worker = null
    this.phase = 'off'
    request?.reject(error)
  }

  dispose() {
    this.disposed = true
    this.fail(abortError())
  }
}
