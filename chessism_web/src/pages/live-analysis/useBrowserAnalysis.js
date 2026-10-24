import { useEffect, useRef, useState } from 'react'
import { BrowserStockfish } from './browserStockfish.js'

export function useBrowserAnalysis(fen, enabled) {
  const engine = useRef(null)
  const [visible, setVisible] = useState(() => !document.hidden)
  const [analysis, setAnalysis] = useState(null)
  const [evaluation, setEvaluation] = useState(null)
  const [status, setStatus] = useState('off')
  const [error, setError] = useState('')

  useEffect(() => {
    const stop = () => { engine.current?.dispose(); engine.current = null }
    const visibility = () => { if (document.hidden) stop(); setVisible(!document.hidden) }
    document.addEventListener('visibilitychange', visibility)
    window.addEventListener('pagehide', stop)
    return () => {
      stop()
      document.removeEventListener('visibilitychange', visibility)
      window.removeEventListener('pagehide', stop)
    }
  }, [])

  useEffect(() => {
    setAnalysis(null)
    setError('')
    if (!enabled || !visible) {
      engine.current?.dispose()
      engine.current = null
      setStatus(enabled ? 'paused' : 'off')
      return undefined
    }
    const controller = new AbortController()
    const update = value => {
      if (controller.signal.aborted) return
      setAnalysis(value)
      const best = value.analysis?.find(line => line.multipv === 1)
      if (Number.isFinite(best?.score)) setEvaluation({ fen: value.fen, score: best.score })
    }
    setStatus(engine.current?.worker ? 'analyzing' : 'loading')
    const timer = window.setTimeout(async () => {
      engine.current ||= new BrowserStockfish()
      try {
        const result = await engine.current.analyze(fen, {
          signal: controller.signal,
          onStatus: value => { if (!controller.signal.aborted) setStatus(value) },
          onUpdate: update,
        })
        if (!controller.signal.aborted) { update(result); setStatus('ready') }
      } catch (reason) {
        if (!controller.signal.aborted && reason?.name !== 'AbortError') {
          setAnalysis(null)
          setError(reason instanceof Error ? reason.message : 'Browser analysis failed')
          setStatus('error')
        }
      }
    }, 180)
    return () => { window.clearTimeout(timer); controller.abort() }
  }, [enabled, fen, visible])

  // Keep only the last evaluation across position changes, never old PV moves.
  return { analysis: enabled && analysis?.fen === fen ? analysis : null, evaluation, status, error }
}
