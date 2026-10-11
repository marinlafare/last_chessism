import { lazy, memo, Suspense, useEffect, useState } from 'react'
import { fetchAdvantageByRating } from './scoredPositionsApi'
import { styleTarget } from '../../appearance/styleCore'

const RatingScatterChart = lazy(() => import('./RatingScatterChart'))

function RatingChartDisclosure() {
  const [open, setOpen] = useState(false)
  const [payload, setPayload] = useState(null)
  const [error, setError] = useState('')
  const [revision, setRevision] = useState(0)
  const [loadedAt, setLoadedAt] = useState(null)

  useEffect(() => {
    if (!open || payload) return undefined
    const controller = new AbortController()
    setError('')
    fetchAdvantageByRating({ signal: controller.signal }).then(data => {
      if (controller.signal.aborted) return
      setPayload(data)
      setLoadedAt(new Date())
    }).catch(err => {
      if (!controller.signal.aborted) setError(err.message || 'Rating chart unavailable.')
    })
    return () => controller.abort()
  }, [open, payload, revision])

  const refresh = () => {
    setError('')
    setPayload(null)
    setRevision(value => value + 1)
  }

  return <section {...styleTarget('chart.disclosure')} className="scored-positions-panel scored-rating-scatter-panel" aria-label="Rating groups chart">
    <h2 className="rating-chart-heading"><button type="button" id="rating-chart-toggle" className="rating-chart-toggle"
      aria-expanded={open} aria-controls="rating-chart-content" onClick={() => setOpen(value => !value)}>
      <span>Rating groups</span>
      <svg viewBox="0 0 20 20" aria-hidden="true"><path d="m5 7.5 5 5 5-5" /></svg>
    </button></h2>
    <div id="rating-chart-content" role="region" aria-labelledby="rating-chart-toggle" hidden={!open}>
      {open && <>
        <div className="rating-chart-toolbar">
          <p>{payload
            ? `Loaded ${loadedAt.toLocaleTimeString()}. Refresh to include newer analyses.`
            : 'Chart data loads only when this section is opened.'}</p>
          <button type="button" className="btn btn-secondary btn-inline" disabled={!payload && !error} onClick={refresh}>
            {error ? 'Retry chart' : 'Refresh chart'}
          </button>
        </div>
        {error && <p className="status-banner warn" role="alert">{error}</p>}
        {!payload && !error && <p className="result-line" role="status">Loading rating chart…</p>}
        {payload && <Suspense fallback={<p className="result-line" role="status">Preparing rating chart…</p>}>
          <RatingScatterChart payload={payload} />
        </Suspense>}
      </>}
    </div>
  </section>
}

export default memo(RatingChartDisclosure)
