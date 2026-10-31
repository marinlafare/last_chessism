import { useEffect, useState } from 'react'
import './scored-positions/scoredPositions.css'
import SideRail from '../components/layout/SideRail'
import AppearanceChartPanel from './scored-positions/AppearanceChartPanel'
import ScoredOverviewChart from './scored-positions/ScoredOverviewChart'
import RatingChartDisclosure from './scored-positions/RatingChartDisclosure'
import { styleTarget } from '../appearance/styleCore'
import {
  fetchScoredGamesOverview,
  fetchScoredCoverage,
  fetchScoredOverview,
} from './scored-positions/scoredPositionsApi'

function ScoredPositions() {
  const [overview, setOverview] = useState(null)
  const [gameOverview, setGameOverview] = useState(null)
  const [coverage, setCoverage] = useState(null)
  const [coverageError, setCoverageError] = useState('')
  const [error, setError] = useState('')
  const [gameOverviewError, setGameOverviewError] = useState('')
  const [reloadToken, setReloadToken] = useState(0)
  useEffect(() => {
    const controller = new AbortController()

    const load = async (fetcher, setData, setLoadError) => {
      try {
        const payload = await fetcher({ signal: controller.signal })
        if (controller.signal.aborted) return
        setData(payload)
        setLoadError('')
      } catch (loadError) {
        if (!controller.signal.aborted) {
          setLoadError(loadError.message || 'Summary unavailable.')
        }
      }
    }

    load(fetchScoredOverview, setOverview, setError)
    load(fetchScoredGamesOverview, setGameOverview, setGameOverviewError)
    load(fetchScoredCoverage, setCoverage, setCoverageError)
    return () => controller.abort()
  }, [reloadToken])

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      if (document.visibilityState === 'visible') {
        setReloadToken((current) => current + 1)
      }
    }, 15000)

    return () => window.clearInterval(intervalId)
  }, [])

  return (
    <div {...styleTarget('page')} className="page-frame scored-positions-page">
      <SideRail />
      <div className="home-shell">
        <main className="scored-positions-main">
          <h1 {...styleTarget('heading')} className="scored-positions-title">POSITIONS</h1>
          <section {...styleTarget('layout.top')} className="scored-top-grid">
            <section {...styleTarget('metrics.panel')} className="scored-positions-panel scored-engine-panel">
              {error ? <div className="status-banner warn">{error}</div> : null}
              {gameOverviewError ? <div className="status-banner warn">{gameOverviewError}</div> : null}
              {coverageError ? <div className="status-banner warn">{coverageError}</div> : null}

              <ScoredOverviewChart overview={overview} gameOverview={gameOverview} coverage={coverage} />
            </section>

            <AppearanceChartPanel reloadToken={reloadToken} />
          </section>

          <section {...styleTarget('cuda.panel')} className="scored-positions-panel scored-rating-advantage-panel cuda-reservation" aria-label="Advantage by rating">
            <h2 {...styleTarget('panel.title')} className="panel-title">Advantage by rating</h2>
            <div className="cuda-reservation-content" role="note">
              <svg className="cuda-reservation-icon" viewBox="0 0 48 48" aria-hidden="true" focusable="false">
                <circle cx="24" cy="24" r="19" />
                <path d="m17 17 14 14 M31 17 17 31" />
              </svg>
              <span>reserved for cuda</span>
            </div>
          </section>

          <RatingChartDisclosure />

        </main>
      </div>
    </div>
  )
}

export default ScoredPositions
