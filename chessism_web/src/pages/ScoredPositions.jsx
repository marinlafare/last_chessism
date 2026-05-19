import { useEffect, useMemo, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import { API_BASE_URL } from '../config'

const RATING_GROUP_OPTIONS = [
  { key: 'bad', label: 'bad' },
  { key: 'medium', label: 'medium' },
  { key: 'great', label: 'great' }
]

const RATING_GROUP_COLORS = {
  bad: '#f07167',
  medium: '#f4c057',
  great: '#3fd089'
}

const formatNumber = (value) => {
  const numeric = Number(value ?? 0)
  if (!Number.isFinite(numeric)) return '0'
  return numeric.toLocaleString('en-US')
}

const niceStep = (span, targetTickCount) => {
  const rawStep = Math.max(1, span / Math.max(1, targetTickCount))
  const magnitude = 10 ** Math.floor(Math.log10(rawStep))
  const normalized = rawStep / magnitude

  if (normalized <= 1) return magnitude
  if (normalized <= 2) return 2 * magnitude
  if (normalized <= 2.5) return 2.5 * magnitude
  if (normalized <= 5) return 5 * magnitude
  return 10 * magnitude
}

const buildCountTicks = (maxValue, targetTickCount = 8) => {
  const max = Math.max(1, Math.ceil(Number(maxValue || 1)))
  const step = niceStep(max, targetTickCount)
  const ticks = [0]

  for (let tick = step; tick < max; tick += step) {
    ticks.push(Math.round(tick))
  }

  if (ticks[ticks.length - 1] !== max) {
    if (max - ticks[ticks.length - 1] < step * 0.45) {
      ticks[ticks.length - 1] = max
      return ticks
    }
    ticks.push(max)
  }

  return ticks
}

async function fetchJson(path, signal) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    signal,
    credentials: 'include',
    headers: { Accept: 'application/json' }
  })
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

function RatingScatterChart({ payload }) {
  const points = Array.isArray(payload?.ratings) ? payload.ratings : []
  if (!points.length) {
    return <p className="result-line">No fully analyzed game ratings yet.</p>
  }

  const width = 920
  const height = 330
  const margin = { top: 18, right: 24, bottom: 72, left: 54 }
  const plotWidth = width - margin.left - margin.right
  const plotHeight = height - margin.top - margin.bottom
  const minRating = Math.min(...points.map((point) => Number(point.rating || 0)))
  const maxRating = Math.max(...points.map((point) => Number(point.rating || 0)))
  const maxIndex = Math.max(1, ...points.map((point) => Number(point.x_index || 0)))
  const ratingSpan = Math.max(1, maxRating - minRating)
  const xTicks = buildCountTicks(maxIndex)
  const yTicks = [minRating, Math.round((minRating + maxRating) / 2), maxRating]
  const xFor = (index) => margin.left + (Number(index || 0) / maxIndex) * plotWidth
  const yFor = (rating) => margin.top + plotHeight - ((Number(rating || 0) - minRating) / ratingSpan) * plotHeight

  return (
    <div className="rating-scatter-wrap">
      <svg className="rating-scatter-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Fully analyzed games sorted by rating">
        {yTicks.map((tick) => {
          const y = yFor(tick)
          return (
            <g key={`y-${tick}`}>
              <line className="rating-grid-line" x1={margin.left} y1={y} x2={width - margin.right} y2={y} />
              <text className="rating-axis-label" x={margin.left - 10} y={y + 4} textAnchor="end">{formatNumber(tick)}</text>
            </g>
          )
        })}
        {xTicks.map((tick) => {
          const x = xFor(tick)
          return (
            <g key={`x-${tick}`}>
              <line className="rating-grid-line vertical" x1={x} y1={margin.top} x2={x} y2={height - margin.bottom} />
              <text className="rating-axis-label" x={x} y={height - margin.bottom + 28} textAnchor="middle">{formatNumber(tick)}</text>
            </g>
          )
        })}
        <line className="rating-axis-line" x1={margin.left} y1={height - margin.bottom} x2={width - margin.right} y2={height - margin.bottom} />
        <line className="rating-axis-line" x1={margin.left} y1={margin.top} x2={margin.left} y2={height - margin.bottom} />
        <text
          className="rating-axis-label rating-axis-title-y"
          x={margin.left - 38}
          y={margin.top + plotHeight / 2}
          textAnchor="middle"
          transform={`rotate(-90 ${margin.left - 38} ${margin.top + plotHeight / 2})`}
        >
          avg elo
        </text>
        <text className="rating-axis-label" x={margin.left + plotWidth / 2} y={height - margin.bottom + 58} textAnchor="middle">
          game count, sorted by avg elo
        </text>
        {points.map((point) => {
          const group = point.group || 'medium'
          return (
            <circle
              cx={xFor(point.x_index)}
              cy={yFor(point.rating)}
              r={3.4}
              fill={RATING_GROUP_COLORS[group] || RATING_GROUP_COLORS.medium}
              key={`${point.link}-${point.x_index}`}
            />
          )
        })}
      </svg>
      <div className="rating-scatter-legend">
        {RATING_GROUP_OPTIONS.map((option) => (
          <span key={`scatter-${option.key}`}>
            <i style={{ background: RATING_GROUP_COLORS[option.key] }} />
            {option.label}
          </span>
        ))}
      </div>
    </div>
  )
}

function ScoredPositions() {
  const [overview, setOverview] = useState(null)
  const [gameOverview, setGameOverview] = useState(null)
  const [advantageByRating, setAdvantageByRating] = useState(null)
  const [selectedRatingGroup, setSelectedRatingGroup] = useState('medium')
  const [error, setError] = useState('')
  const [reloadToken, setReloadToken] = useState(0)

  const ratingGroups = useMemo(() => {
    return Array.isArray(advantageByRating?.groups) ? advantageByRating.groups : []
  }, [advantageByRating])

  const selectedRatingData = useMemo(() => {
    return ratingGroups.find((group) => group.key === selectedRatingGroup) || ratingGroups[0] || null
  }, [ratingGroups, selectedRatingGroup])

  const maxRatingBucketPositions = useMemo(() => {
    const buckets = Array.isArray(selectedRatingData?.buckets) ? selectedRatingData.buckets : []
    return Math.max(1, ...buckets.map((bucket) => Number(bucket.positions || 0)))
  }, [selectedRatingData])

  useEffect(() => {
    const controller = new AbortController()

    const load = async () => {
      setError('')
      try {
        const [overviewPayload, gameOverviewPayload, advantageByRatingPayload] = await Promise.all([
          fetchJson('/fens/scored/overview', controller.signal),
          fetchJson('/fens/scored/games/overview', controller.signal),
          fetchJson('/fens/scored/advantage_by_rating', controller.signal)
        ])
        setOverview(overviewPayload)
        setGameOverview(gameOverviewPayload)
        setAdvantageByRating(advantageByRatingPayload)
      } catch (loadError) {
        if (loadError?.name !== 'AbortError') {
          setError(loadError.message || 'Scored positions unavailable.')
        }
      }
    }

    load()
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
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="scored-positions-main">
          <section className="scored-top-grid">
            <section className="scored-positions-panel scored-engine-panel">
              {error ? <div className="status-banner warn">{error}</div> : null}

              <div className="position-coverage-grid">
                <article className="metric-card">
                  <span>Scored Positions</span>
                  <strong>{overview ? formatNumber(overview.scored_positions) : '-'}</strong>
                </article>
                <article className="metric-card">
                  <span>Unscored FENs</span>
                  <strong>{overview ? formatNumber(overview.unscored_fens) : '-'}</strong>
                </article>
                <article className="metric-card">
                  <span>Fully Scored Games</span>
                  <strong>{gameOverview ? formatNumber(gameOverview.fully_analyzed_games) : '-'}</strong>
                </article>
                <article className="metric-card">
                  <span>Incomplete Games</span>
                  <strong>{gameOverview ? formatNumber(gameOverview.incomplete_games) : '-'}</strong>
                </article>
              </div>
            </section>

            <section className="scored-positions-panel scored-rating-advantage-panel">
              <div className="advantage-rating-head">
                <h2 className="panel-title">ADVANTAGE BY RATING</h2>
                <div className="rating-group-controls" aria-label="Rating groups">
                  {RATING_GROUP_OPTIONS.map((option) => (
                    <button
                      className={`rating-group-button ${selectedRatingGroup === option.key ? 'active' : ''}`}
                      type="button"
                      key={option.key}
                      onClick={() => setSelectedRatingGroup(option.key)}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
              </div>

              {selectedRatingData ? (
                <>
                  <div className="rating-range-summary">
                    <span>{formatNumber(selectedRatingData.min_rating)}-{formatNumber(selectedRatingData.max_rating)} avg elo</span>
                    <span>{formatNumber(selectedRatingData.games)} games</span>
                    <span>{formatNumber(selectedRatingData.positions)} positions</span>
                  </div>
                  <div className="score-bucket-list">
                    {(selectedRatingData.buckets || []).map((bucket) => {
                      const width = Math.max(3, (Number(bucket.positions || 0) / maxRatingBucketPositions) * 100)
                      return (
                        <div className="score-bucket-row" key={`rating-${selectedRatingData.key}-${bucket.key}`}>
                          <span>{bucket.label}</span>
                          <div className="score-bucket-track">
                            <div className={`score-bucket-fill ${bucket.key}`} style={{ width: `${width}%` }} />
                          </div>
                          <strong>{formatNumber(bucket.positions)}</strong>
                        </div>
                      )
                    })}
                  </div>
                </>
              ) : (
                <p className="result-line">No fully analyzed games with ratings yet.</p>
              )}
            </section>
          </section>

          <section className="scored-positions-panel scored-rating-scatter-panel">
            <h2 className="panel-title">RATING GROUPS</h2>
            <RatingScatterChart payload={advantageByRating} />
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default ScoredPositions
