import { useEffect, useId, useMemo, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { fetchPlayerRatingDistribution } from './playerAnalysisApi'
import { formatNumber } from '../../utils/formatters'
import { ratingDistributionPlot } from './ratingDistribution.js'
import useSummaryChartWidth from './useSummaryChartWidth'
import { PLAYER_ANALYSIS_MODES } from './PlayerModeToggles'
import { useAnalysisVisible } from './PlayerAnalysisDisclosure'
import './player-rating-distribution.css'

const LEFT = 46, TOP = 12, BOTTOM = 94
const SHORT_BAR_HEIGHT = 14, ZOOM_BINS = 5, ZOOM_WIDTH = 300
const percentage = new Intl.NumberFormat('en-US', { maximumFractionDigits: 4 })

export default function PlayerRatingDistribution({ playerName, onExplore, summaryRevision }) {
  const visible = useAnalysisVisible()
  const [mode, setMode] = useState('all')
  const [retry, setRetry] = useState(0)
  const [request, setRequest] = useState({ loading: true, data: null, error: '' })
  const [focusedBin, setFocusedBin] = useState(null)
  const [zoomIndex, setZoomIndex] = useState(null)
  const titleId = useId()
  const chart = useSummaryChartWidth()
  const right = Math.max(LEFT + 1, chart.width - 20)
  useEffect(() => {
    if (!visible) return undefined
    const controller = new AbortController()
    setRequest({ loading: true, data: null, error: '' })
    setFocusedBin(null)
    setZoomIndex(null)
    fetchPlayerRatingDistribution({ playerName, mode, signal: controller.signal }).then(data => {
      if (!controller.signal.aborted) setRequest({ loading: false, data, error: '' })
    }).catch(error => {
      if (!controller.signal.aborted) setRequest({ loading: false, data: null, error: error.message || 'Unable to load ratings.' })
    })
    return () => controller.abort()
  }, [playerName, mode, retry, visible, summaryRevision])
  useEffect(() => { setZoomIndex(null) }, [chart.width])
  const zoomOpen = zoomIndex !== null
  useEffect(() => {
    if (!zoomOpen) return undefined
    const dismiss = () => setZoomIndex(null)
    const escape = event => { if (event.key === 'Escape') dismiss() }
    window.addEventListener('scroll', dismiss, true)
    window.addEventListener('resize', dismiss)
    window.addEventListener('keydown', escape)
    return () => {
      window.removeEventListener('scroll', dismiss, true)
      window.removeEventListener('resize', dismiss)
      window.removeEventListener('keydown', escape)
    }
  }, [zoomOpen])
  const plot = useMemo(() => ratingDistributionPlot(request.data), [request.data])
  const rangeDescription = plot.isTrimmed
    ? `Automatic range shows ${formatNumber(plot.visiblePercent, 2)}% of rated games, with 20 rating points of padding. Percentages still use all rated games.`
    : 'Full rating range. Percentages use all rated games.'
  const y = value => BOTTOM - value / plot.ceiling * (BOTTOM - TOP)
  const width = plot.bins.length ? (right - LEFT) / plot.bins.length : 0
  const selected = focusedBin === null ? null : plot.bins[focusedBin]
  const zoomStart = Math.max(0, Math.min(plot.bins.length - ZOOM_BINS, (zoomIndex ?? 0) - Math.floor(ZOOM_BINS / 2)))
  const zoomBins = zoomOpen ? plot.bins.slice(zoomStart, zoomStart + ZOOM_BINS) : []
  const zoomMaximum = zoomBins.reduce((maximum, bin) => Math.max(maximum, bin.percent), 0) || 1
  const zoomWidth = Math.min(ZOOM_WIDTH, chart.width)
  const zoomLeft = Math.max(0, Math.min(chart.width - zoomWidth, LEFT + ((zoomIndex ?? 0) + 0.5) * width - zoomWidth / 2))
  const describe = bin => `${bin.rating}–${bin.rating + plot.binWidth - 1}: ${percentage.format(bin.percent)}%`
  const openBin = bin => {
    if (!bin || !bin.percent || request.loading || request.data?.mode !== mode) return
    setZoomIndex(null)
    onExplore?.({
      scope: 'rating', rating_from: bin.rating, analyzed_only: false,
      modes: mode === 'all' ? PLAYER_ANALYSIS_MODES : [mode],
    })
  }
  const explore = event => {
    const transform = event.currentTarget.getScreenCTM()
    if (!transform || !plot.bins.length) return
    const point = event.currentTarget.createSVGPoint()
    point.x = event.clientX; point.y = event.clientY
    const local = point.matrixTransform(transform.inverse())
    const index = local.x >= LEFT && local.x < right && local.y >= TOP && local.y <= BOTTOM + 6
      ? Math.floor((local.x - LEFT) / width) : null
    setFocusedBin(index)
    const bin = index === null ? null : plot.bins[index]
    if (event.pointerType !== 'touch' && bin?.percent > 0
      && BOTTOM - y(bin.percent) < SHORT_BAR_HEIGHT
      && local.y >= BOTTOM - SHORT_BAR_HEIGHT - 4) {
      // Keep the lens stationary while exploring its neighborhood so it is
      // easy to move from a tiny bar into the enlarged, clickable columns.
      setZoomIndex(current => current !== null && index >= zoomStart && index < zoomStart + ZOOM_BINS ? current : index)
    } else if (!(zoomOpen && event.pointerType !== 'touch'
      && local.x >= zoomLeft && local.x <= zoomLeft + zoomWidth
      && local.y >= BOTTOM - SHORT_BAR_HEIGHT - 4 && local.y <= BOTTOM + 6)) {
      // Allow crossing an empty neighboring interval on the way into the lens.
      setZoomIndex(null)
    }
  }

  return (
    <div {...pageStyleTarget('chart.bars')} className="player-rating-distribution" aria-busy={request.loading}>
      <div className="rating-distribution-heading">
        <span {...pageStyleTarget('label')} id={titleId}>Rating distribution</span>
        <select {...pageStyleTarget('field')} aria-label="Rating distribution game mode" value={mode} onChange={event => setMode(event.target.value)}>
          <option value="all">All</option><option value="bullet">Bullet</option>
          <option value="blitz">Blitz</option><option value="rapid">Rapid</option>
        </select>
      </div>
      {request.loading ? <p role="status">Loading ratings…</p> : request.error ? (
        <div role="alert"><p>{request.error}</p><button {...pageStyleTarget('button')} type="button" onClick={() => setRetry(value => value + 1)}>Retry</button></div>
      ) : !plot.bins.length ? <p role="status">No rated games in this selection.</p> : (
        <>
          <div className="rating-distribution-plot" onPointerLeave={() => { setFocusedBin(null); setZoomIndex(null) }}
            onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget)) setZoomIndex(null) }}>
          <svg ref={chart.ref} className="rating-distribution-chart" viewBox={`0 0 ${chart.width} 136`} role="img" aria-labelledby={titleId} tabIndex="0"
            aria-label="Percentage of games in each 10-point rating interval. Use left and right arrow keys to explore; press Enter to view games."
            onPointerMove={explore}
            onFocus={() => setFocusedBin(0)} onBlur={() => setFocusedBin(null)}
            onKeyDown={event => {
              if (event.key === 'Escape') setFocusedBin(null)
              if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault()
                openBin(selected)
              }
              if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
                event.preventDefault()
                setFocusedBin(value => Math.max(0, Math.min(plot.bins.length - 1, (value ?? 0) + (event.key === 'ArrowRight' ? 1 : -1))))
              }
            }}>
            <desc>Recorded own ratings in {formatNumber(request.data.games_count)} games. Percentages, not cumulative probabilities. {rangeDescription}</desc>
            {[0, 0.5, 1].map(ratio => (
              <g key={ratio} className="rating-distribution-grid" aria-hidden="true">
                <line x1={LEFT} x2={right} y1={y(ratio * plot.ceiling)} y2={y(ratio * plot.ceiling)} />
                <text x={LEFT - 8} y={y(ratio * plot.ceiling) + 4} textAnchor="end">{formatNumber(ratio * plot.ceiling)}%</text>
              </g>
            ))}
            {plot.bins.map((bin, index) => <g key={bin.rating} onClick={() => openBin(bin)}>
              <title>{describe(bin)}</title>
              {bin.percent > 0 ? <rect className="rating-distribution-hit-area"
                x={LEFT + index * width} y={Math.min(y(bin.percent), BOTTOM - SHORT_BAR_HEIGHT)}
                width={width} height={Math.max(BOTTOM - y(bin.percent), SHORT_BAR_HEIGHT)} /> : null}
              <rect className={`rating-distribution-bar${index === focusedBin ? ' active' : ''}`}
                x={LEFT + index * width} y={y(bin.percent)} width={Math.max(0.1, width - Math.min(1, width * 0.15))}
                height={BOTTOM - y(bin.percent)} />
            </g>)}
            {plot.ticks.map(index => <text key={index} x={LEFT + (index + 0.5) * width} y={BOTTOM + 18} textAnchor="middle">{plot.bins[index].rating}</text>)}
            <text x={(LEFT + right) / 2} y="132" textAnchor="middle">Rating</text>
          </svg>
          {zoomBins.length > 0 ? (
            <div className="rating-distribution-zoom" role="group" aria-label="Enlarged rating intervals. Select an interval to view its games."
              style={{ left: zoomLeft, width: zoomWidth, bottom: 136 - BOTTOM + SHORT_BAR_HEIGHT }}>
              <div className="rating-distribution-zoom-heading">
                <b>Hover zoom</b><small>Scale: 0–{percentage.format(zoomMaximum)}%</small>
              </div>
              <div className="rating-distribution-zoom-columns">
                {zoomBins.map((bin, index) => (
                  <button key={bin.rating} type="button" disabled={!bin.percent}
                    className={`rating-distribution-zoom-column${zoomStart + index === focusedBin ? ' active' : ''}`}
                    aria-label={`${describe(bin)}. View games.`}
                    title={describe(bin)}
                    onPointerEnter={() => setFocusedBin(zoomStart + index)} onFocus={() => setFocusedBin(zoomStart + index)}
                    onClick={() => openBin(bin)}>
                    <small>{percentage.format(bin.percent)}%</small>
                    <i className="rating-distribution-zoom-track" aria-hidden="true">
                      {bin.percent > 0 ? <i style={{ height: `${bin.percent / zoomMaximum * 100}%` }} /> : null}
                    </i>
                    <small>{bin.rating}</small>
                  </button>
                ))}
              </div>
            </div>
          ) : null}
          </div>
          <small className="rating-distribution-note" title={rangeDescription}>{formatNumber(request.data.total_games_count ?? request.data.games_count)} games</small>
          <span className="rating-distribution-accessible-hint" role="status" aria-live="polite">{selected ? describe(selected) : ''}</span>
          {request.data.excluded_games_count > 0 ? <small>{formatNumber(request.data.excluded_games_count)} games without a valid rating excluded.</small> : null}
        </>
      )}
    </div>
  )
}
