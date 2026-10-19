import { useEffect, useId, useMemo, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { fetchPlayerRatingDistribution } from './playerAnalysisApi'
import { formatNumber } from '../../utils/formatters'
import { ratingDistributionPlot } from './ratingDistribution.js'
import useSummaryChartWidth from './useSummaryChartWidth'
import { PLAYER_ANALYSIS_MODES } from './PlayerModeToggles'
import './player-rating-distribution.css'

const LEFT = 46, TOP = 12, BOTTOM = 94

export default function PlayerRatingDistribution({ playerName, onExplore }) {
  const [mode, setMode] = useState('all')
  const [retry, setRetry] = useState(0)
  const [request, setRequest] = useState({ loading: true, data: null, error: '' })
  const [focusedBin, setFocusedBin] = useState(null)
  const titleId = useId()
  const chart = useSummaryChartWidth()
  const right = Math.max(LEFT + 1, chart.width - 20)
  useEffect(() => {
    const controller = new AbortController()
    setRequest({ loading: true, data: null, error: '' })
    setFocusedBin(null)
    fetchPlayerRatingDistribution({ playerName, mode, signal: controller.signal }).then(data => {
      if (!controller.signal.aborted) setRequest({ loading: false, data, error: '' })
    }).catch(error => {
      if (!controller.signal.aborted) setRequest({ loading: false, data: null, error: error.message || 'Unable to load ratings.' })
    })
    return () => controller.abort()
  }, [playerName, mode, retry])
  const plot = useMemo(() => ratingDistributionPlot(request.data), [request.data])
  const rangeDescription = plot.isTrimmed
    ? `Automatic range shows ${formatNumber(plot.visiblePercent, 2)}% of rated games, with 20 rating points of padding. Percentages still use all rated games.`
    : 'Full rating range. Percentages use all rated games.'
  const y = value => BOTTOM - value / plot.ceiling * (BOTTOM - TOP)
  const width = plot.bins.length ? (right - LEFT) / plot.bins.length : 0
  const selected = focusedBin === null ? null : plot.bins[focusedBin]
  const describe = bin => `${bin.rating}–${bin.rating + plot.binWidth - 1}: ${formatNumber(bin.percent, 2)}%`
  const openBin = bin => {
    if (!bin || !bin.percent || request.loading || request.data?.mode !== mode) return
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
    setFocusedBin(local.x >= LEFT && local.x < right && local.y >= TOP && local.y <= BOTTOM
      ? Math.floor((local.x - LEFT) / width) : null)
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
          <svg ref={chart.ref} className="rating-distribution-chart" viewBox={`0 0 ${chart.width} 136`} role="img" aria-labelledby={titleId} tabIndex="0"
            aria-label="Percentage of games in each 10-point rating interval. Use left and right arrow keys to explore; press Enter to view games."
            onPointerMove={explore} onPointerLeave={() => setFocusedBin(null)}
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
            {plot.bins.map((bin, index) => <rect key={bin.rating} className={`rating-distribution-bar${index === focusedBin ? ' active' : ''}`}
              onClick={() => openBin(bin)}
              x={LEFT + index * width} y={y(bin.percent)} width={Math.max(0.1, width - Math.min(1, width * 0.15))}
              height={BOTTOM - y(bin.percent)}><title>{describe(bin)}</title></rect>)}
            {plot.ticks.map(index => <text key={index} x={LEFT + (index + 0.5) * width} y={BOTTOM + 18} textAnchor="middle">{plot.bins[index].rating}</text>)}
            <text x={(LEFT + right) / 2} y="132" textAnchor="middle">Rating</text>
          </svg>
          <small className="rating-distribution-note" title={rangeDescription}>{formatNumber(request.data.total_games_count ?? request.data.games_count)} games</small>
          <span className="rating-distribution-accessible-hint" role="status" aria-live="polite">{selected ? describe(selected) : ''}</span>
          {request.data.excluded_games_count > 0 ? <small>{formatNumber(request.data.excluded_games_count)} games without a valid rating excluded.</small> : null}
        </>
      )}
    </div>
  )
}
