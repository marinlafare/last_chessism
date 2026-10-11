import { memo, useMemo } from 'react'
import { formatNumber } from '../../utils/formatters'
import { ratingScatterBounds, RATING_GROUP_COLORS, RATING_GROUP_OPTIONS } from './ratingScatter'

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
  for (let tick = step; tick < max; tick += step) ticks.push(Math.round(tick))
  if (ticks[ticks.length - 1] !== max) {
    if (max - ticks[ticks.length - 1] < step * 0.45) ticks[ticks.length - 1] = max
    else ticks.push(max)
  }
  return ticks
}

function RatingScatterChart({ payload }) {
  const points = Array.isArray(payload?.ratings) ? payload.ratings : []
  const { minRating, maxRating, maxIndex } = useMemo(() => ratingScatterBounds(points), [points])
  if (!points.length) return <p className="result-line">No fully analyzed game ratings yet.</p>

  const width = 920
  const height = 330
  const margin = { top: 18, right: 24, bottom: 72, left: 54 }
  const plotWidth = width - margin.left - margin.right
  const plotHeight = height - margin.top - margin.bottom
  const ratingSpan = Math.max(1, maxRating - minRating)
  const xTicks = buildCountTicks(maxIndex)
  const yTicks = [...new Set([minRating, Math.round((minRating + maxRating) / 2), maxRating])]
  const xFor = (index) => margin.left + (Number(index || 0) / maxIndex) * plotWidth
  const yFor = (rating) => margin.top + plotHeight - ((Number(rating || 0) - minRating) / ratingSpan) * plotHeight

  return <div className="rating-scatter-wrap">
    <svg className="rating-scatter-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Fully analyzed games sorted by rating">
      {yTicks.map((tick) => {
        const y = yFor(tick)
        return <g key={`y-${tick}`}>
          <line className="rating-grid-line" x1={margin.left} y1={y} x2={width - margin.right} y2={y} />
          <text className="rating-axis-label" x={margin.left - 10} y={y + 4} textAnchor="end">{formatNumber(tick)}</text>
        </g>
      })}
      {xTicks.map((tick) => {
        const x = xFor(tick)
        return <g key={`x-${tick}`}>
          <line className="rating-grid-line vertical" x1={x} y1={margin.top} x2={x} y2={height - margin.bottom} />
          <text className="rating-axis-label" x={x} y={height - margin.bottom + 28} textAnchor="middle">{formatNumber(tick)}</text>
        </g>
      })}
      <line className="rating-axis-line" x1={margin.left} y1={height - margin.bottom} x2={width - margin.right} y2={height - margin.bottom} />
      <line className="rating-axis-line" x1={margin.left} y1={margin.top} x2={margin.left} y2={height - margin.bottom} />
      <text className="rating-axis-label rating-axis-title-y" x={margin.left - 38} y={margin.top + plotHeight / 2}
        textAnchor="middle" transform={`rotate(-90 ${margin.left - 38} ${margin.top + plotHeight / 2})`}>avg elo</text>
      <text className="rating-axis-label" x={margin.left + plotWidth / 2} y={height - margin.bottom + 58} textAnchor="middle">
        game count, sorted by avg elo
      </text>
      {points.map((point) => <circle cx={xFor(point.x_index)} cy={yFor(point.rating)} r={3.4}
        fill={RATING_GROUP_COLORS[point.group || 'medium'] || RATING_GROUP_COLORS.medium}
        key={`${point.link}-${point.x_index}`} />)}
    </svg>
    <div className="rating-scatter-legend">
      {RATING_GROUP_OPTIONS.map((option) => <span key={`scatter-${option.key}`}>
        <i style={{ background: RATING_GROUP_COLORS[option.key] }} />{option.label}
      </span>)}
    </div>
  </div>
}

// Overview/backup polling must not reconcile the same large chart again.
export default memo(RatingScatterChart)
