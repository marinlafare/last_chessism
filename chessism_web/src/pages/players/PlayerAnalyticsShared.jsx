import { memo, useMemo } from 'react'
import { formatNumber } from '../../utils/formatters'

export const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

export function TimeContextNotice({ context }) {
  if (!context) return null
  return (
    <div className={`hero-time-context ${context.time_basis === 'utc' ? 'utc' : ''}`}>
      <div>
        <span>TIME BASIS</span>
        <strong>{context.timezone}</strong>
      </div>
      <p>{context.message || 'Dates and hours use the player’s resolved local timezone.'}</p>
      {context.component_timezones?.length > 1 ? (
        <small>Centered from: {context.component_timezones.join(' + ')}</small>
      ) : null}
    </div>
  )
}

export function AnalyticsCard({ title, subtitle, metric, children, className = '' }) {
  return (
    <article className={`player-chart-card hero-analytics-card ${className}`}>
      <header className="player-chart-head">
        <div><h3>{title}</h3><p>{subtitle}</p></div>
        {metric ? <span>{metric}</span> : null}
      </header>
      <div className="player-chart-body">{children}</div>
    </article>
  )
}

export function EmptyAnalytics({ children }) {
  return <p className="player-chart-empty">{children}</p>
}

export const WeekdayHourHeatmap = memo(function WeekdayHourHeatmap({
  cells = [],
  value = (cell) => cell.games,
  title = (cell) => `${formatNumber(cell.games)} games`,
  signed = false,
}) {
  const cellMap = useMemo(
    () => new Map(cells.map((cell) => [`${cell.weekday}-${cell.hour}`, Number(value(cell) || 0)])),
    [cells, value]
  )
  const maximum = Math.max(1, ...Array.from(cellMap.values()).map((item) => Math.abs(item)))
  return (
    <div className="hero-hour-heatmap" role="img" aria-label="Weekday by hour heatmap">
      <span />
      {Array.from({ length: 24 }, (_, hour) => <small key={hour}>{hour % 3 === 0 ? hour : ''}</small>)}
      {WEEKDAYS.map((day, dayIndex) => (
        <div className="hero-hour-heatmap-row" key={day}>
          <strong>{day}</strong>
          {Array.from({ length: 24 }, (_, hour) => {
            const original = cells.find((cell) => cell.weekday === dayIndex + 1 && cell.hour === hour) || { weekday: dayIndex + 1, hour }
            const cellValue = cellMap.get(`${dayIndex + 1}-${hour}`) || 0
            return (
              <i
                key={hour}
                title={`${day} ${String(hour).padStart(2, '0')}:00 · ${title(original)}`}
                className={signed && cellValue < 0 ? 'negative' : ''}
                style={{ '--heat': Math.abs(cellValue) / maximum }}
              />
            )
          })}
        </div>
      ))}
    </div>
  )
})

function ratingPath(points, width = 600, height = 120) {
  const observed = points.filter((point) => point.last_rating !== null)
  if (!observed.length) return ''
  const ratings = observed.map((point) => Number(point.last_rating))
  const minimum = Math.min(...ratings)
  const maximum = Math.max(...ratings)
  const spread = Math.max(1, maximum - minimum)
  const pointIndex = new Map(points.map((point, index) => [point.date, index]))
  return observed.map((point, index) => {
    const sourceIndex = pointIndex.get(point.date)
    const x = points.length === 1 ? width / 2 : (sourceIndex / (points.length - 1)) * width
    const y = height - ((Number(point.last_rating) - minimum) / spread) * height
    return `${index ? 'L' : 'M'} ${x.toFixed(1)} ${y.toFixed(1)}`
  }).join(' ')
}

export const RatingSeries = memo(function RatingSeries({ series = [] }) {
  if (!series.some((item) => item.points?.some((point) => point.last_rating !== null))) {
    return <EmptyAnalytics>No ratings match this selection.</EmptyAnalytics>
  }
  return (
    <div className="hero-rating-series">
      <svg viewBox="0 0 600 124" role="img" aria-label="Daily rating series" preserveAspectRatio="none">
        <path className="chart-grid" d="M0 31 H600 M0 62 H600 M0 93 H600" />
        {series.map((item) => <path className={item.mode} key={item.mode} d={ratingPath(item.points || [])} />)}
      </svg>
      <div className="hero-rating-legend">
        {series.map((item) => {
          const latest = [...(item.points || [])].reverse().find((point) => point.last_rating !== null)
          return <span className={item.mode} key={item.mode}>{item.mode} <strong>{latest ? formatNumber(latest.last_rating) : 'N/A'}</strong></span>
        })}
      </div>
    </div>
  )
})
