import { useMemo } from 'react'
import { formatNumber } from '../../utils/formatters'
import { TimeContextNotice } from './PlayerAnalyticsShared'

const WIDTH = 1200
const HEIGHT = 580
const PLOT_LEFT = 205
const PLOT_RIGHT = 1172
const PLOT_WIDTH = PLOT_RIGHT - PLOT_LEFT
const HOUR_STEP = PLOT_WIDTH / 24
const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
const MODE_COLORS = {
  bullet: '#15c780',
  blitz: '#d3b66d',
  rapid: '#9a78c5',
  unknown: '#8da1b2',
}

const numeric = (value) => Number(value || 0)
const hourX = (hour) => PLOT_LEFT + numeric(hour) * HOUR_STEP

function localHour(value, fallbackHour = 0) {
  const match = String(value || '').match(/T(\d{2}):(\d{2}):(\d{2})/)
  if (!match) return numeric(fallbackHour)
  return Number(match[1]) + Number(match[2]) / 60 + Number(match[3]) / 3600
}

function resultTotals(rows = []) {
  return rows.reduce((totals, row) => ({
    games: totals.games + numeric(row.games),
    wins: totals.wins + numeric(row.wins),
    draws: totals.draws + numeric(row.draws),
    losses: totals.losses + numeric(row.losses),
  }), { games: 0, wins: 0, draws: 0, losses: 0 })
}

function groupByMode(observations) {
  const grouped = new Map()
  observations.forEach((item) => {
    const current = grouped.get(item.mode) || []
    current.push(item)
    grouped.set(item.mode, current)
  })
  return Array.from(grouped.entries())
}

function buildRatingPlot(ratings, dayData, selectedDate) {
  let groups
  let domainStart
  let domainEnd

  if (selectedDate) {
    const observations = (dayData?.hours || []).flatMap((hour) => (
      (hour.ratings || []).map((item) => ({
        mode: item.mode || 'unknown',
        rating: numeric(item.rating),
        position: localHour(item.played_at_local, hour.hour),
        label: item.played_at_local || `${selectedDate} ${hour.hour}:00`,
      }))
    ))
    groups = groupByMode(observations)
    domainStart = 0
    domainEnd = 24
  } else {
    const observations = (ratings?.series || []).flatMap((series) => (
      (series.points || [])
        .filter((point) => point.last_rating !== null)
        .map((point) => ({
          mode: series.mode || 'unknown',
          rating: numeric(point.last_rating),
          position: Date.parse(`${point.date}T00:00:00Z`),
          label: point.date,
        }))
    ))
    groups = groupByMode(observations)
    domainStart = observations.length ? Math.min(...observations.map((item) => item.position)) : 0
    domainEnd = observations.length ? Math.max(...observations.map((item) => item.position)) : 1
  }

  const allPoints = groups.flatMap(([, points]) => points)
  const minimum = allPoints.length ? Math.min(...allPoints.map((item) => item.rating)) : 0
  const maximum = allPoints.length ? Math.max(...allPoints.map((item) => item.rating)) : 0
  const ratingSpread = Math.max(1, maximum - minimum)
  const timeSpread = Math.max(1, domainEnd - domainStart)
  const chartTop = 447
  const chartBottom = 542

  const lines = groups.map(([mode, points]) => ({
    mode,
    color: MODE_COLORS[mode] || MODE_COLORS.unknown,
    points: [...points]
      .sort((left, right) => left.position - right.position)
      .map((item) => ({
        ...item,
        x: PLOT_LEFT + ((item.position - domainStart) / timeSpread) * PLOT_WIDTH,
        y: chartBottom - ((item.rating - minimum) / ratingSpread) * (chartBottom - chartTop),
      })),
  }))

  return {
    lines,
    minimum,
    maximum,
    startLabel: selectedDate ? '00:00' : ratings?.date_from,
    endLabel: selectedDate ? '24:00' : ratings?.date_to,
  }
}

export default function PlayerBehaviouralPanel({
  activity,
  ratings,
  selectedDate,
  dayData,
  dayLoading,
  dayError,
}) {
  const heatCells = activity?.weekday_hours || []
  const heatMap = useMemo(
    () => new Map(heatCells.map((cell) => [`${cell.weekday}-${cell.hour}`, cell])),
    [heatCells]
  )
  const heatMaximum = Math.max(1, ...heatCells.map((cell) => numeric(cell.games)))
  const hourRows = selectedDate ? (dayData?.hours || []) : (activity?.hours || [])
  const hourMap = useMemo(
    () => new Map(hourRows.map((row) => [numeric(row.hour), row])),
    [hourRows]
  )
  const hourMaximum = Math.max(1, ...hourRows.map((row) => numeric(row.games)))
  const totals = resultTotals(hourRows)
  const ratingPlot = useMemo(
    () => buildRatingPlot(ratings, dayData, selectedDate),
    [ratings, dayData, selectedDate]
  )
  const context = dayData?.time_context || activity?.time_context || ratings?.time_context

  return (
    <div className="behavior-dashboard">
      <TimeContextNotice context={context} />
      <div className="behavior-summary" aria-label="Behavioral selection summary">
        <div><span>Scope</span><strong>{selectedDate || 'All recorded dates'}</strong></div>
        <div><span>Games</span><strong>{formatNumber(selectedDate && !dayData ? 0 : totals.games)}</strong></div>
        <div><span>Results</span><strong>{formatNumber(totals.wins)}W · {formatNumber(totals.draws)}D · {formatNumber(totals.losses)}L</strong></div>
        <div><span>Rating range</span><strong>{ratingPlot.lines.length ? `${formatNumber(ratingPlot.minimum)}–${formatNumber(ratingPlot.maximum)}` : 'N/A'}</strong></div>
      </div>

      <article className="behavior-graph-card">
        <header className="behavior-graph-header">
          <div>
            <h3>Behavioral map</h3>
            <p>Historical weekday/hour habits, hourly outcomes, and rating movement in one view.</p>
          </div>
          <div className="behavior-legend" aria-label="Graph legend">
            <span><i className="games" />Games</span>
            <span><i className="wins" />Wins</span>
            <span><i className="draws" />Draws</span>
            <span><i className="losses" />Losses</span>
          </div>
        </header>

        {dayError ? <p className="player-analysis-error" role="alert">{dayError}</p> : null}
        {dayLoading ? <p className="behavior-day-status">Loading {selectedDate}…</p> : null}

        <div className="behavior-graph-scroll">
          <svg
            className="behavior-graph"
            viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
            role="img"
            aria-label={`Playing patterns for ${selectedDate || 'all recorded dates'}`}
          >
            <text className="graph-section-title" x="0" y="18">WHEN GAMES START · HISTORICAL WEEKDAY × HOUR</text>
            {Array.from({ length: 24 }, (_, hour) => (
              <text className="graph-axis-label" x={hourX(hour) + HOUR_STEP / 2} y="36" textAnchor="middle" key={`heat-hour-${hour}`}>
                {hour % 3 === 0 ? String(hour).padStart(2, '0') : ''}
              </text>
            ))}
            {WEEKDAYS.map((label, dayIndex) => {
              const weekday = activity?.weekdays?.[dayIndex] || {}
              const y = 46 + dayIndex * 22
              return (
                <g key={label}>
                  <text className="graph-weekday-label" x="0" y={y + 13}>{label}</text>
                  <text className="graph-weekday-share" x="34" y={y + 13}>{(numeric(weekday.proportion) * 100).toFixed(1)}%</text>
                  <text className="graph-weekday-results" x="72" y={y + 13}>
                    {`${formatNumber(weekday.wins || 0)}W ${formatNumber(weekday.draws || 0)}D ${formatNumber(weekday.losses || 0)}L`}
                  </text>
                  {Array.from({ length: 24 }, (_, hour) => {
                    const cell = heatMap.get(`${dayIndex + 1}-${hour}`) || {}
                    const games = numeric(cell.games)
                    const balance = numeric(cell.wins) - numeric(cell.losses)
                    return (
                      <rect
                        className={balance < 0 ? 'behavior-heat-loss' : 'behavior-heat-win'}
                        x={hourX(hour) + 2}
                        y={y}
                        width={Math.max(1, HOUR_STEP - 4)}
                        height="17"
                        rx="2"
                        opacity={games ? 0.12 + (games / heatMaximum) * 0.88 : 0.035}
                        key={`${label}-${hour}`}
                      >
                        <title>{`${label} ${String(hour).padStart(2, '0')}:00 · ${formatNumber(games)} games · ${numeric(cell.wins)}W ${numeric(cell.draws)}D ${numeric(cell.losses)}L`}</title>
                      </rect>
                    )
                  })}
                </g>
              )
            })}

            <line className="graph-divider" x1="0" x2={WIDTH} y1="211" y2="211" />
            <text className="graph-section-title" x="0" y="235">
              {selectedDate ? `HOURLY RESULTS · ${selectedDate}` : 'HOURLY RESULTS · ALL RECORDED DATES'}
            </text>
            {[0, 0.5, 1].map((ratio) => {
              const y = 374 - ratio * 116
              return <line className="graph-grid-line" x1={PLOT_LEFT} x2={PLOT_RIGHT} y1={y} y2={y} key={`hour-grid-${ratio}`} />
            })}
            {Array.from({ length: 24 }, (_, hour) => {
              const row = hourMap.get(hour) || {}
              const x = hourX(hour) + 5
              const barWidth = Math.max(4, HOUR_STEP - 10)
              const scale = 116 / hourMaximum
              const lossHeight = numeric(row.losses) * scale
              const drawHeight = numeric(row.draws) * scale
              const winHeight = numeric(row.wins) * scale
              const baseline = 374
              return (
                <g key={`hour-bar-${hour}`}>
                  <rect className="behavior-bar-loss" x={x} y={baseline - lossHeight} width={barWidth} height={lossHeight} />
                  <rect className="behavior-bar-draw" x={x} y={baseline - lossHeight - drawHeight} width={barWidth} height={drawHeight} />
                  <rect className="behavior-bar-win" x={x} y={baseline - lossHeight - drawHeight - winHeight} width={barWidth} height={winHeight} />
                  <rect className="behavior-bar-hitbox" x={x} y="258" width={barWidth} height="116">
                    <title>{`${String(hour).padStart(2, '0')}:00 · ${numeric(row.games)} games · ${numeric(row.wins)}W ${numeric(row.draws)}D ${numeric(row.losses)}L`}</title>
                  </rect>
                  <text className="graph-axis-label" x={x + barWidth / 2} y="393" textAnchor="middle">
                    {hour % 3 === 0 ? String(hour).padStart(2, '0') : ''}
                  </text>
                </g>
              )
            })}
            {!hourRows.some((row) => numeric(row.games)) ? (
              <text className="graph-empty-label" x={(PLOT_LEFT + PLOT_RIGHT) / 2} y="325" textAnchor="middle">
                {dayLoading ? 'Loading this date…' : 'No games were initialized in this selection.'}
              </text>
            ) : null}

            <line className="graph-divider" x1="0" x2={WIDTH} y1="414" y2="414" />
            <text className="graph-section-title" x="0" y="438">
              {selectedDate ? 'RATING THROUGH THE SELECTED DAY' : 'DAILY LAST RATING'}
            </text>
            {[447, 494.5, 542].map((y) => (
              <line className="graph-grid-line" x1={PLOT_LEFT} x2={PLOT_RIGHT} y1={y} y2={y} key={`rating-grid-${y}`} />
            ))}
            {ratingPlot.lines.map((line) => (
              <g key={line.mode}>
                <polyline
                  className="behavior-rating-line"
                  fill="none"
                  stroke={line.color}
                  points={line.points.map((point) => `${point.x},${point.y}`).join(' ')}
                />
                {selectedDate ? line.points.map((point, index) => (
                  <circle cx={point.x} cy={point.y} r="3.4" fill={line.color} key={`${line.mode}-${index}`}>
                    <title>{`${line.mode} · ${point.rating} · ${point.label}`}</title>
                  </circle>
                )) : null}
              </g>
            ))}
            <text className="graph-rating-label" x="0" y="453">{ratingPlot.lines.length ? formatNumber(ratingPlot.maximum) : 'N/A'}</text>
            <text className="graph-rating-label" x="0" y="542">{ratingPlot.lines.length ? formatNumber(ratingPlot.minimum) : 'N/A'}</text>
            <text className="graph-axis-label" x={PLOT_LEFT} y="566">{ratingPlot.startLabel || 'First game'}</text>
            <text className="graph-axis-label" x={PLOT_RIGHT} y="566" textAnchor="end">{ratingPlot.endLabel || 'Latest game'}</text>
            {!ratingPlot.lines.length ? (
              <text className="graph-empty-label" x={(PLOT_LEFT + PLOT_RIGHT) / 2} y="500" textAnchor="middle">No rating observations.</text>
            ) : null}
          </svg>
        </div>

        <footer className="behavior-rating-legend">
          {ratingPlot.lines.map((line) => (
            <span key={line.mode}><i style={{ background: line.color }} />{line.mode}</span>
          ))}
          <small>Hover heat cells, hourly bars, or selected-day rating points for exact values.</small>
        </footer>
      </article>
    </div>
  )
}
