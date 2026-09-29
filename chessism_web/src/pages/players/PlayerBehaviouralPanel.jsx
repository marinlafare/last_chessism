import { useEffect, useMemo, useState } from 'react'
import { formatNumber } from '../../utils/formatters'

const WIDTH = 1200
const HEIGHT = 405
const PLOT_LEFT = 120
const PLOT_RIGHT = 1190
const PLOT_WIDTH = PLOT_RIGHT - PLOT_LEFT
const HOUR_STEP = PLOT_WIDTH / 24
const RATING_TITLE_Y = 18
const RATING_TOP = 34
const RATING_BOTTOM = 129
const RATING_AXIS_Y = 157
const GRAPH_DIVIDER_Y = 177
const HOURLY_TITLE_Y = 199
const HOURLY_TOP = 223
const HOURLY_HEIGHT = 139
const HOURLY_BASELINE = HOURLY_TOP + HOURLY_HEIGHT
const HOURLY_AXIS_Y = 383
const MODE_COLORS = {
  bullet: '#3aa7ff',
  blitz: '#d3b66d',
  rapid: '#f08ac0',
  unknown: '#8da1b2',
}
const RATING_MODES = ['bullet', 'blitz', 'rapid']
const LOSS_COLOR = [213, 92, 92]
const DRAW_COLOR = [141, 107, 180]
const WIN_COLOR = [21, 199, 128]

const numeric = (value) => Number(value || 0)
const hourX = (hour) => PLOT_LEFT + numeric(hour) * HOUR_STEP

function hourResultTooltip(hour, row = {}) {
  return [
    `${String(hour).padStart(2, '0')}:00`,
    `${formatNumber(numeric(row.games))} games`,
    `${formatNumber(numeric(row.wins))} wins`,
    `${formatNumber(numeric(row.draws))} draws`,
    `${formatNumber(numeric(row.losses))} losses`,
  ].join('\n')
}

function scoreColor(row = {}) {
  const games = numeric(row.games)
  if (!games) return '#172029'
  const score = Math.min(1, Math.max(0, (numeric(row.wins) + numeric(row.draws) * 0.5) / games))
  const start = score <= 0.5 ? LOSS_COLOR : DRAW_COLOR
  const end = score <= 0.5 ? DRAW_COLOR : WIN_COLOR
  const segmentPosition = score <= 0.5 ? score * 2 : (score - 0.5) * 2
  const channels = start.map((channel, index) => (
    Math.round(channel + (end[index] - channel) * segmentPosition)
  ))
  return `rgb(${channels.join(', ')})`
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

function buildRatingPlot(ratings) {
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
  const groups = groupByMode(observations)
  const domainStart = observations.length ? Math.min(...observations.map((item) => item.position)) : 0
  const domainEnd = observations.length ? Math.max(...observations.map((item) => item.position)) : 1

  const allPoints = groups.flatMap(([, points]) => points)
  const minimum = allPoints.length ? Math.min(...allPoints.map((item) => item.rating)) : 0
  const maximum = allPoints.length ? Math.max(...allPoints.map((item) => item.rating)) : 0
  const ratingSpread = Math.max(1, maximum - minimum)
  const timeSpread = Math.max(1, domainEnd - domainStart)
  const chartTop = RATING_TOP
  const chartBottom = RATING_BOTTOM

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
    startLabel: ratings?.date_from,
    endLabel: ratings?.date_to,
  }
}

function WeekdayStartChart({ weekdays = [], weekdayHours = [], timeContext }) {
  const [expandedWeekdays, setExpandedWeekdays] = useState(() => new Set())
  const timezoneLabel = timeContext?.timezone || 'UTC'
  const cellsByWeekday = useMemo(() => {
    const grouped = new Map()
    weekdayHours.forEach((cell) => {
      const weekday = numeric(cell.weekday)
      const current = grouped.get(weekday) || []
      current.push(cell)
      grouped.set(weekday, current)
    })
    return grouped
  }, [weekdayHours])
  const toggleWeekday = (weekday) => {
    setExpandedWeekdays((current) => {
      const next = new Set(current)
      if (next.has(weekday)) next.delete(weekday)
      else next.add(weekday)
      return next
    })
  }

  return (
    <section className="behavior-weekday-chart" aria-label={`Usage by weekday and hour in ${timezoneLabel}`}>
      <div className="behavior-subchart-heading">
        <div>
          <strong>Usage</strong>
        </div>
        <div className="behavior-score-scale" aria-label="Performance color scale from loss through draw to win">
          <div><span>Loss</span><span>Draw</span><span>Win</span></div>
          <i />
        </div>
      </div>
      <div className="behavior-weekday-plot-scroll">
        <div className="behavior-weekday-plot-canvas">
          <div className="behavior-weekday-rows">
            {weekdays.map((weekday) => {
              const isExpanded = expandedWeekdays.has(weekday.weekday)
              const cells = cellsByWeekday.get(weekday.weekday) || []
              const cellMap = new Map(cells.map((cell) => [numeric(cell.hour), cell]))
              const noteId = `weekday-usage-note-${weekday.weekday}`
              return (
                <div className={`behavior-weekday-row ${isExpanded ? 'expanded' : ''}`} key={weekday.weekday}>
                  <button
                    className="behavior-weekday-summary"
                    type="button"
                    aria-expanded={isExpanded}
                    aria-describedby={noteId}
                    onClick={() => toggleWeekday(weekday.weekday)}
                  >
                    <span className="behavior-weekday-badge">{String(weekday.label).toUpperCase()}</span>
                    <span className="behavior-weekday-stage">
                      <span className="behavior-weekday-hours" aria-hidden={!isExpanded}>
                        {Array.from({ length: 24 }, (_, hour) => {
                          const cell = cellMap.get(hour) || {}
                          return (
                            <span
                              className="behavior-weekday-hour"
                              key={hour}
                              title={hourResultTooltip(hour, cell)}
                            >
                              <i style={{ backgroundColor: scoreColor(cell) }} />
                            </span>
                          )
                        })}
                      </span>
                      <span className="behavior-weekday-curtain" aria-hidden="true">
                        <span className="behavior-weekday-wide-bar">
                          <i style={{ backgroundColor: scoreColor(weekday) }} />
                        </span>
                      </span>
                    </span>
                  </button>
                  <span className="behavior-weekday-note" id={noteId} role="tooltip">
                    {formatNumber(weekday.games)} games · {formatNumber(weekday.wins)}W, {formatNumber(weekday.draws)}D, {formatNumber(weekday.losses)}L · {(numeric(weekday.proportion) * 100).toFixed(1)}%
                  </span>
                </div>
              )
            })}
          </div>
          <div className="behavior-weekday-axis" aria-hidden="true">
            <div className="behavior-weekday-axis-content">
              <div className="behavior-weekday-ticks">
                {Array.from({ length: 24 }, (_, hour) => (
                  <span key={hour}>{String(hour).padStart(2, '0')}</span>
                ))}
              </div>
              <strong>HRS</strong>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}

export default function PlayerBehaviouralPanel({
  activity,
  ratings,
}) {
  const [visibleRatingModes, setVisibleRatingModes] = useState(() => new Set(RATING_MODES))
  const hourRows = activity?.hours || []
  const hourMap = useMemo(
    () => new Map(hourRows.map((row) => [numeric(row.hour), row])),
    [hourRows]
  )
  const hourResultMaximum = Math.max(
    1,
    ...hourRows.flatMap((row) => [numeric(row.wins), numeric(row.draws), numeric(row.losses)])
  )
  const totals = resultTotals(hourRows)
  const ratingPlot = useMemo(() => buildRatingPlot(ratings), [ratings])
  const availableRatingModes = useMemo(
    () => new Set(ratingPlot.lines.map((line) => line.mode)),
    [ratingPlot.lines]
  )
  const visibleRatingLines = ratingPlot.lines.filter((line) => visibleRatingModes.has(line.mode))
  const timeContext = activity?.time_context || ratings?.time_context || {}
  const timezoneLabel = timeContext.timezone || 'UTC'

  useEffect(() => {
    setVisibleRatingModes(new Set(RATING_MODES))
  }, [ratings])

  const toggleRatingMode = (mode) => {
    setVisibleRatingModes((current) => {
      const next = new Set(current)
      if (next.has(mode)) next.delete(mode)
      else next.add(mode)
      return next
    })
  }

  return (
    <div className="behavior-dashboard">
      <div className="behavior-summary" aria-label="Behavioral selection summary">
        <div><span>Games</span><strong>{formatNumber(totals.games)}</strong></div>
        <div><span>Results</span><strong>{formatNumber(totals.wins)}W · {formatNumber(totals.draws)}D · {formatNumber(totals.losses)}L</strong></div>
        <div><span>Rating range</span><strong>{ratingPlot.lines.length ? `${formatNumber(ratingPlot.minimum)}–${formatNumber(ratingPlot.maximum)}` : 'N/A'}</strong></div>
      </div>

      <article className="behavior-graph-card">
        <header className="behavior-graph-header">
          <div>
            <h3>Behavioral map</h3>
            <p>Weekday habits, hourly outcomes, and rating movement.</p>
          </div>
        </header>

        <div className="behavior-graph-scroll">
          <div className="behavior-graph-canvas">
            <div className="behavior-rating-controls" aria-label="Visible rating series">
              {RATING_MODES.map((mode) => {
                const active = visibleRatingModes.has(mode)
                return (
                  <button
                    type="button"
                    className={active ? 'active' : ''}
                    aria-pressed={active}
                    disabled={!availableRatingModes.has(mode)}
                    style={{ '--mode-color': MODE_COLORS[mode] }}
                    onClick={() => toggleRatingMode(mode)}
                    key={mode}
                  >
                    <i aria-hidden="true" />{mode}
                  </button>
                )
              })}
            </div>
            <svg
              className="behavior-graph"
              viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
              role="img"
              aria-label="Daily ratings followed by hourly results for all recorded dates"
            >
            <text className="graph-section-title" x="0" y={RATING_TITLE_Y}>
              DAILY LAST RATING
            </text>
            {[RATING_TOP, (RATING_TOP + RATING_BOTTOM) / 2, RATING_BOTTOM].map((y) => (
              <line className="graph-grid-line" x1={PLOT_LEFT} x2={PLOT_RIGHT} y1={y} y2={y} key={`rating-grid-${y}`} />
            ))}
            {visibleRatingLines.map((line) => (
              <g key={line.mode}>
                <polyline
                  className="behavior-rating-line"
                  fill="none"
                  stroke={line.color}
                  points={line.points.map((point) => `${point.x},${point.y}`).join(' ')}
                />
              </g>
            ))}
            <text className="graph-rating-label" x="0" y={RATING_TOP + 6}>{ratingPlot.lines.length ? formatNumber(ratingPlot.maximum) : 'N/A'}</text>
            <text className="graph-rating-label" x="0" y={RATING_BOTTOM}>{ratingPlot.lines.length ? formatNumber(ratingPlot.minimum) : 'N/A'}</text>
            <text className="graph-axis-label" x={PLOT_LEFT} y={RATING_AXIS_Y}>{ratingPlot.startLabel || 'First game'}</text>
            <text className="graph-axis-label" x={PLOT_RIGHT} y={RATING_AXIS_Y} textAnchor="end">{ratingPlot.endLabel || 'Latest game'}</text>
            {!ratingPlot.lines.length ? (
              <text className="graph-empty-label" x={(PLOT_LEFT + PLOT_RIGHT) / 2} y={(RATING_TOP + RATING_BOTTOM) / 2} textAnchor="middle">No rating observations.</text>
            ) : null}

            <line className="graph-divider" x1="0" x2={WIDTH} y1={GRAPH_DIVIDER_Y} y2={GRAPH_DIVIDER_Y} />
            <text className="graph-section-title" x="0" y={HOURLY_TITLE_Y}>
              HOURLY RESULTS
            </text>
            <g className="behavior-svg-result-legend" transform={`translate(${PLOT_RIGHT - 160} ${HOURLY_TITLE_Y})`}>
              <rect className="wins" x="0" y="-8" width="8" height="8" rx="2" />
              <text x="12" y="0">Wins</text>
              <rect className="draws" x="52" y="-8" width="8" height="8" rx="2" />
              <text x="64" y="0">Draws</text>
              <rect className="losses" x="112" y="-8" width="8" height="8" rx="2" />
              <text x="124" y="0">Losses</text>
            </g>
            {[0, 0.5, 1].map((ratio) => {
              const y = HOURLY_BASELINE - ratio * HOURLY_HEIGHT
              return (
                <g key={`hour-grid-${ratio}`}>
                  <line className="graph-grid-line" x1={PLOT_LEFT} x2={PLOT_RIGHT} y1={y} y2={y} />
                  <text className="graph-axis-label hourly-y-tick" x={PLOT_LEFT - 10} y={y + 3} textAnchor="end">
                    {formatNumber(Math.round(hourResultMaximum * ratio))}
                  </text>
                </g>
              )
            })}
            {Array.from({ length: 24 }, (_, hour) => {
              const row = hourMap.get(hour) || {}
              const binX = hourX(hour)
              const sideBarWidth = HOUR_STEP * 0.55
              const drawBarWidth = HOUR_STEP * 0.75
              const winX = binX + HOUR_STEP * 0.1
              const lossX = binX + HOUR_STEP * 0.9 - sideBarWidth
              const drawX = binX + (HOUR_STEP - drawBarWidth) / 2
              const scale = HOURLY_HEIGHT / hourResultMaximum
              const lossHeight = numeric(row.losses) * scale
              const drawHeight = numeric(row.draws) * scale
              const winHeight = numeric(row.wins) * scale
              return (
                <g key={`hour-bar-${hour}`}>
                  <rect className="behavior-bar-win" x={winX} y={HOURLY_BASELINE - winHeight} width={sideBarWidth} height={winHeight} />
                  <rect className="behavior-bar-loss" x={lossX} y={HOURLY_BASELINE - lossHeight} width={sideBarWidth} height={lossHeight} />
                  <rect className="behavior-bar-draw" x={drawX} y={HOURLY_BASELINE - drawHeight} width={drawBarWidth} height={drawHeight} />
                  <rect className="behavior-bar-hitbox" x={binX} y={HOURLY_TOP} width={HOUR_STEP} height={HOURLY_HEIGHT}>
                    <title>{hourResultTooltip(hour, row)}</title>
                  </rect>
                  <text className="graph-axis-label hourly-axis-tick" x={binX + HOUR_STEP / 2} y={HOURLY_AXIS_Y} textAnchor="middle">
                    {String(hour).padStart(2, '0')}
                  </text>
                </g>
              )
            })}
            {Array.from({ length: 25 }, (_, boundary) => (
              <line
                className="hourly-bin-divider"
                x1={PLOT_LEFT + boundary * HOUR_STEP}
                x2={PLOT_LEFT + boundary * HOUR_STEP}
                y1={HOURLY_TOP}
                y2={HOURLY_BASELINE}
                key={`hour-boundary-${boundary}`}
              />
            ))}
            {!hourRows.some((row) => numeric(row.games)) ? (
              <text className="graph-empty-label" x={(PLOT_LEFT + PLOT_RIGHT) / 2} y={HOURLY_TOP + HOURLY_HEIGHT / 2} textAnchor="middle">
                No games were initialized in this selection.
              </text>
            ) : null}
            </svg>
          </div>
        </div>

        <footer className="behavior-time-note">
          <small>
            Hours use {timezoneLabel} ({timeContext.source || 'UTC fallback'}).
            {timeContext.is_estimated ? ' Country-center estimate.' : ''}
          </small>
        </footer>

        <WeekdayStartChart
          weekdays={activity?.weekdays}
          weekdayHours={activity?.weekday_hours}
          timeContext={timeContext}
        />
      </article>
    </div>
  )
}
