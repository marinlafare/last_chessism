import { useEffect, useMemo, useState } from 'react'
import { formatNumber } from '../../utils/formatters'
import PlayerModeToggles, {
  PLAYER_ANALYSIS_MODES,
  PLAYER_MODE_COLORS,
} from './PlayerModeToggles'

const WIDTH = 1200
const RATING_SVG_HEIGHT = 156
const HOURS_SVG_HEIGHT = 210
const PLOT_LEFT = 120
const PLOT_RIGHT = 1190
const PLOT_WIDTH = PLOT_RIGHT - PLOT_LEFT
const HOUR_STEP = PLOT_WIDTH / 24
const RATING_TOP = 8
const RATING_BOTTOM = 118
const RATING_AXIS_Y = 146
const HOURLY_TOP = 8
const HOURLY_HEIGHT = 139
const HOURLY_BASELINE = HOURLY_TOP + HOURLY_HEIGHT
const HOURLY_AXIS_Y = 168
const HOURLY_AXIS_TITLE_Y = 201
const ALL_RATING_MODES = new Set(PLAYER_ANALYSIS_MODES)
const RESULT_FIELDS = ['games', 'wins', 'draws', 'losses']
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

function buildRatingPlot(ratings, selectedYear = 'all') {
  const observations = (ratings?.series || []).flatMap((series) => (
    (series.points || [])
      .filter((point) => (
        point.last_rating !== null
        && (selectedYear === 'all' || String(point.date).startsWith(`${selectedYear}-`))
      ))
      .map((point) => ({
        mode: series.mode || 'unknown',
        rating: numeric(point.last_rating),
        position: Date.parse(`${point.date}T00:00:00Z`),
        label: point.date,
      }))
  ))
  const groups = groupByMode(observations)
  const observationDates = observations.map((item) => item.label).sort()
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
    color: PLAYER_MODE_COLORS[mode] || PLAYER_MODE_COLORS.unknown,
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
    startLabel: observationDates[0],
    endLabel: observationDates.at(-1),
  }
}

function getRatingYears(ratings) {
  const years = (ratings?.series || []).flatMap((series) => (
    (series.points || [])
      .filter((point) => point.last_rating !== null)
      .map((point) => String(point.date).slice(0, 4))
      .filter((year) => /^\d{4}$/.test(year))
  ))
  return [...new Set(years)].sort((left, right) => Number(left) - Number(right))
}

function activityForModes(activity, activeModes) {
  if (!activity?.by_mode) return activity || { weekdays: [], hours: [], weekday_hours: [] }
  const selected = PLAYER_ANALYSIS_MODES
    .filter((mode) => activeModes.has(mode))
    .map((mode) => activity.by_mode[mode])
    .filter(Boolean)
  const totalGames = selected.reduce((total, mode) => total + numeric(mode.total_games), 0)
  const combineRows = (key, identity) => {
    const modeMaps = selected.map((mode) => new Map(
      (mode[key] || []).map((row) => [identity(row), row])
    ))
    return (activity[key] || []).map((template) => {
      const bucket = { ...template, games: 0, wins: 0, draws: 0, losses: 0 }
      modeMaps.forEach((modeMap) => {
        const row = modeMap.get(identity(template)) || {}
        RESULT_FIELDS.forEach((field) => {
          bucket[field] += numeric(row[field])
        })
      })
      bucket.proportion = totalGames ? bucket.games / totalGames : 0
      return bucket
    })
  }
  return {
    total_games: totalGames,
    weekdays: combineRows('weekdays', (row) => numeric(row.weekday)),
    hours: combineRows('hours', (row) => numeric(row.hour)),
    weekday_hours: combineRows(
      'weekday_hours',
      (row) => `${numeric(row.weekday)}-${numeric(row.hour)}`
    ),
  }
}

function WeekdayStartChart({
  availableModes,
  onToggleMode,
  timeContext,
  visibleModes,
  weekdayHours = [],
  weekdays = [],
}) {
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
    <section className="behavior-weekday-chart" aria-label={`Days by weekday and hour in ${timezoneLabel}`}>
      <div className="behavior-subchart-heading">
        <div>
          <strong>Days</strong>
        </div>
        <div className="behavior-score-scale" aria-label="Performance color scale from loss through draw to win">
          <div><span>Loss</span><span>Draw</span><span>Win</span></div>
          <i />
        </div>
        <PlayerModeToggles
          activeModes={visibleModes}
          availableModes={availableModes}
          label="Game types shown in Days"
          onToggle={onToggleMode}
        />
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
  const [visibleRatingModes, setVisibleRatingModes] = useState(() => new Set(PLAYER_ANALYSIS_MODES))
  const [visibleHourModes, setVisibleHourModes] = useState(() => new Set(PLAYER_ANALYSIS_MODES))
  const [visibleDayModes, setVisibleDayModes] = useState(() => new Set(PLAYER_ANALYSIS_MODES))
  const [selectedRatingYear, setSelectedRatingYear] = useState('all')
  const hourActivity = useMemo(
    () => activityForModes(activity, visibleHourModes),
    [activity, visibleHourModes]
  )
  const dayActivity = useMemo(
    () => activityForModes(activity, visibleDayModes),
    [activity, visibleDayModes]
  )
  const hourRows = hourActivity.hours || []
  const hourMap = useMemo(
    () => new Map(hourRows.map((row) => [numeric(row.hour), row])),
    [hourRows]
  )
  const hourResultMaximum = Math.max(
    1,
    ...hourRows.flatMap((row) => [numeric(row.wins), numeric(row.draws), numeric(row.losses)])
  )
  const totals = resultTotals(activityForModes(activity, ALL_RATING_MODES).hours)
  const ratingYears = useMemo(() => getRatingYears(ratings), [ratings])
  const ratingPlot = useMemo(
    () => buildRatingPlot(ratings, selectedRatingYear),
    [ratings, selectedRatingYear]
  )
  const availableRatingModes = useMemo(
    () => new Set(ratingPlot.lines.map((line) => line.mode)),
    [ratingPlot.lines]
  )
  const availableActivityModes = useMemo(
    () => new Set(PLAYER_ANALYSIS_MODES.filter((mode) => (
      !activity?.by_mode || numeric(activity.by_mode[mode]?.total_games) > 0
    ))),
    [activity]
  )
  const visibleRatingLines = ratingPlot.lines.filter((line) => visibleRatingModes.has(line.mode))
  const timeContext = activity?.time_context || ratings?.time_context || {}

  useEffect(() => {
    setVisibleRatingModes(new Set(PLAYER_ANALYSIS_MODES))
    setSelectedRatingYear('all')
  }, [ratings])

  useEffect(() => {
    setVisibleHourModes(new Set(PLAYER_ANALYSIS_MODES))
    setVisibleDayModes(new Set(PLAYER_ANALYSIS_MODES))
  }, [activity])

  const toggleMode = (setter, mode) => {
    setter((current) => {
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
            <section className="behavior-chart-section" aria-label="Rating chart">
              <div className="behavior-subchart-heading behavior-rating-heading">
                <div>
                  <strong>Rating</strong>
                </div>
                <div className="behavior-rating-years" aria-label="Rating year">
                  {['all', ...ratingYears].map((year) => (
                    <button
                      type="button"
                      className={selectedRatingYear === year ? 'active' : ''}
                      aria-pressed={selectedRatingYear === year}
                      onClick={() => setSelectedRatingYear(year)}
                      key={year}
                    >
                      {year}
                    </button>
                  ))}
                </div>
                <PlayerModeToggles
                  activeModes={visibleRatingModes}
                  availableModes={availableRatingModes}
                  colored
                  label="Game types shown in Rating"
                  onToggle={(mode) => toggleMode(setVisibleRatingModes, mode)}
                />
              </div>
              <svg
                className="behavior-graph"
                viewBox={`0 0 ${WIDTH} ${RATING_SVG_HEIGHT}`}
                role="img"
                aria-label="Daily last ratings for the selected game types and year"
              >
                {[0, 1 / 3, 2 / 3, 1].map((ratio) => {
                  const y = RATING_BOTTOM - ratio * (RATING_BOTTOM - RATING_TOP)
                  const value = ratingPlot.minimum + ratio * (ratingPlot.maximum - ratingPlot.minimum)
                  return (
                    <g key={`rating-grid-${ratio}`}>
                      <line className="graph-grid-line" x1={PLOT_LEFT} x2={PLOT_RIGHT} y1={y} y2={y} />
                      <text className="graph-rating-label" x={PLOT_LEFT - 10} y={y + 3} textAnchor="end">
                        {ratingPlot.lines.length ? formatNumber(Math.round(value)) : 'N/A'}
                      </text>
                    </g>
                  )
                })}
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
                <text className="graph-axis-label" x={PLOT_LEFT} y={RATING_AXIS_Y}>{ratingPlot.startLabel || 'First game'}</text>
                <text className="graph-axis-label" x={PLOT_RIGHT} y={RATING_AXIS_Y} textAnchor="end">{ratingPlot.endLabel || 'Latest game'}</text>
                {!ratingPlot.lines.length ? (
                  <text className="graph-empty-label" x={(PLOT_LEFT + PLOT_RIGHT) / 2} y={(RATING_TOP + RATING_BOTTOM) / 2} textAnchor="middle">No rating observations.</text>
                ) : null}
              </svg>
            </section>

            <section className="behavior-chart-section behavior-hours-chart" aria-label="Hours chart">
              <div className="behavior-subchart-heading">
                <div>
                  <strong>Hours</strong>
                </div>
                <div className="behavior-result-legend" aria-label="Result colors">
                  <span><i className="wins" />Wins</span>
                  <span><i className="draws" />Draws</span>
                  <span><i className="losses" />Losses</span>
                </div>
                <PlayerModeToggles
                  activeModes={visibleHourModes}
                  availableModes={availableActivityModes}
                  label="Game types shown in Hours"
                  onToggle={(mode) => toggleMode(setVisibleHourModes, mode)}
                />
              </div>
              <svg
                className="behavior-graph"
                viewBox={`0 0 ${WIDTH} ${HOURS_SVG_HEIGHT}`}
                role="img"
                aria-label="Hourly results for the selected game types"
              >
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
                <text
                  className="graph-axis-label hourly-axis-title"
                  x={(PLOT_LEFT + PLOT_RIGHT) / 2}
                  y={HOURLY_AXIS_TITLE_Y}
                  textAnchor="middle"
                >
                  HRS
                </text>
              </svg>
            </section>
          </div>
        </div>

        <WeekdayStartChart
          availableModes={availableActivityModes}
          onToggleMode={(mode) => toggleMode(setVisibleDayModes, mode)}
          timeContext={timeContext}
          visibleModes={visibleDayModes}
          weekdays={dayActivity.weekdays}
          weekdayHours={dayActivity.weekday_hours}
        />
      </article>
    </div>
  )
}
