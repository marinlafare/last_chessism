import { useEffect, useMemo, useState } from 'react'
import { fetchDailyEfficiency } from './playerAnalysisApi'
import PlayerModeToggles, { PLAYER_ANALYSIS_MODES } from './PlayerModeToggles'

const WIDTH = 1200
const HEIGHT = 250
const PLOT_LEFT = 112
const PLOT_RIGHT = 1182
const PLOT_TOP = 14
const PLOT_BOTTOM = 194
const AXIS_Y = 222
const AXIS_TITLE_Y = 244

const emptyRequest = { data: null, error: '', loading: false }

function seriesYears(points) {
  const years = (points || [])
    .map((point) => String(point[0]).slice(0, 4))
    .filter((year) => /^\d{4}$/.test(year))
  return [...new Set(years)].sort((left, right) => Number(left) - Number(right))
}

function efficiencyPoints(points, selectedYear) {
  return (points || [])
    .filter(([date]) => selectedYear === 'all' || String(date).startsWith(`${selectedYear}-`))
    .map(([date, gameEfficiency]) => ({
      date,
      gameEfficiency: Number(gameEfficiency),
      timestamp: Date.parse(`${date}T00:00:00Z`),
    }))
}

function axisDate(timestamp, span) {
  const date = new Date(timestamp)
  if (span > 2200 * 86400000) return String(date.getUTCFullYear())
  if (span > 90 * 86400000) {
    return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}`
  }
  return `${String(date.getUTCMonth() + 1).padStart(2, '0')}-${String(date.getUTCDate()).padStart(2, '0')}`
}

function niceTickStep(range, targetIntervals = 5) {
  const roughStep = Math.max(Number.EPSILON, range / targetIntervals)
  const magnitude = 10 ** Math.floor(Math.log10(roughStep))
  const fraction = roughStep / magnitude
  const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10
  return niceFraction * magnitude
}

function efficiencyScale(points) {
  if (!points.length) return { minimum: 0, maximum: 100, step: 25 }
  const values = points.map((point) => point.gameEfficiency)
  const dataMinimum = Math.min(...values)
  const dataMaximum = Math.max(...values)
  const dataRange = dataMaximum - dataMinimum
  const padding = dataRange > 0 ? Math.min(2, Math.max(0.5, dataRange * 0.08)) : 5
  const paddedMinimum = Math.max(0, dataMinimum - padding)
  const paddedMaximum = Math.min(100, dataMaximum + padding)
  const step = niceTickStep(Math.max(1, paddedMaximum - paddedMinimum))
  let minimum = Math.max(0, Math.floor(paddedMinimum / step) * step)
  let maximum = Math.min(100, Math.ceil(paddedMaximum / step) * step)

  if (maximum <= minimum) {
    minimum = Math.max(0, minimum - step)
    maximum = Math.min(100, maximum + step)
  }
  return { minimum, maximum, step }
}

function buildPlot(points) {
  const scale = efficiencyScale(points)
  const valueRange = Math.max(1, scale.maximum - scale.minimum)
  const yTicks = []
  for (let value = scale.maximum; value >= scale.minimum - Number.EPSILON; value -= scale.step) {
    const normalizedValue = Number(value.toFixed(scale.step < 1 ? 1 : 0))
    yTicks.push({
      value: normalizedValue,
      y: PLOT_TOP + ((scale.maximum - value) / valueRange) * (PLOT_BOTTOM - PLOT_TOP),
    })
  }
  if (!points.length) return { points: [], xTicks: [], yTicks }

  const start = points[0].timestamp
  const end = points.at(-1).timestamp
  const timeSpread = Math.max(1, end - start)
  const plotted = points.map((point) => ({
    ...point,
    x: points.length === 1
      ? (PLOT_LEFT + PLOT_RIGHT) / 2
      : PLOT_LEFT + ((point.timestamp - start) / timeSpread) * (PLOT_RIGHT - PLOT_LEFT),
    y: PLOT_BOTTOM
      - ((point.gameEfficiency - scale.minimum) / valueRange) * (PLOT_BOTTOM - PLOT_TOP),
  }))
  const xTicks = points.length === 1
    ? [{ label: axisDate(start, 0), x: (PLOT_LEFT + PLOT_RIGHT) / 2 }]
    : Array.from({ length: 7 }, (_, index) => {
      const ratio = index / 6
      const timestamp = start + ratio * timeSpread
      return {
        label: axisDate(timestamp, timeSpread),
        x: PLOT_LEFT + ratio * (PLOT_RIGHT - PLOT_LEFT),
      }
    })
  return { points: plotted, xTicks, yTicks }
}

function efficiencyBand(value) {
  if (value >= 90) return 'excellent'
  if (value >= 75) return 'strong'
  if (value >= 60) return 'mixed'
  return 'poor'
}

export default function PlayerDailyEfficiencyChart({ availableModes, playerName, onSelectPoint }) {
  const [activeModes, setActiveModes] = useState(() => new Set(PLAYER_ANALYSIS_MODES))
  const [selectedYear, setSelectedYear] = useState('all')
  const [request, setRequest] = useState(emptyRequest)
  const selectedModes = useMemo(() => PLAYER_ANALYSIS_MODES.filter(
    (mode) => activeModes.has(mode)
  ), [activeModes])
  const modeQuery = selectedModes.join(',')
  const years = useMemo(() => seriesYears(request.data?.points), [request.data])
  const dailyPoints = useMemo(
    () => efficiencyPoints(request.data?.points, selectedYear),
    [request.data, selectedYear]
  )
  const plot = useMemo(() => buildPlot(dailyPoints), [dailyPoints])

  useEffect(() => {
    setActiveModes(new Set(PLAYER_ANALYSIS_MODES))
    setSelectedYear('all')
  }, [playerName])

  useEffect(() => {
    if (selectedYear !== 'all' && !years.includes(selectedYear)) setSelectedYear('all')
  }, [selectedYear, years])

  useEffect(() => {
    if (!playerName || !modeQuery) {
      setRequest(emptyRequest)
      return undefined
    }
    const controller = new AbortController()
    setRequest((current) => ({ ...current, error: '', loading: true }))
    fetchDailyEfficiency({
      playerName,
      mode: modeQuery,
      signal: controller.signal,
      bypassCache: true,
    })
      .then((data) => {
        if (!controller.signal.aborted) setRequest({ data, error: '', loading: false })
      })
      .catch((error) => {
        if (error.name !== 'AbortError') {
          setRequest({ data: null, error: error.message || 'Unable to load daily accuracy.', loading: false })
        }
      })
    return () => controller.abort()
  }, [modeQuery, playerName])

  const toggleMode = (mode) => {
    setActiveModes((current) => {
      const next = new Set(current)
      if (next.has(mode)) next.delete(mode)
      else next.add(mode)
      return next
    })
  }

  return (
    <section className="measure-chart-section measure-daily-efficiency-section" aria-label="Daily game accuracy chart">
      <div className="behavior-subchart-heading measure-chart-heading measure-daily-efficiency-heading">
        <div><strong>Accuracy</strong></div>
        <div className="measure-daily-years" aria-label="Accuracy year">
          {['all', ...years].map((year) => (
            <button
              type="button"
              className={selectedYear === year ? 'active' : ''}
              aria-pressed={selectedYear === year}
              onClick={() => setSelectedYear(year)}
              key={year}
            >
              {year}
            </button>
          ))}
        </div>
        <PlayerModeToggles
          activeModes={activeModes}
          availableModes={availableModes}
          label="Game types shown in accuracy"
          onToggle={toggleMode}
        />
      </div>

      {request.loading ? <div className="measure-chart-message">Calculating daily accuracy…</div> : null}
      {!request.loading && request.error ? <div className="measure-chart-message error" role="alert">{request.error}</div> : null}
      {!request.loading && !request.error ? (
        <svg className="measure-daily-efficiency-graph" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label="Average game accuracy by local date">
          {plot.yTicks.map((tick) => (
            <g key={`daily-efficiency-y-${tick.value}`}>
              <line className="graph-grid-line" x1={PLOT_LEFT} x2={PLOT_RIGHT} y1={tick.y} y2={tick.y} />
              <text className="graph-rating-label" x={PLOT_LEFT - 10} y={tick.y + 3} textAnchor="end">{tick.value}</text>
            </g>
          ))}
          {plot.points.length ? (
            <>
              {plot.points.map((point) => (
                <circle
                  className={`measure-daily-efficiency-point ${efficiencyBand(point.gameEfficiency)}`}
                  cx={point.x}
                  cy={point.y}
                  r="4"
                  role="button"
                  tabIndex="0"
                  aria-label={`${point.date}, ${point.gameEfficiency.toFixed(2)} accuracy. Explore games.`}
                  onClick={() => onSelectPoint?.({
                    scope: 'date',
                    date: point.date,
                    label: point.date,
                    modes: selectedModes,
                    minimum_game_moves_exclusive: 10,
                  })}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter' || event.key === ' ') {
                      event.preventDefault()
                      onSelectPoint?.({
                        scope: 'date',
                        date: point.date,
                        label: point.date,
                        modes: selectedModes,
                        minimum_game_moves_exclusive: 10,
                      })
                    }
                  }}
                  key={point.date}
                >
                  <title>{`${point.date}\n${point.gameEfficiency.toFixed(2)} accuracy`}</title>
                </circle>
              ))}
            </>
          ) : (
            <text className="graph-empty-label" x={(PLOT_LEFT + PLOT_RIGHT) / 2} y={(PLOT_TOP + PLOT_BOTTOM) / 2} textAnchor="middle">No analyzed games in this selection.</text>
          )}
          {plot.xTicks.map((tick, index) => (
            <text className="graph-axis-label measure-daily-x-tick" x={tick.x} y={AXIS_Y} textAnchor={index === 0 ? 'start' : index === plot.xTicks.length - 1 ? 'end' : 'middle'} key={`daily-efficiency-x-${index}`}>{tick.label}</text>
          ))}
          <text className="graph-axis-label measure-daily-axis-title" x={(PLOT_LEFT + PLOT_RIGHT) / 2} y={AXIS_TITLE_Y} textAnchor="middle">DATE</text>
        </svg>
      ) : null}
    </section>
  )
}
