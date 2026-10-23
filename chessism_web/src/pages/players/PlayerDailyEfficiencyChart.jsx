import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useMemo, useRef, useState } from 'react'
import { fetchDailyEfficiency, fetchDailySalienceAccuracy } from './playerAnalysisApi'
import PlayerModeToggles, { PLAYER_ANALYSIS_MODES } from './PlayerModeToggles'
import DailySalienceRange from './DailySalienceRange'
import DailyYearBands from './DailyYearBands'
import { buildYearScale, yearAtX } from './dailyYearNavigation'
import { relativeDailySalience, filterDailySalience } from './dailySalience'
import './daily-salience.css'
import { useAnalysisVisible } from './PlayerAnalysisDisclosure'

const WIDTH = 1200
const HEIGHT = 234
const PLOT_LEFT = 112
const PLOT_RIGHT = 1182
const PLOT_TOP = 14
const PLOT_BOTTOM = 194
const AXIS_Y = 222
const MODE_CACHE_MS = 60000

const emptyRequest = { data: null, error: '', loading: false, playerName: null, modeQuery: '' }

function seriesYears(points) {
  const years = (points || [])
    .map((point) => String(point[0]).slice(0, 4))
    .filter((year) => /^\d{4}$/.test(year))
  return [...new Set(years)].sort((left, right) => Number(left) - Number(right))
}

function efficiencyPoints(points, salienceReady) {
  return (points || [])
    .map(([date, weightedAccuracy, unweightedAccuracy, dailySalience, games]) => ({
      date,
      gameEfficiency: Number(weightedAccuracy),
      unweightedAccuracy: Number.isFinite(Number(unweightedAccuracy))
        ? Number(unweightedAccuracy)
        : Number(weightedAccuracy),
      dailySalience: salienceReady && dailySalience !== null && dailySalience !== undefined
        && Number.isFinite(Number(dailySalience))
        ? Math.max(0, Number(dailySalience))
        : null,
      games: Number.isFinite(Number(games)) ? Number(games) : null,
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
  if (!points.length) return { points: [], xTicks: [], yTicks, yearBands: [] }

  const { start, end, bands: yearBands } = buildYearScale(points, PLOT_LEFT, PLOT_RIGHT)
  const timeSpread = Math.max(1, end - start)
  const plotted = points.map((point) => ({
    ...point,
    x: PLOT_LEFT + ((point.timestamp - start) / timeSpread) * (PLOT_RIGHT - PLOT_LEFT),
    y: PLOT_BOTTOM
      - ((point.gameEfficiency - scale.minimum) / valueRange) * (PLOT_BOTTOM - PLOT_TOP),
  }))
  const xTicks = Array.from({ length: 7 }, (_, index) => {
    const ratio = index / 6
    const timestamp = start + ratio * timeSpread
    return {
      label: axisDate(timestamp, timeSpread),
      x: PLOT_LEFT + ratio * (PLOT_RIGHT - PLOT_LEFT),
    }
  })
  return { points: plotted, xTicks, yTicks, yearBands }
}

function efficiencyBand(value) {
  if (value >= 90) return 'excellent'
  if (value >= 75) return 'strong'
  if (value >= 60) return 'mixed'
  return 'poor'
}

export default function PlayerDailyEfficiencyChart({ availableModes, playerName, onSelectPoint }) {
  const visible = useAnalysisVisible()
  const [activeModes, setActiveModes] = useState(() => new Set(PLAYER_ANALYSIS_MODES))
  const [selectedYear, setSelectedYear] = useState('all')
  const [hoveredYear, setHoveredYear] = useState(null)
  const [focusedYear, setFocusedYear] = useState(null)
  const allYearsButton = useRef(null)
  const [request, setRequest] = useState(emptyRequest)
  const modeCache = useRef(new Map())
  const [refreshToken, setRefreshToken] = useState(0)
  const [salienceRange, setSalienceRange] = useState([0, 100])
  const selectedModes = useMemo(() => PLAYER_ANALYSIS_MODES.filter(
    (mode) => activeModes.has(mode)
  ), [activeModes])
  const modeQuery = selectedModes.join(',')
  const data = request.playerName === playerName ? request.data : null
  const selectionReady = Boolean(data && request.modeQuery === modeQuery && !request.loading && !request.error)
  const years = useMemo(() => seriesYears(data?.points), [data])
  const salienceReady = data?.salience?.status === 'ready'
  const salienceScale = useMemo(
    () => relativeDailySalience(efficiencyPoints(data?.points, salienceReady)),
    [data, salienceReady]
  )
  const dailyPoints = useMemo(
    () => salienceScale.points.filter((point) => selectedYear === 'all' || String(point.date).startsWith(`${selectedYear}-`)),
    [salienceScale, selectedYear]
  )
  // Plot before hiding points: both axes and normalization stay fixed while dragging.
  const plot = useMemo(() => buildPlot(dailyPoints), [dailyPoints])
  const visiblePoints = useMemo(() => filterDailySalience(plot.points, salienceRange, salienceScale.enabled),
    [plot, salienceRange, salienceScale.enabled])
  const highlightedBand = selectedYear === 'all'
    ? plot.yearBands.find(band => band.year === (hoveredYear || focusedYear)) : null

  const selectYear = year => {
    setSelectedYear(year)
    setHoveredYear(null)
    setFocusedYear(null)
    // The selected SVG button disappears on zoom; retain a useful keyboard focus.
    allYearsButton.current?.focus({ preventScroll: true })
  }
  const hoverYear = event => {
    if (selectedYear !== 'all' || event.pointerType === 'touch') return
    const svg = event.currentTarget
    const transform = svg.getScreenCTM()
    if (!transform) return
    const cursor = svg.createSVGPoint()
    cursor.x = event.clientX
    cursor.y = event.clientY
    const point = cursor.matrixTransform(transform.inverse())
    setHoveredYear(point.y >= PLOT_TOP && point.y <= AXIS_Y + 8
      ? yearAtX(plot.yearBands, point.x) : null)
  }

  useEffect(() => { setSalienceRange([0, 100]) }, [playerName])

  useEffect(() => {
    modeCache.current.clear()
    setActiveModes(new Set(PLAYER_ANALYSIS_MODES))
    setSelectedYear('all')
    setHoveredYear(null)
    setFocusedYear(null)
    setRefreshToken(0)
  }, [playerName])

  useEffect(() => {
    if (selectedYear !== 'all' && !years.includes(selectedYear)) setSelectedYear('all')
  }, [selectedYear, years])

  useEffect(() => {
    if (!visible) return undefined
    if (!playerName || !modeQuery) {
      setRequest({ ...emptyRequest, playerName, modeQuery })
      return undefined
    }
    const requestKey = `${playerName}:${modeQuery}`
    const cached = modeCache.current.get(requestKey)
    if (cached && cached.refreshToken === refreshToken && Date.now() - cached.savedAt < MODE_CACHE_MS) {
      setRequest({ data: cached.data, playerName, modeQuery, error: '', loading: false })
      return undefined
    }
    const controller = new AbortController()
    const timeout = window.setTimeout(() => {
      controller.abort()
      setRequest(current => ({ ...current, error: 'Daily accuracy took too long to load. Please retry.', loading: false }))
    }, 20000)
    // Preserve this player's last plot during a mode change, but never show a
    // different player's data or allow an old point to open the new filter.
    setRequest(current => ({ ...(current.playerName === playerName ? current : emptyRequest), playerName, error: '', loading: true }))
    const loadAccuracy = async () => {
      let salienceData = null
      try {
        salienceData = await fetchDailySalienceAccuracy({
          playerName,
          mode: modeQuery,
          signal: controller.signal,
          bypassCache: true,
        })
      } catch (error) {
        if (controller.signal.aborted) throw error
      }
      if (salienceData?.salience?.status === 'ready') return salienceData

      const fallback = await fetchDailyEfficiency({
        playerName,
        mode: modeQuery,
        signal: controller.signal,
        bypassCache: true,
      })
      return {
        ...fallback,
        salience: salienceData?.salience || { status: 'unavailable' },
        saliencePending: true,
      }
    }

    loadAccuracy()
      .then((data) => {
        if (!controller.signal.aborted) {
          modeCache.current.set(requestKey, { data, refreshToken, savedAt: Date.now() })
          setRequest({ data, playerName, modeQuery, error: '', loading: false })
        }
      })
      .catch((error) => {
        // Changing player/mode or closing the panel must not update the next
        // request. A real timeout, however, must end the loading state.
        if (controller.signal.aborted) return
        setRequest(current => ({ ...current, error: error.message || 'Unable to load daily accuracy.', loading: false }))
      })
      .finally(() => window.clearTimeout(timeout))
    return () => {
      window.clearTimeout(timeout)
      controller.abort()
    }
  }, [modeQuery, playerName, refreshToken, visible])

  useEffect(() => {
    const status = request.data?.salience?.status
    if (!visible || request.loading || request.error || !status || status === 'ready') return undefined
    const timer = window.setTimeout(() => setRefreshToken((value) => value + 1), 15000)
    return () => window.clearTimeout(timer)
  }, [playerName, request.data, request.loading, request.error, visible])

  const toggleMode = (mode) => {
    setActiveModes((current) => {
      const next = new Set(current)
      if (next.has(mode)) next.delete(mode)
      else next.add(mode)
      return next
    })
  }

  return (
    <section {...pageStyleTarget('panel')} className="measure-chart-section measure-daily-efficiency-section" aria-label="Daily game accuracy chart" aria-busy={request.loading}>
      <div className="behavior-subchart-heading measure-chart-heading measure-daily-efficiency-heading">
        <div><strong {...pageStyleTarget('value')}>Accuracy</strong></div>
        <div className="measure-daily-years" aria-label="Accuracy year">
          <button {...pageStyleTarget('button')}
            ref={allYearsButton}
            type="button"
            className={selectedYear === 'all' ? 'active' : ''}
            aria-pressed={selectedYear === 'all'}
            aria-label="Show all years"
            onClick={() => selectYear('all')}
          >All</button>
        </div>
        <PlayerModeToggles
          activeModes={activeModes}
          availableModes={availableModes}
          styleId="player.accuracy.modes"
          label="Game types shown in accuracy"
          onToggle={toggleMode}
        />
      </div>

      {request.loading && !data ? <div className="measure-chart-message">Calculating daily accuracy…</div> : null}
      {!request.loading && request.error ? <div className="measure-chart-message error" role="alert">
        {request.error}{' '}
        <button {...pageStyleTarget('button')} type="button" onClick={() => setRefreshToken(value => value + 1)}>Retry</button>
      </div> : null}
      {data || (!request.loading && !request.error) ? (
        <div className="daily-salience-layout">
          <div className="daily-salience-plot">
            <svg {...pageStyleTarget('chart.accuracy')} className="measure-daily-efficiency-graph" viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="group" aria-label={`Average game accuracy by local date — ${selectedYear === 'all' ? 'all years' : selectedYear}`} onPointerMove={hoverYear} onPointerLeave={() => setHoveredYear(null)}>
              <desc>{data?.saliencePending ? 'Unweighted accuracy. Salience is not available yet.' : 'Salience-weighted daily accuracy.'}</desc>
              {selectedYear === 'all' ? <DailyYearBands
                bands={plot.yearBands}
                activeYear={highlightedBand?.year}
                top={PLOT_TOP}
                bottom={AXIS_Y + 8}
                onSelect={selectYear}
                onFocusYear={setFocusedYear}
              /> : null}
              {plot.yTicks.map((tick) => (
                <g key={`daily-efficiency-y-${tick.value}`} pointerEvents="none">
                  <line className="graph-grid-line" x1={PLOT_LEFT} x2={PLOT_RIGHT} y1={tick.y} y2={tick.y} />
                  <text className="graph-rating-label" x={PLOT_LEFT - 10} y={tick.y + 3} textAnchor="end">{tick.value}</text>
                </g>
              ))}
              {visiblePoints.length ? (
                <>
                  {visiblePoints.map((point) => (
                    <circle
                      className={`measure-daily-efficiency-point ${efficiencyBand(point.gameEfficiency)}`}
                      cx={point.x}
                      cy={point.y}
                      r={2}
                      opacity={1}
                      data-date={point.date}
                      role="button"
                      tabIndex={selectionReady ? 0 : -1}
                      aria-disabled={!selectionReady}
                      aria-label={point.dailySalience === null
                        ? `${point.date}, ${point.gameEfficiency.toFixed(2)} accuracy. Explore games.`
                        : `${point.date}, ${point.gameEfficiency.toFixed(2)} salience-weighted accuracy, ${point.averageGameSalience === null ? 'average game salience unavailable' : `${point.averageGameSalience.toPrecision(4)} average game salience`}. Explore games.`}
                      onClick={() => selectionReady && onSelectPoint?.({
                        scope: 'date',
                        date: point.date,
                        label: point.date,
                        modes: selectedModes,
                        minimum_game_moves_exclusive: 10,
                      })}
                      onKeyDown={(event) => {
                        if (selectionReady && (event.key === 'Enter' || event.key === ' ')) {
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
                      <title>{point.dailySalience === null
                        ? `${point.date}\n${point.gameEfficiency.toFixed(2)} accuracy\nSalience rebuild pending`
                        : `${point.date}\n${point.gameEfficiency.toFixed(2)} salience-weighted accuracy\n${point.unweightedAccuracy.toFixed(2)} unweighted accuracy\n${point.averageGameSalience === null ? 'Average game salience unavailable' : `${point.averageGameSalience.toPrecision(4)} average game salience`}${point.relativeSalience === null ? '' : `\n${point.relativeSalience.toFixed(1)} / 100 relative average game salience`}\n${point.dailySalience.toFixed(2)} total effective games\n${point.games} eligible analyzed games`}</title>
                    </circle>
                  ))}
                </>
              ) : (
                <text className="graph-empty-label" pointerEvents="none" x={(PLOT_LEFT + PLOT_RIGHT) / 2} y={(PLOT_TOP + PLOT_BOTTOM) / 2} textAnchor="middle">{!modeQuery ? 'Select a game type.' : plot.points.length ? 'No days in this salience range.' : 'No analyzed games in this selection.'}</text>
              )}
              {plot.xTicks.map((tick, index) => (
                <text className="graph-axis-label measure-daily-x-tick" pointerEvents="none" x={tick.x} y={AXIS_Y} textAnchor={index === 0 ? 'start' : index === plot.xTicks.length - 1 ? 'end' : 'middle'} key={`daily-efficiency-x-${index}`}>{tick.label}</text>
              ))}
              {highlightedBand ? <text className="daily-year-label" pointerEvents="none" x={(highlightedBand.left + highlightedBand.right) / 2} y={PLOT_TOP + 18} textAnchor="middle" aria-hidden="true">{highlightedBand.year}</text> : null}
            </svg>
          </div>
          <DailySalienceRange range={salienceRange} scale={salienceScale} onChange={setSalienceRange} />
        </div>
      ) : null}
    </section>
  )
}
