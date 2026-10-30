import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useMemo, useRef, useState } from 'react'
import { fetchDailyAccuracyBands, fetchDailyEfficiency, fetchDailySalienceAccuracy } from './playerAnalysisApi'
import { DAILY_ACCURACY_LABELS, accuracyBandPoints, accuracyBandNote } from './dailyAccuracyBands'
import PlayerModeToggles, { PLAYER_ANALYSIS_MODES } from './PlayerModeToggles'
import DailySalienceRange from './DailySalienceRange'
import { attachSalienceBands, filterSalienceBands, salienceBandNote } from './dailySalienceBands'
import DailyDateBands from './DailyDateBands'
import { buildDateScale, periodAtX, dateInPeriod, datePeriodLabel, periodTicks } from './dailyYearNavigation'
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

function buildPlot(points, modal, period) {
  const scale = modal ? { minimum: 0, maximum: 5, step: 1 } : efficiencyScale(points)
  const valueRange = Math.max(1, scale.maximum - scale.minimum)
  const yTicks = []
  for (let value = scale.maximum; value >= scale.minimum - Number.EPSILON; value -= scale.step) {
    const normalizedValue = Number(value.toFixed(scale.step < 1 ? 1 : 0))
    yTicks.push({
      value: normalizedValue,
      label: modal ? DAILY_ACCURACY_LABELS[normalizedValue] : normalizedValue,
      y: PLOT_TOP + ((scale.maximum - value) / valueRange) * (PLOT_BOTTOM - PLOT_TOP),
    })
  }
  if (!points.length) return { points: [], xTicks: [], yTicks, yearBands: [] }

  const { start, end, bands: yearBands } = buildDateScale(points, PLOT_LEFT, PLOT_RIGHT, period)
  const timeSpread = Math.max(1, end - start)
  const plotted = points.map((point) => ({
    ...point,
    x: PLOT_LEFT + ((point.timestamp - start) / timeSpread) * (PLOT_RIGHT - PLOT_LEFT),
    y: PLOT_BOTTOM
      - ((point.gameEfficiency - scale.minimum) / valueRange) * (PLOT_BOTTOM - PLOT_TOP),
  }))
  const xTicks = period !== 'all' ? periodTicks(start, end, PLOT_LEFT, PLOT_RIGHT, period) : Array.from({ length: 7 }, (_, index) => {
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
  const [selectedPeriod, setSelectedPeriod] = useState('all')
  const [hoveredYear, setHoveredYear] = useState(null)
  const [focusedYear, setFocusedYear] = useState(null)
  const allYearsButton = useRef(null)
  const yearButton = useRef(null)
  const [request, setRequest] = useState(emptyRequest)
  const modeCache = useRef(new Map())
  const [refreshToken, setRefreshToken] = useState(0)
  const [salienceRange, setSalienceRange] = useState([0, 100])
  const selectedModes = useMemo(() => PLAYER_ANALYSIS_MODES.filter(
    (mode) => activeModes.has(mode)
  ), [activeModes])
  const modeQuery = selectedModes.join(',')
  const data = request.playerName === playerName ? request.data : null
  const modal = data?.metric === 'modal_accuracy_band'
  const selectionReady = Boolean(data && request.modeQuery === modeQuery && !request.loading && !request.error)
  const dates = useMemo(() => (data?.points || []).map(point => String(point[0])), [data])
  const monthView = selectedPeriod.length === 7
  const pointsSelectable = monthView && selectionReady
  const salienceReady = data?.salience?.status === 'ready'
  const fixedSalience = data?.salience?.metric === 'modal_salience_band'
  const salienceScale = useMemo(
    () => {
      const points = modal ? accuracyBandPoints(data?.points, salienceReady) : efficiencyPoints(data?.points, salienceReady)
      return fixedSalience ? { points: attachSalienceBands(points, data.salience), enabled: salienceReady }
        : relativeDailySalience(points)
    },
    [data, salienceReady, modal, fixedSalience]
  )
  const dailyPoints = useMemo(
    () => salienceScale.points.filter((point) => dateInPeriod(point.date, selectedPeriod)),
    [salienceScale, selectedPeriod]
  )
  // Plot before filtering so both axes stay fixed when a range is selected.
  const plot = useMemo(() => buildPlot(dailyPoints, modal, selectedPeriod), [dailyPoints, modal, selectedPeriod])
  const visiblePoints = useMemo(() => fixedSalience
    ? filterSalienceBands(plot.points, salienceRange, salienceScale.enabled)
    : filterDailySalience(plot.points, salienceRange, salienceScale.enabled),
    [plot, fixedSalience, salienceRange, salienceScale.enabled])
  const pointNote = point => {
    if (modal) return accuracyBandNote(point) + (fixedSalience ? salienceBandNote(point) : '')
    if (fixedSalience) return `${point.date}\n${point.gameEfficiency.toFixed(2)} accuracy` + salienceBandNote(point)
    if (point.dailySalience === null) return `${point.date}\n${point.gameEfficiency.toFixed(2)} accuracy\nSalience rebuild pending`
    return `${point.date}\n${point.gameEfficiency.toFixed(2)} salience-weighted accuracy\n${point.unweightedAccuracy.toFixed(2)} unweighted accuracy\n${point.averageGameSalience === null ? 'Average game salience unavailable' : `${point.averageGameSalience.toPrecision(4)} average game salience`}${point.relativeSalience === null ? '' : `\n${point.relativeSalience.toFixed(1)} / 100 relative average game salience`}\n${point.dailySalience.toFixed(2)} total effective games\n${point.games} eligible analyzed games`
  }
  const highlightedBand = plot.yearBands.find(band => band.key === (hoveredYear || focusedYear))

  const selectPeriod = period => {
    setSelectedPeriod(period)
    setHoveredYear(null)
    setFocusedYear(null)
    // The selected SVG button disappears on zoom; retain a useful keyboard focus.
    const focusTarget = period.length === 7 ? yearButton : allYearsButton
    focusTarget.current?.focus({ preventScroll: true })
  }
  const hoverYear = event => {
    if (monthView || event.pointerType === 'touch') return
    const svg = event.currentTarget
    const transform = svg.getScreenCTM()
    if (!transform) return
    const cursor = svg.createSVGPoint()
    cursor.x = event.clientX
    cursor.y = event.clientY
    const point = cursor.matrixTransform(transform.inverse())
    setHoveredYear(point.y >= PLOT_TOP && point.y <= AXIS_Y + 8
      ? periodAtX(plot.yearBands, point.x) : null)
  }

  useEffect(() => { setSalienceRange([0, 100]) }, [playerName, fixedSalience])

  useEffect(() => {
    modeCache.current.clear()
    setActiveModes(new Set(PLAYER_ANALYSIS_MODES))
    setSelectedPeriod('all')
    setHoveredYear(null)
    setFocusedYear(null)
    setRefreshToken(0)
  }, [playerName])

  useEffect(() => {
    if (selectedPeriod !== 'all' && !dates.some(date => dateInPeriod(date, selectedPeriod))) {
      const year = selectedPeriod.slice(0, 4)
      setSelectedPeriod(dates.some(date => dateInPeriod(date, year)) ? year : 'all')
    }
  }, [selectedPeriod, dates])

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
      let fixedBands = null
      try {
        const bands = await fetchDailyAccuracyBands({ playerName, mode: modeQuery, signal: controller.signal, bypassCache: true })
        if (bands.salience?.metric === 'modal_salience_band') fixedBands = bands.salience
        if (bands.metric === 'modal_accuracy_band' && !bands.summary_pending) return bands
      } catch (error) {
        if (controller.signal.aborted) throw error
      }
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
      if (salienceData?.salience?.status === 'ready') return { ...salienceData,
        salience: fixedBands || salienceData.salience, accuracyMigrationPending: true }

      const fallback = await fetchDailyEfficiency({
        playerName,
        mode: modeQuery,
        signal: controller.signal,
        bypassCache: true,
      })
      return {
        ...fallback,
        salience: fixedBands || salienceData?.salience || { status: 'unavailable' },
        saliencePending: true,
        accuracyMigrationPending: true,
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
    if (!visible || modal || request.loading || request.error || !status || status === 'ready') return undefined
    const timer = window.setTimeout(() => setRefreshToken((value) => value + 1), 15000)
    return () => window.clearTimeout(timer)
  }, [playerName, request.data, request.loading, request.error, visible, modal])

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
        <div><strong {...pageStyleTarget('player.accuracy.daily.title')}>{modal ? 'Most common accuracy band' : 'Accuracy'}</strong></div>
        <div className="measure-daily-years" aria-label="Accuracy date range">
          <button {...pageStyleTarget('button')}
            ref={allYearsButton}
            type="button"
            className={selectedPeriod === 'all' ? 'active' : ''}
            aria-pressed={selectedPeriod === 'all'}
            aria-label="Show all years"
            onClick={() => selectPeriod('all')}
          >All</button>
          {selectedPeriod !== 'all' && <button {...pageStyleTarget('button')} ref={yearButton} type="button"
            className={!monthView ? 'active' : ''} aria-pressed={!monthView}
            aria-label={`Show all of ${selectedPeriod.slice(0, 4)}`} onClick={() => selectPeriod(selectedPeriod.slice(0, 4))}
          >{selectedPeriod.slice(0, 4)}</button>}
          {monthView && <span {...pageStyleTarget('label')} className="daily-period-current" aria-current="date">{datePeriodLabel(selectedPeriod).split(' ')[0]}</span>}
        </div>
        <PlayerModeToggles
          activeModes={activeModes}
          availableModes={availableModes}
          styleId="player.accuracy.modes"
          label="Game types shown in accuracy"
          onToggle={toggleMode}
        />
      </div>

      {request.loading && !data ? <div className="measure-chart-message">Loading daily accuracy…</div> : null}
      {data?.accuracyMigrationPending ? <p {...pageStyleTarget('player.accuracy.daily.status')}>Daily bands await a manual CUDA run. Showing the previous accuracy chart.</p> : null}
      {modal && data?.stale ? <p {...pageStyleTarget('player.accuracy.daily.status')}>Showing the last complete summary; updated data awaits a manual CUDA run.</p> : null}
      {!request.loading && request.error ? <div className="measure-chart-message error" role="alert">
        {request.error}{' '}
        <button {...pageStyleTarget('button')} type="button" onClick={() => setRefreshToken(value => value + 1)}>Retry</button>
      </div> : null}
      {data || (!request.loading && !request.error) ? (
        <div className="daily-salience-layout">
          <div className="daily-salience-plot">
            <svg {...pageStyleTarget('chart.accuracy')} className="measure-daily-efficiency-graph" data-date-zoom={monthView ? 'month' : selectedPeriod === 'all' ? 'all' : 'year'} viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="group" aria-label={`${modal ? 'Most common game accuracy band' : 'Average game accuracy'} by local date — ${datePeriodLabel(selectedPeriod)}`} onPointerMove={hoverYear} onPointerLeave={() => setHoveredYear(null)}>
              <desc>{modal ? 'Daily game counts in six bands. All tied most common bands are shown. Boundaries are 0 to 50 inclusive, then above 50 to 60 inclusive, and similarly through 100.' : data?.saliencePending ? 'Unweighted accuracy. Salience is not available yet.' : 'Salience-weighted daily accuracy.'}</desc>
              {!monthView ? <DailyDateBands
                bands={plot.yearBands}
                activePeriod={highlightedBand?.key}
                top={PLOT_TOP}
                bottom={AXIS_Y + 8}
                onSelect={selectPeriod}
                onFocusPeriod={setFocusedYear}
              /> : null}
              {plot.yTicks.map((tick) => (
                <g key={`daily-efficiency-y-${tick.value}`} pointerEvents="none">
                  <line className="graph-grid-line" x1={PLOT_LEFT} x2={PLOT_RIGHT} y1={tick.y} y2={tick.y} />
                  <text className="graph-rating-label" x={PLOT_LEFT - 10} y={tick.y + 3} textAnchor="end">{tick.label}</text>
                </g>
              ))}
              {visiblePoints.length ? (
                <>
                  {visiblePoints.map((point) => (
                    <circle
                      {...(modal ? pageStyleTarget(`player.accuracy.daily.band.${point.band}`) : {})}
                      className={`measure-daily-efficiency-point ${modal ? `accuracy-band-${point.band}` : efficiencyBand(point.gameEfficiency)}`}
                      cx={point.x}
                      cy={point.y}
                      r={monthView ? 4 : 2}
                      opacity={1}
                      data-date={point.date}
                      data-band={point.band}
                      data-tied={modal ? point.modalBands.length > 1 : undefined}
                      role={monthView ? 'button' : 'img'}
                      tabIndex={pointsSelectable ? 0 : undefined}
                      aria-disabled={monthView ? !pointsSelectable : undefined}
                      aria-label={pointNote(point) + (monthView ? '\nExplore games.' : '')}
                      onClick={() => pointsSelectable && onSelectPoint?.({
                        scope: 'date',
                        date: point.date,
                        label: point.date,
                        modes: selectedModes,
                        minimum_game_moves_exclusive: 10,
                      })}
                      onKeyDown={(event) => {
                        if (pointsSelectable && (event.key === 'Enter' || event.key === ' ')) {
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
                      key={modal ? `${point.date}:${point.band}` : point.date}
                    >
                      <title>{pointNote(point)}</title>
                    </circle>
                  ))}
                </>
              ) : (
                <text className="graph-empty-label" pointerEvents="none" x={(PLOT_LEFT + PLOT_RIGHT) / 2} y={(PLOT_TOP + PLOT_BOTTOM) / 2} textAnchor="middle">{!modeQuery ? 'Select a game type.' : plot.points.length ? 'No days in this salience range.' : 'No analyzed games in this selection.'}</text>
              )}
              {plot.xTicks.map((tick, index) => (
                <text className="graph-axis-label measure-daily-x-tick" pointerEvents="none" x={tick.x} y={AXIS_Y} textAnchor={selectedPeriod !== 'all' ? 'middle' : index === 0 ? 'start' : index === plot.xTicks.length - 1 ? 'end' : 'middle'} key={`daily-efficiency-x-${index}`}>{tick.label}</text>
              ))}
              {highlightedBand ? <text className="daily-year-label" pointerEvents="none" x={(highlightedBand.left + highlightedBand.right) / 2} y={PLOT_TOP + 18} textAnchor="middle" aria-hidden="true">{highlightedBand.label}</text> : null}
            </svg>
          </div>
          <DailySalienceRange range={salienceRange} banded={fixedSalience} onChange={setSalienceRange}
            scale={{ ...salienceScale, enabled: salienceScale.enabled && selectionReady }}
            unavailableMessage={fixedSalience && !salienceReady
              ? data.salience.status === 'unavailable_timezone' ? 'Unavailable for this timezone' : 'Awaiting CUDA run'
              : ''} />
        </div>
      ) : null}
    </section>
  )
}
