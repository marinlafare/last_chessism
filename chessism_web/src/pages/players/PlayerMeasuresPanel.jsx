import { useEffect, useMemo, useState } from 'react'
import { formatNumber } from '../../utils/formatters'
import PlayerDailyEfficiencyChart from './PlayerDailyEfficiencyChart'
import PlayerModeToggles, { PLAYER_ANALYSIS_MODES } from './PlayerModeToggles'

const MEASURE_FIELDS = [
  'positions', 'positive_positions', 'negative_positions', 'equal_positions',
  'transitions', 'cp_gain_events', 'cp_loss_events', 'player_cp_sum',
  'total_cp_gain', 'total_cp_loss', 'own_move_cp_gain', 'own_move_cp_loss',
  'opponent_move_cp_gain', 'opponent_move_cp_loss', 'mate_for', 'mate_against',
  'tablebase_winning', 'tablebase_drawing', 'tablebase_losing',
]
const ALL_MODES = new Set(PLAYER_ANALYSIS_MODES)
const METRICS = {
  net_per_transition: { label: 'Net / player move', signed: true },
  total_cp_gain: { label: 'Total gain', signed: false },
  total_cp_loss: { label: 'Total loss', signed: false, loss: true },
  net_cp_change: { label: 'Net total', signed: true },
}
const HOURS_WIDTH = 1200
const HOURS_HEIGHT = 210
const HOURS_LEFT = 120
const HOURS_RIGHT = 1190
const HOURS_TOP = 10
const HOURS_BOTTOM = 150
const HOURS_STEP = (HOURS_RIGHT - HOURS_LEFT) / 24

const numeric = (value) => Number(value || 0)

function cp(value, digits = 1) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return 'N/A'
  const parsed = Number(value)
  return `${parsed > 0 ? '+' : ''}${parsed.toFixed(digits)} cp`
}

function finalizeMeasure(bucket) {
  const positions = numeric(bucket.positions)
  const transitions = numeric(bucket.transitions)
  const gain = transitions ? numeric(bucket.total_cp_gain) : null
  const loss = transitions ? numeric(bucket.total_cp_loss) : null
  const playerSum = positions ? numeric(bucket.player_cp_sum) : null
  const net = gain === null || loss === null ? null : gain - loss
  return {
    ...bucket,
    has_cp_data: positions > 0,
    has_cp_transitions: transitions > 0,
    player_cp_sum: playerSum,
    total_cp_gain: gain,
    total_cp_loss: loss,
    net_cp_change: net,
    net_per_transition: transitions && net !== null ? net / transitions : null,
    player_cp_average: positions && playerSum !== null ? playerSum / positions : null,
  }
}

function qualityForModes(quality, activeModes) {
  if (!quality?.by_mode) return quality || { coverage: {}, weekdays: [], hours: [], weekday_hours: [] }
  const selected = PLAYER_ANALYSIS_MODES
    .filter((mode) => activeModes.has(mode))
    .map((mode) => quality.by_mode[mode])
    .filter(Boolean)
  const combineRows = (key, identity) => {
    const maps = selected.map((mode) => new Map((mode[key] || []).map((row) => [identity(row), row])))
    return (quality[key] || []).map((template) => {
      const bucket = { ...template }
      MEASURE_FIELDS.forEach((field) => { bucket[field] = 0 })
      maps.forEach((modeMap) => {
        const row = modeMap.get(identity(template)) || {}
        MEASURE_FIELDS.forEach((field) => { bucket[field] += numeric(row[field]) })
      })
      return finalizeMeasure(bucket)
    })
  }
  const coverage = selected.reduce((total, mode) => ({
    total_games: total.total_games + numeric(mode.coverage?.total_games),
    eligible_games: total.eligible_games + numeric(mode.coverage?.eligible_games),
    excluded_games: total.excluded_games + numeric(mode.coverage?.excluded_games),
    scored_positions: total.scored_positions + numeric(mode.coverage?.scored_positions),
  }), { total_games: 0, eligible_games: 0, excluded_games: 0, scored_positions: 0 })
  return {
    coverage,
    weekdays: combineRows('weekdays', (row) => numeric(row.weekday)),
    hours: combineRows('hours', (row) => numeric(row.hour)),
    weekday_hours: combineRows('weekday_hours', (row) => `${numeric(row.weekday)}-${numeric(row.hour)}`),
  }
}

function metricValue(row, metric) {
  const value = row?.[metric]
  return value === null || value === undefined || !Number.isFinite(Number(value)) ? null : Number(value)
}

function MetricSelector({ value, onChange, label }) {
  return (
    <label className="measure-metric-selector">
      <span>{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        {Object.entries(METRICS).map(([key, metric]) => (
          <option value={key} key={key}>{metric.label}</option>
        ))}
      </select>
    </label>
  )
}

function measureColor(value, maximum, metric) {
  if (value === null) return '#172029'
  const strength = Math.min(1, Math.abs(value) / Math.max(maximum, 1))
  const color = METRICS[metric].loss || value < 0 ? '213, 92, 92' : '21, 199, 128'
  return `rgba(${color}, ${0.2 + strength * 0.8})`
}

function MeasureDaysChart({ quality, activeModes, availableModes, onToggleMode }) {
  const [metric, setMetric] = useState('net_per_transition')
  const [expanded, setExpanded] = useState(() => new Set())
  const cellsByWeekday = useMemo(() => {
    const grouped = new Map()
    ;(quality.weekday_hours || []).forEach((cell) => {
      const current = grouped.get(cell.weekday) || []
      current.push(cell)
      grouped.set(cell.weekday, current)
    })
    return grouped
  }, [quality.weekday_hours])
  const allValues = [
    ...(quality.weekdays || []).map((row) => metricValue(row, metric)),
    ...(quality.weekday_hours || []).map((row) => metricValue(row, metric)),
  ].filter((value) => value !== null)
  const maximum = Math.max(1, ...allValues.map(Math.abs))
  const toggleDay = (weekday) => {
    setExpanded((current) => {
      const next = new Set(current)
      if (next.has(weekday)) next.delete(weekday)
      else next.add(weekday)
      return next
    })
  }

  return (
    <section className="measure-chart-section">
      <div className="behavior-subchart-heading measure-chart-heading">
        <div><strong>Days</strong></div>
        <MetricSelector value={metric} onChange={setMetric} label="Day metric" />
        <PlayerModeToggles
          activeModes={activeModes}
          availableModes={availableModes}
          label="Game types shown in Stockfish Days"
          onToggle={onToggleMode}
        />
      </div>
      <div className="measure-day-rows">
        {(quality.weekdays || []).map((day) => {
          const isExpanded = expanded.has(day.weekday)
          const dayValue = metricValue(day, metric)
          const hourMap = new Map((cellsByWeekday.get(day.weekday) || []).map((cell) => [cell.hour, cell]))
          return (
            <button
              className={`measure-day-row ${isExpanded ? 'expanded' : ''}`}
              type="button"
              aria-expanded={isExpanded}
              onClick={() => toggleDay(day.weekday)}
              key={day.weekday}
            >
              <span className="measure-day-name">{String(day.label).toUpperCase()}</span>
              <span className="measure-day-stage">
                <span className="measure-day-summary" title={`${day.label}\n${METRICS[metric].label}: ${cp(dayValue)}\n${formatNumber(day.positions)} scored positions`}>
                  <i style={{ background: measureColor(dayValue, maximum, metric) }} />
                  <strong>{cp(dayValue)}</strong>
                </span>
                <span className="measure-day-hours">
                  {Array.from({ length: 24 }, (_, hour) => {
                    const cell = hourMap.get(hour) || {}
                    const value = metricValue(cell, metric)
                    return (
                      <i
                        style={{ background: measureColor(value, maximum, metric) }}
                        title={`${String(hour).padStart(2, '0')}:00\n${METRICS[metric].label}: ${cp(value)}\n${formatNumber(cell.positions || 0)} scored positions`}
                        key={hour}
                      />
                    )
                  })}
                </span>
              </span>
            </button>
          )
        })}
      </div>
      <div className="measure-day-axis" aria-hidden="true">
        {Array.from({ length: 24 }, (_, hour) => <span key={hour}>{String(hour).padStart(2, '0')}</span>)}
        <strong>HRS</strong>
      </div>
    </section>
  )
}

function MeasureHoursChart({ quality, activeModes, availableModes, onToggleMode }) {
  const [metric, setMetric] = useState('net_per_transition')
  const rows = quality.hours || []
  const values = rows.map((row) => metricValue(row, metric))
  const maximum = Math.max(1, ...values.filter((value) => value !== null).map(Math.abs))
  const signed = METRICS[metric].signed
  const zeroY = signed ? (HOURS_TOP + HOURS_BOTTOM) / 2 : HOURS_BOTTOM
  const rangeHeight = signed ? (HOURS_BOTTOM - HOURS_TOP) / 2 : HOURS_BOTTOM - HOURS_TOP

  return (
    <section className="measure-chart-section measure-hours-section">
      <div className="behavior-subchart-heading measure-chart-heading">
        <div><strong>Hours</strong></div>
        <MetricSelector value={metric} onChange={setMetric} label="Hour metric" />
        <PlayerModeToggles
          activeModes={activeModes}
          availableModes={availableModes}
          label="Game types shown in Stockfish Hours"
          onToggle={onToggleMode}
        />
      </div>
      <svg className="measure-hours-graph" viewBox={`0 0 ${HOURS_WIDTH} ${HOURS_HEIGHT}`} role="img" aria-label={`${METRICS[metric].label} by hour`}>
        <line className="graph-grid-line" x1={HOURS_LEFT} x2={HOURS_RIGHT} y1={HOURS_TOP} y2={HOURS_TOP} />
        <line className="measure-zero-line" x1={HOURS_LEFT} x2={HOURS_RIGHT} y1={zeroY} y2={zeroY} />
        <line className="graph-grid-line" x1={HOURS_LEFT} x2={HOURS_RIGHT} y1={HOURS_BOTTOM} y2={HOURS_BOTTOM} />
        <text className="graph-rating-label" x={HOURS_LEFT - 10} y={HOURS_TOP + 3} textAnchor="end">{cp(maximum)}</text>
        <text className="graph-rating-label" x={HOURS_LEFT - 10} y={zeroY + 3} textAnchor="end">0</text>
        {signed ? <text className="graph-rating-label" x={HOURS_LEFT - 10} y={HOURS_BOTTOM + 3} textAnchor="end">{cp(-maximum)}</text> : null}
        {Array.from({ length: 24 }, (_, hour) => {
          const row = rows.find((item) => numeric(item.hour) === hour) || {}
          const value = metricValue(row, metric)
          const barHeight = value === null ? 0 : (Math.abs(value) / maximum) * rangeHeight
          const x = HOURS_LEFT + hour * HOURS_STEP + HOURS_STEP * 0.15
          const y = signed && value < 0 ? zeroY : zeroY - barHeight
          return (
            <g key={hour}>
              <rect
                className={`measure-hour-bar ${(value !== null && value < 0) || METRICS[metric].loss ? 'negative' : ''}`}
                x={x}
                y={y}
                width={HOURS_STEP * 0.7}
                height={barHeight}
              >
                <title>{`${String(hour).padStart(2, '0')}:00\n${METRICS[metric].label}: ${cp(value)}\n${formatNumber(row.positions || 0)} scored positions`}</title>
              </rect>
              <text className="hourly-axis-tick" x={x + HOURS_STEP * 0.35} y="174" textAnchor="middle">{String(hour).padStart(2, '0')}</text>
            </g>
          )
        })}
        <text className="hourly-axis-title" x={(HOURS_LEFT + HOURS_RIGHT) / 2} y="204" textAnchor="middle">HRS</text>
      </svg>
    </section>
  )
}

function GameSequenceChart({ positions = [] }) {
  const points = positions.filter((position) => position.score_kind === 'cp')
  if (points.length < 2) return null
  const width = 1200
  const height = 180
  const left = 90
  const right = 1180
  const top = 12
  const bottom = 150
  const scores = points.map((point) => numeric(point.player_score))
  const minimum = Math.min(...scores)
  const maximum = Math.max(...scores)
  const spread = Math.max(1, maximum - minimum)
  const plotted = points.map((point, index) => ({
    ...point,
    x: left + (index / Math.max(1, points.length - 1)) * (right - left),
    y: bottom - ((numeric(point.player_score) - minimum) / spread) * (bottom - top),
  }))
  return (
    <div className="measure-game-sequence-scroll">
      <svg className="measure-game-sequence" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Player-oriented evaluation through the game">
        <line className="graph-grid-line" x1={left} x2={right} y1={top} y2={top} />
        <line className="graph-grid-line" x1={left} x2={right} y1={bottom} y2={bottom} />
        <text className="graph-rating-label" x={left - 10} y={top + 3} textAnchor="end">{cp(maximum, 0)}</text>
        <text className="graph-rating-label" x={left - 10} y={bottom + 3} textAnchor="end">{cp(minimum, 0)}</text>
        <polyline className="measure-game-line" points={plotted.map((point) => `${point.x},${point.y}`).join(' ')} />
        {plotted.map((point) => (
          <circle className="measure-game-point" cx={point.x} cy={point.y} r="3" key={point.ply}>
            <title>{`Ply ${point.ply} · ${point.move || 'move'}\n${cp(point.player_score)}\nchange ${cp(point.cp_change)}`}</title>
          </circle>
        ))}
        <text className="graph-axis-label" x={left} y="172">First scored ply</text>
        <text className="graph-axis-label" x={right} y="172" textAnchor="end">Last scored ply</text>
      </svg>
    </div>
  )
}

function positionScore(position) {
  if (position.score_kind === 'cp') return cp(position.player_score)
  if (position.score_kind === 'mate') return position.player_score > 0 ? 'Mate for player' : 'Mate against player'
  if (position.score_kind === 'tablebase') {
    if (position.player_score > 0) return 'Tablebase win'
    if (position.player_score < 0) return 'Tablebase loss'
    return 'Tablebase draw'
  }
  return 'N/A'
}

function MeasuresInspector({ gameInspector, hourInspector }) {
  const games = hourInspector.data?.games || []
  const game = gameInspector.data?.game
  return (
    <section className="measure-inspector">
      <div className="behavior-subchart-heading measure-inspector-heading">
        <div><strong>Explore</strong></div>
        <small>Load a local hour, then select a game for its complete score sequence.</small>
      </div>
      <div className="measure-inspector-controls">
        <label><span>Date</span><input type="date" value={hourInspector.date} onChange={(event) => hourInspector.setDate(event.target.value)} /></label>
        <label><span>Hour</span><select value={hourInspector.hour} onChange={(event) => hourInspector.setHour(event.target.value)}>{Array.from({ length: 24 }, (_, hour) => <option value={hour} key={hour}>{String(hour).padStart(2, '0')}:00</option>)}</select></label>
        <label><span>Game type</span><select value={hourInspector.mode} onChange={(event) => hourInspector.setMode(event.target.value)}><option value="all">All</option>{PLAYER_ANALYSIS_MODES.map((mode) => <option value={mode} key={mode}>{mode}</option>)}</select></label>
        <button type="button" disabled={!hourInspector.date || hourInspector.loading} onClick={hourInspector.load}>{hourInspector.loading ? 'Loading…' : 'Inspect hour'}</button>
        <label className="measure-game-id"><span>Game ID</span><input inputMode="numeric" value={gameInspector.gameId} onChange={(event) => gameInspector.setGameId(event.target.value)} /></label>
        <button type="button" disabled={!gameInspector.gameId || gameInspector.loading} onClick={gameInspector.load}>{gameInspector.loading ? 'Loading…' : 'Inspect game'}</button>
      </div>
      {hourInspector.error ? <p className="player-analysis-error">{hourInspector.error}</p> : null}
      {gameInspector.error ? <p className="player-analysis-error">{gameInspector.error}</p> : null}
      {hourInspector.data ? (
        <div className="measure-hour-results">
          <div className="measure-hour-result-heading">
            <strong>{hourInspector.data.date} · {String(hourInspector.data.hour).padStart(2, '0')}:00</strong>
            <span>{formatNumber(games.length)} loaded games</span>
          </div>
          {games.length ? (
            <div className="measure-game-list">
              {games.map((item) => (
                <button type="button" className={game?.game_id === item.game_id ? 'active' : ''} onClick={() => gameInspector.selectGame(item.game_id)} key={item.game_id}>
                  <strong>{item.game_id}</strong>
                  <span>{item.mode} · {item.color} · vs {item.opponent}</span>
                  <small>{formatNumber(item.positions.length)} scored positions</small>
                </button>
              ))}
              {hourInspector.data.pagination?.has_more ? <button className="measure-load-more" type="button" disabled={hourInspector.loading} onClick={hourInspector.loadMore}>Load more</button> : null}
            </div>
          ) : <p className="measure-empty">No games started during this hour.</p>}
        </div>
      ) : null}
      {game ? (
        <div className="measure-game-detail">
          <div className="measure-game-meta">
            <strong>Game {game.game_id}</strong>
            <span>{game.mode}</span><span>{game.color}</span><span>vs {game.opponent}</span>
            <span>{formatNumber(game.analyzed_positions)} / {formatNumber(game.total_positions)} positions</span>
          </div>
          <GameSequenceChart positions={game.positions} />
          <div className="measure-position-table-wrap">
            <table>
              <thead><tr><th>Ply</th><th>Move</th><th>Player score</th><th>Change</th><th>Gain</th><th>Loss</th><th>Source</th></tr></thead>
              <tbody>
                {game.positions.map((position) => (
                  <tr className={position.mover_is_player ? 'player-move' : ''} key={position.ply}>
                    <td>{position.ply}</td><td>{position.move || '—'}</td><td>{positionScore(position)}</td>
                    <td>{cp(position.cp_change)}</td><td>{cp(position.cp_gain)}</td><td>{cp(position.cp_loss)}</td>
                    <td>{position.analysis_source || 'stockfish'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ) : null}
    </section>
  )
}

export default function PlayerMeasuresPanel({ playerName, quality, gameInspector, hourInspector }) {
  const [dayModes, setDayModes] = useState(() => new Set(PLAYER_ANALYSIS_MODES))
  const [hourModes, setHourModes] = useState(() => new Set(PLAYER_ANALYSIS_MODES))
  const aggregate = useMemo(() => qualityForModes(quality, ALL_MODES), [quality])
  const dayQuality = useMemo(() => qualityForModes(quality, dayModes), [quality, dayModes])
  const hourQuality = useMemo(() => qualityForModes(quality, hourModes), [quality, hourModes])
  const availableModes = useMemo(() => new Set(PLAYER_ANALYSIS_MODES.filter((mode) => (
    !quality.by_mode || numeric(quality.by_mode[mode]?.coverage?.eligible_games) > 0
  ))), [quality])
  const coverage = aggregate.coverage || {}
  const coveragePercent = numeric(coverage.total_games)
    ? (numeric(coverage.eligible_games) / numeric(coverage.total_games)) * 100
    : 0
  const toggleMode = (setter, mode) => setter((current) => {
    const next = new Set(current)
    if (next.has(mode)) next.delete(mode)
    else next.add(mode)
    return next
  })

  useEffect(() => {
    setDayModes(new Set(PLAYER_ANALYSIS_MODES))
    setHourModes(new Set(PLAYER_ANALYSIS_MODES))
  }, [quality])

  return (
    <div className="measure-dashboard">
      <div className="measure-coverage" aria-label="Stockfish analysis coverage">
        <div><span>Total games</span><strong>{formatNumber(coverage.total_games || 0)}</strong></div>
        <div><span>Complete games</span><strong>{formatNumber(coverage.eligible_games || 0)}</strong><small>{coveragePercent.toFixed(1)}%</small></div>
        <div><span>Excluded games</span><strong>{formatNumber(coverage.excluded_games || 0)}</strong></div>
        <div><span>Scored positions</span><strong>{formatNumber(coverage.scored_positions || 0)}</strong></div>
      </div>
      <article className="measure-chart-card">
        <PlayerDailyEfficiencyChart
          availableModes={availableModes}
          playerName={playerName}
        />
        <MeasureDaysChart
          quality={dayQuality}
          activeModes={dayModes}
          availableModes={availableModes}
          onToggleMode={(mode) => toggleMode(setDayModes, mode)}
        />
        <MeasureHoursChart
          quality={hourQuality}
          activeModes={hourModes}
          availableModes={availableModes}
          onToggleMode={(mode) => toggleMode(setHourModes, mode)}
        />
        <MeasuresInspector gameInspector={gameInspector} hourInspector={hourInspector} />
      </article>
    </div>
  )
}
