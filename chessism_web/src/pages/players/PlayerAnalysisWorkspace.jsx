import { memo, useEffect, useMemo, useState } from 'react'
import { formatNumber } from '../../utils/formatters'
import { fetchPlayerAnalysis } from './playerAnalysisService'

const MODES = ['all', 'bullet', 'blitz', 'rapid']
const TABS = [
  { key: 'engine', label: 'Engine Insights', note: 'Stockfish and tablebase evidence' },
  { key: 'patterns', label: 'Playing Patterns', note: 'Games, clocks, ratings and openings' }
]
const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
const TIME_BANDS = ['00–06', '06–12', '12–18', '18–24']

const number = (value) => Number(value || 0)
const percentage = (value, digits = 1) => `${number(value).toFixed(digits)}%`
const boundedWidth = (value, maximum) => `${Math.min(100, Math.max(0, maximum ? (number(value) / maximum) * 100 : 0))}%`

function EmptyChart({ children }) {
  return <p className="player-chart-empty">{children}</p>
}

function ChartCard({ title, subtitle, metric, children, className = '' }) {
  return (
    <article className={`player-chart-card ${className}`}>
      <header className="player-chart-head">
        <div>
          <h3>{title}</h3>
          <p>{subtitle}</p>
        </div>
        {metric ? <span>{metric}</span> : null}
      </header>
      <div className="player-chart-body">{children}</div>
    </article>
  )
}

const PhaseQualityChart = memo(function PhaseQualityChart({ rows = [] }) {
  const maximumLoss = Math.max(1, ...rows.map((row) => number(row.average_cp_loss)))
  if (!rows.length) return <EmptyChart>No complete scored games match this filter.</EmptyChart>

  return (
    <div className="player-phase-chart">
      <div className="player-chart-legend"><span className="loss">Average loss</span><span className="errors">Blunders</span></div>
      {rows.map((row) => (
        <div className="player-phase-row" key={row.phase}>
          <div><strong>{row.phase}</strong><small>{formatNumber(row.moves)} moves</small></div>
          <div className="player-phase-bars">
            <div className="player-horizontal-track"><span className="loss" style={{ width: boundedWidth(row.average_cp_loss, maximumLoss) }} /></div>
            <div className="player-horizontal-track"><span className="errors" style={{ width: boundedWidth(row.blunder_rate, 15) }} /></div>
          </div>
          <div className="player-phase-values">
            <strong>{number(row.average_cp_loss).toFixed(1)} cp</strong>
            <small>{percentage(row.blunder_rate)} blunders · ±{number(row.margin_of_error).toFixed(1)}</small>
          </div>
        </div>
      ))}
    </div>
  )
})

const ConversionChart = memo(function ConversionChart({ rows = [], resilience = {} }) {
  if (!rows.length) return <EmptyChart>No games reached a measurable advantage in this selection.</EmptyChart>
  return (
    <div className="player-conversion-chart">
      {rows.map((row) => {
        const games = Math.max(1, number(row.games))
        return (
          <div className="player-conversion-row" key={row.advantage}>
            <div><strong>{row.advantage}</strong><small>{formatNumber(row.games)} games</small></div>
            <div className="player-stacked-track" aria-label={`${row.advantage}: ${percentage(row.conversion_rate)} converted`}>
              <span className="wins" style={{ width: `${(number(row.wins) / games) * 100}%` }} />
              <span className="draws" style={{ width: `${(number(row.draws) / games) * 100}%` }} />
              <span className="losses" style={{ width: `${(number(row.losses) / games) * 100}%` }} />
            </div>
            <strong>{percentage(row.conversion_rate)}</strong>
          </div>
        )
      })}
      <div className="player-chart-callouts">
        <div><span>Winning chances missed</span><strong>{formatNumber(resilience.thrown_games || 0)} / {formatNumber(resilience.winning_opportunities || 0)}</strong></div>
        <div><span>Lost positions saved</span><strong>{formatNumber(resilience.comebacks || 0)} / {formatNumber(resilience.losing_positions || 0)}</strong></div>
      </div>
    </div>
  )
})

const TimePressureChart = memo(function TimePressureChart({ rows = [] }) {
  const maximumLoss = Math.max(1, ...rows.map((row) => number(row.average_cp_loss)))
  if (!rows.length) return <EmptyChart>No scored moves with clock annotations match this filter.</EmptyChart>
  return (
    <div className="player-pressure-chart">
      {rows.map((row) => (
        <div className="player-pressure-row" key={row.bucket}>
          <strong>{row.bucket}</strong>
          <div className="player-horizontal-track"><span style={{ width: boundedWidth(row.average_cp_loss, maximumLoss) }} /></div>
          <div><strong>{number(row.average_cp_loss).toFixed(1)} cp</strong><small>{percentage(row.blunder_rate)} blunders</small></div>
        </div>
      ))}
    </div>
  )
})

const EndgameChart = memo(function EndgameChart({ rows = [] }) {
  if (!rows.length) return <EmptyChart>No consecutive tablebase-scored player moves match this filter.</EmptyChart>
  return (
    <div className="player-endgame-chart">
      {rows.map((row) => (
        <div className="player-endgame-row" key={row.piece_count}>
          <div><strong>{row.piece_count} pieces</strong><small>{formatNumber(row.moves)} moves</small></div>
          <div className="player-horizontal-track"><span style={{ width: boundedWidth(row.precision_rate, 100) }} /></div>
          <strong>{percentage(row.precision_rate)}</strong>
        </div>
      ))}
    </div>
  )
})

function linePath(values, valueAccessor, width = 620, height = 174) {
  if (!values.length) return ''
  const points = values.map((item) => number(valueAccessor(item)))
  const minimum = Math.min(...points)
  const maximum = Math.max(...points)
  const spread = Math.max(1, maximum - minimum)
  return values.map((item, index) => {
    const x = values.length === 1 ? width / 2 : (index / (values.length - 1)) * width
    const y = height - ((number(valueAccessor(item)) - minimum) / spread) * height
    return `${index ? 'L' : 'M'} ${x.toFixed(1)} ${y.toFixed(1)}`
  }).join(' ')
}

const JourneyChart = memo(function JourneyChart({ rows = [] }) {
  const ratingPath = useMemo(() => linePath(rows, (row) => row.average_rating), [rows])
  const scorePath = useMemo(() => linePath(rows, (row) => row.score_rate), [rows])
  if (!rows.length) return <EmptyChart>No dated games match this filter.</EmptyChart>
  const first = rows[0]
  const last = rows[rows.length - 1]
  return (
    <div className="player-line-chart">
      <div className="player-chart-legend"><span className="rating">Average rating</span><span className="score">Score rate</span></div>
      <svg viewBox="0 0 620 180" role="img" aria-label="Monthly average rating and score-rate trends" preserveAspectRatio="none">
        <path className="chart-grid" d="M0 45 H620 M0 90 H620 M0 135 H620" />
        <path className="rating" d={ratingPath} />
        <path className="score" d={scorePath} />
      </svg>
      <div className="player-line-axis"><span>{String(first.month).slice(0, 7)}</span><strong>{formatNumber(rows.length)} months</strong><span>{String(last.month).slice(0, 7)}</span></div>
      <div className="player-line-summary">
        <span>Rating <strong>{formatNumber(Math.round(number(first.average_rating)))} → {formatNumber(Math.round(number(last.average_rating)))}</strong></span>
        <span>Latest score <strong>{percentage(last.score_rate)}</strong></span>
      </div>
    </div>
  )
})

const OpeningChart = memo(function OpeningChart({ rows = [] }) {
  const maximumGames = Math.max(1, ...rows.map((row) => number(row.games)))
  if (!rows.length) return <EmptyChart>No opening records match this filter.</EmptyChart>
  return (
    <div className="player-opening-chart">
      {rows.map((row) => (
        <div className="player-opening-row" key={`${row.eco}-${row.color}`}>
          <div><strong title={row.eco}>{row.eco}</strong><small>{row.color}</small></div>
          <div className="player-horizontal-track"><span className={row.color} style={{ width: boundedWidth(row.games, maximumGames) }} /></div>
          <div><strong>{formatNumber(row.games)}</strong><small>{percentage(row.score_rate)} score</small></div>
        </div>
      ))}
    </div>
  )
})

const ClockChart = memo(function ClockChart({ rows = [] }) {
  const reactionPath = useMemo(() => linePath(rows, (row) => row.median_reaction_seconds), [rows])
  const remainingPath = useMemo(() => linePath(rows, (row) => row.median_time_left), [rows])
  if (!rows.length) return <EmptyChart>No clock records match this filter.</EmptyChart>
  const first = rows[0]
  const last = rows[rows.length - 1]
  return (
    <div className="player-line-chart">
      <div className="player-chart-legend"><span className="reaction">Move time</span><span className="remaining">Time remaining</span></div>
      <svg viewBox="0 0 620 180" role="img" aria-label="Median clock use through the game" preserveAspectRatio="none">
        <path className="chart-grid" d="M0 45 H620 M0 90 H620 M0 135 H620" />
        <path className="reaction" d={reactionPath} />
        <path className="remaining" d={remainingPath} />
      </svg>
      <div className="player-line-axis"><span>Moves {first.move_range}</span><span>Moves {last.move_range}</span></div>
      <div className="player-line-summary">
        <span>Opening move time <strong>{number(first.median_reaction_seconds).toFixed(1)}s</strong></span>
        <span>Late clock <strong>{number(last.median_time_left).toFixed(0)}s</strong></span>
      </div>
    </div>
  )
})

const ProfileChart = memo(function ProfileChart({ activity = [], opponents = [], lengths = [] }) {
  const activityMap = useMemo(() => new Map(activity.map((row) => [`${row.weekday}-${row.time_band}`, number(row.games)])), [activity])
  const maximumActivity = Math.max(1, ...activity.map((row) => number(row.games)))
  const maximumOpponents = Math.max(1, ...opponents.map((row) => number(row.games)))
  const maximumLengths = Math.max(1, ...lengths.map((row) => number(row.games)))
  if (!activity.length && !opponents.length && !lengths.length) return <EmptyChart>No game metadata matches this filter.</EmptyChart>
  return (
    <div className="player-profile-charts">
      <div className="player-activity-heatmap">
        <span />
        {TIME_BANDS.map((band) => <small key={band}>{band}</small>)}
        {WEEKDAYS.map((day, weekdayIndex) => (
          <div className="player-activity-row" key={day}>
            <strong>{day}</strong>
            {TIME_BANDS.map((band, bandIndex) => {
              const games = activityMap.get(`${weekdayIndex + 1}-${bandIndex}`) || 0
              const intensity = games / maximumActivity
              return <span key={band} title={`${day} ${band}: ${formatNumber(games)} games`} style={{ '--activity': intensity }} />
            })}
          </div>
        ))}
      </div>
      <div className="player-distributions">
        <strong>Opponent rating</strong>
        {opponents.map((row) => (
          <div key={row.rating_from}><span>{row.rating_from}–{number(row.rating_from) + 199}</span><i style={{ width: boundedWidth(row.games, maximumOpponents) }} /><small>{formatNumber(row.games)}</small></div>
        ))}
        <strong>Game length</strong>
        {lengths.map((row) => (
          <div key={row.moves_from}><span>{row.moves_from >= 100 ? '100+' : `${row.moves_from}–${number(row.moves_from) + 9}`}</span><i style={{ width: boundedWidth(row.games, maximumLengths) }} /><small>{formatNumber(row.games)}</small></div>
        ))}
      </div>
    </div>
  )
})

function EngineInsights({ data }) {
  const coverage = data.coverage || {}
  return (
    <>
      <div className="player-analysis-coverage">
        <div><span>Fully analyzed</span><strong>{formatNumber(coverage.analyzed_games || 0)} / {formatNumber(coverage.total_games || 0)}</strong></div>
        <div><span>Engine sample</span><strong>{formatNumber(coverage.sampled_games || 0)} games</strong></div>
        <div><span>Scored positions</span><strong>{formatNumber(coverage.analyzed_positions || 0)} / {formatNumber(coverage.positions || 0)}</strong></div>
        <div><span>Move samples</span><strong>{formatNumber(coverage.move_samples || 0)}</strong></div>
      </div>
      <div className="player-chart-grid">
        <ChartCard title="Move Quality by Game Phase" subtitle="Average centipawn loss and serious-error rate" metric="Complete games"><PhaseQualityChart rows={data.phase_quality} /></ChartCard>
        <ChartCard title="Advantage Conversion" subtitle="Results after reaching an engine advantage" metric="W / D / L"><ConversionChart rows={data.conversion} resilience={data.resilience} /></ChartCard>
        <ChartCard title="Time Pressure and Mistakes" subtitle="Move quality grouped by time remaining" metric="Clock + scores"><TimePressureChart rows={data.time_pressure} /></ChartCard>
        <ChartCard title="Endgame Precision" subtitle="Moves that preserve tablebase expected score" metric="7 pieces or fewer"><EndgameChart rows={data.endgame_precision} /></ChartCard>
      </div>
    </>
  )
}

function PlayingPatterns({ data }) {
  const summary = data.summary || {}
  const scoreRate = number(summary.games) ? ((number(summary.wins) + number(summary.draws) * 0.5) / number(summary.games)) * 100 : 0
  return (
    <>
      <div className="player-analysis-coverage">
        <div><span>Games</span><strong>{formatNumber(summary.games || 0)}</strong></div>
        <div><span>Score rate</span><strong>{percentage(scoreRate)}</strong></div>
        <div><span>Average opponent</span><strong>{formatNumber(Math.round(number(summary.average_opponent_rating)))}</strong></div>
        <div><span>Clock sample</span><strong>{formatNumber(summary.clock_sample_games || 0)} games</strong></div>
      </div>
      <div className="player-chart-grid">
        <ChartCard title="Rating and Results Journey" subtitle="Monthly rating and score-rate direction" metric="Up to 20 years"><JourneyChart rows={data.rating_results} /></ChartCard>
        <ChartCard title="Opening Repertoire" subtitle="Most frequent ECO lines by color and score" metric="Top 12"><OpeningChart rows={data.opening_repertoire} /></ChartCard>
        <ChartCard title="Clock Habits" subtitle="Median thinking time and clock remaining" metric="5-move bands"><ClockChart rows={data.clock_curve} /></ChartCard>
        <ChartCard title="Activity and Opponent Profile" subtitle="When games happen and what they look like" metric="UTC game time"><ProfileChart activity={data.activity} opponents={data.opponent_distribution} lengths={data.length_distribution} /></ChartCard>
      </div>
    </>
  )
}

export default function PlayerAnalysisWorkspace({ playerName, disabled = false }) {
  const [tab, setTab] = useState('engine')
  const [mode, setMode] = useState('all')
  const [draftDateFrom, setDraftDateFrom] = useState('')
  const [draftDateTo, setDraftDateTo] = useState('')
  const [dates, setDates] = useState({ from: '', to: '' })
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    if (!playerName || disabled) {
      setData(null)
      return undefined
    }
    const controller = new AbortController()
    setLoading(true)
    setError('')
    fetchPlayerAnalysis({
      playerName,
      tab,
      mode,
      dateFrom: dates.from,
      dateTo: dates.to,
      signal: controller.signal
    }).then((payload) => {
      setData(payload)
    }).catch((requestError) => {
      if (requestError.name !== 'AbortError') {
        setError(requestError.message || 'Unable to load player analysis.')
        setData(null)
      }
    }).finally(() => {
      if (!controller.signal.aborted) setLoading(false)
    })
    return () => controller.abort()
  }, [playerName, disabled, tab, mode, dates])

  const applyDates = () => {
    if (draftDateFrom && draftDateTo && draftDateFrom > draftDateTo) {
      setError('The start date must be before the end date.')
      return
    }
    setDates({ from: draftDateFrom, to: draftDateTo })
  }

  const clearDates = () => {
    setDraftDateFrom('')
    setDraftDateTo('')
    setDates({ from: '', to: '' })
  }

  const scope = `${mode === 'all' ? 'All modes' : mode} · ${dates.from || dates.to ? `${dates.from || 'First game'} → ${dates.to || 'Latest game'}` : 'All recorded dates'}`

  return (
    <section className="games-mode-detail player-analysis-workspace">
      <div className="player-analysis-heading">
        <div><p className="eyebrow">PLAYER ANALYSIS</p><h2>Performance laboratory</h2><p>Aggregated reports from this player’s games. No raw position lists are sent to the browser.</p></div>
        <span className="stat-chip">{scope}</span>
      </div>
      <div className="player-analysis-tabs" role="tablist" aria-label="Player analysis categories">
        {TABS.map((item) => (
          <button className={`player-analysis-tab ${tab === item.key ? 'active' : ''}`} id={`player-analysis-tab-${item.key}`} key={item.key} type="button" role="tab" aria-controls="player-analysis-panel" aria-selected={tab === item.key} onClick={() => setTab(item.key)}>
            <strong>{item.label}</strong><small>{item.note}</small>
          </button>
        ))}
      </div>
      <div className="player-analysis-panel" id="player-analysis-panel" role="tabpanel" aria-labelledby={`player-analysis-tab-${tab}`}>
        <div className="player-analysis-filters">
          <div className="player-analysis-mode-filter" aria-label="Game mode">
            {MODES.map((item) => <button className={mode === item ? 'active' : ''} key={item} type="button" disabled={disabled} onClick={() => setMode(item)}>{item}</button>)}
          </div>
          <div className="player-analysis-date-filter">
            <label><span>From</span><input className="text-input" type="date" value={draftDateFrom} max={draftDateTo || undefined} disabled={disabled} onChange={(event) => setDraftDateFrom(event.target.value)} /></label>
            <label><span>To</span><input className="text-input" type="date" value={draftDateTo} min={draftDateFrom || undefined} disabled={disabled} onChange={(event) => setDraftDateTo(event.target.value)} /></label>
            <button className="btn btn-primary btn-inline" type="button" disabled={disabled || (draftDateFrom === dates.from && draftDateTo === dates.to)} onClick={applyDates}>Apply dates</button>
            {(draftDateFrom || draftDateTo || dates.from || dates.to) ? <button className="btn btn-secondary btn-inline" type="button" onClick={clearDates}>Clear</button> : null}
          </div>
        </div>
        {error ? <p className="player-analysis-error" role="alert">{error}</p> : null}
        {loading ? <div className="player-analysis-loading" aria-live="polite"><span />Calculating aggregated charts…</div> : null}
        {!loading && !error && data ? (tab === 'engine' ? <EngineInsights data={data} /> : <PlayingPatterns data={data} />) : null}
        {!loading && !error && !data ? <EmptyChart>Select a player to load analysis.</EmptyChart> : null}
      </div>
    </section>
  )
}
