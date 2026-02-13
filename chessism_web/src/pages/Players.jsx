import { useMemo, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import { API_BASE_URL } from '../config'

async function fetchPlayerProfile(playerName) {
  const response = await fetch(`${API_BASE_URL}/players/${encodeURIComponent(playerName)}`)
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function downloadAllPlayerGames(playerName) {
  const response = await fetch(`${API_BASE_URL}/games`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ player_name: playerName })
  })
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchGameSummary(playerName) {
  const response = await fetch(`${API_BASE_URL}/games/${encodeURIComponent(playerName)}/summary`)
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchRecentGames(playerName, pageSize = 20) {
  const response = await fetch(
    `${API_BASE_URL}/games/${encodeURIComponent(playerName)}/recent?page=1&page_size=${pageSize}`
  )
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchModesStats(playerName) {
  const response = await fetch(`${API_BASE_URL}/players/${encodeURIComponent(playerName)}/modes_stats`)
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchModeChart(playerName, mode, rangeType = 'all', years = null) {
  const params = new URLSearchParams({
    mode,
    range_type: rangeType
  })
  if (rangeType === 'years' && years) {
    params.set('years', String(years))
  }

  const response = await fetch(
    `${API_BASE_URL}/players/${encodeURIComponent(playerName)}/mode_chart?${params.toString()}`
  )
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

function ScatterChart({ chart }) {
  const points = chart?.points || []
  const yAxis = chart?.y_axis || {}
  const stats = chart?.stats || {}

  if (!points.length) {
    return <p className="result-line">No chart data for this mode.</p>
  }

  const width = 820
  const height = 280
  const padLeft = 44
  const padRight = 18
  const padTop = 16
  const padBottom = 30
  const minValue = typeof yAxis.min === 'number' ? yAxis.min : Math.min(...points.map((point) => point.y))
  const maxValue = typeof yAxis.max === 'number' ? yAxis.max : Math.max(...points.map((point) => point.y))
  const valueRange = Math.max(1, maxValue - minValue)
  const plotWidth = width - padLeft - padRight
  const plotHeight = height - padTop - padBottom

  const plottedPoints = points.map((point, index) => {
    const x = points.length === 1
      ? padLeft + plotWidth / 2
      : padLeft + (index / (points.length - 1)) * plotWidth
    const y = padTop + ((maxValue - point.y) / valueRange) * plotHeight
    return { ...point, x, y }
  })

  const yTicks = Array.isArray(yAxis.ticks) && yAxis.ticks.length ? yAxis.ticks : [minValue, maxValue]
  const firstLabel = points[0]?.x || ''
  const lastLabel = points[points.length - 1]?.x || ''

  return (
    <div className="rating-chart-shell">
      <h3>{chart.chart_title || 'rating chart'}</h3>
      <svg viewBox={`0 0 ${width} ${height}`} className="rating-chart" role="img" aria-label={chart.chart_title || 'rating chart'}>
        <line x1={padLeft} y1={padTop} x2={padLeft} y2={height - padBottom} className="axis-line" />
        <line x1={padLeft} y1={height - padBottom} x2={width - padRight} y2={height - padBottom} className="axis-line" />
        {yTicks.map((tick, index) => {
          const y = padTop + ((maxValue - tick) / valueRange) * plotHeight
          return (
            <g key={`tick-${tick}-${index}`}>
              <line x1={padLeft - 5} y1={y} x2={padLeft} y2={y} className="axis-line" />
              <text x={padLeft - 9} y={y + 4} className="axis-tick-label" textAnchor="end">
                {tick}
              </text>
            </g>
          )
        })}
        {plottedPoints.map((point, index) => (
          <circle
            key={`${point.x}-${point.y}-${index}`}
            cx={point.x}
            cy={point.y}
            r="3.2"
            className="scatter-point"
            style={{ fill: point.color }}
          />
        ))}
      </svg>
      <div className="rating-chart-labels">
        <span>{firstLabel}</span>
        <span>{lastLabel}</span>
      </div>
      <div className="rating-chart-meta">
        <span>Mean {stats.mean ?? '-'}</span>
        <span>Std {stats.std ?? '-'}</span>
      </div>
    </div>
  )
}

function Players() {
  const [playerName, setPlayerName] = useState('')
  const [activePlayer, setActivePlayer] = useState('')
  const [loading, setLoading] = useState(false)
  const [message, setMessage] = useState('')
  const [profile, setProfile] = useState(null)
  const [summary, setSummary] = useState(null)
  const [recent, setRecent] = useState(null)
  const [error, setError] = useState('')

  const [modesStats, setModesStats] = useState({})
  const [showModeDropdown, setShowModeDropdown] = useState(false)
  const [selectedMode, setSelectedMode] = useState('')
  const [modeChart, setModeChart] = useState(null)
  const [ratingLoading, setRatingLoading] = useState(false)
  const [ratingError, setRatingError] = useState('')
  const [rangeType, setRangeType] = useState('all')
  const [rangeYears, setRangeYears] = useState('2')

  const modeEntries = useMemo(
    () => Object.entries(modesStats || {}).sort((a, b) => (b[1].n_games || 0) - (a[1].n_games || 0)),
    [modesStats]
  )

  const loadModeChart = async (mode, nextRangeType = rangeType, nextRangeYears = rangeYears) => {
    if (!activePlayer || !mode) return

    setRatingLoading(true)
    setRatingError('')
    try {
      const yearsValue = nextRangeType === 'years' ? Math.max(1, parseInt(nextRangeYears, 10) || 1) : null
      const payload = await fetchModeChart(activePlayer, mode, nextRangeType, yearsValue)
      setModeChart(payload)
    } catch (err) {
      setModeChart(null)
      setRatingError(err instanceof Error ? err.message : 'Failed to load chart.')
    } finally {
      setRatingLoading(false)
    }
  }

  const handleExplore = async () => {
    const name = playerName.trim().toLowerCase()
    if (!name) {
      setError('Enter a player name.')
      return
    }

    setLoading(true)
    setError('')
    setMessage('')
    setRatingError('')
    try {
      let data = await fetchPlayerProfile(name)

      if (data?.joined === null || data?.joined === 0) {
        const dl = await downloadAllPlayerGames(name)
        setMessage(dl.message || 'Downloading games and updating profile...')
        data = await fetchPlayerProfile(name)
      }

      const normalizedPlayer = data?.player_name || name
      setActivePlayer(normalizedPlayer)
      setProfile(data)

      const [sum, rec, modes] = await Promise.all([
        fetchGameSummary(normalizedPlayer),
        fetchRecentGames(normalizedPlayer, 20),
        fetchModesStats(normalizedPlayer)
      ])

      setSummary(sum)
      setRecent(rec)
      setModesStats(modes || {})
      setShowModeDropdown(false)
      setSelectedMode('')
      setModeChart(null)
      setRangeType('all')
      setRangeYears('2')
    } catch (err) {
      const messageText = err instanceof Error ? err.message : 'Request failed.'
      setError(messageText)
      setProfile(null)
      setSummary(null)
      setRecent(null)
      setModesStats({})
      setSelectedMode('')
      setModeChart(null)
      setRatingError('')
      setRangeType('all')
      setRangeYears('2')
    } finally {
      setLoading(false)
    }
  }

  const handleModeSelect = async (mode) => {
    setSelectedMode(mode)
    await loadModeChart(mode, rangeType, rangeYears)
  }

  const handleRangeSelect = async (nextRangeType) => {
    setRangeType(nextRangeType)
    if (!selectedMode) return
    await loadModeChart(selectedMode, nextRangeType, rangeYears)
  }

  const handleApplyYears = async () => {
    const safeYears = String(Math.max(1, parseInt(rangeYears, 10) || 1))
    setRangeYears(safeYears)
    if (!selectedMode) return
    setRangeType('years')
    await loadModeChart(selectedMode, 'years', safeYears)
  }

  const profileRows = profile
    ? [
        ['Player', profile.player_name],
        ['Name', profile.name],
        ['Title', profile.title],
        ['Country', profile.country],
        ['Location', profile.location],
        ['Followers', profile.followers],
        ['Joined', profile.joined],
        ['Status', profile.status]
      ].filter(([, value]) => value !== null && value !== undefined && value !== '')
    : []

  return (
    <div className="home-shell">
      <Header />
      <main>
        <section className="players-hero">
          <div>
            <h1>Player Explorer</h1>
            <p>Search a player to load their profile, stats, and recent games.</p>
          </div>
          <div className="players-input">
            <label htmlFor="player-name">Player name</label>
            <input
              id="player-name"
              className="text-input"
              type="text"
              value={playerName}
              onChange={(event) => setPlayerName(event.target.value)}
              onKeyDown={(event) => event.key === 'Enter' && handleExplore()}
              placeholder="e.g. hikaru"
            />
            <button className="btn btn-primary" type="button" onClick={handleExplore} disabled={loading}>
              {loading ? 'Loading...' : 'Explore'}
            </button>
          </div>
        </section>

        {message ? <p className="result-line">{message}</p> : null}
        {error ? <p className="result-line">{error}</p> : null}

        <section className="player-profile">
          <div className="section-head">
            <h2>Player Profile</h2>
          </div>
          <div className="profile-grid">
            {profileRows.length ? (
              profileRows.map(([label, value]) => (
                <div key={label} className="profile-item">
                  <span>{label}</span>
                  <strong>{String(value)}</strong>
                </div>
              ))
            ) : (
              <p className="result-line">No profile loaded.</p>
            )}
          </div>
        </section>

        <section className="player-stats">
          <div className="section-head">
            <h2>Player Summary</h2>
            <p>Wins, losses, draws, and date range.</p>
          </div>
          {summary ? (
            <div className="games-stats">
              <div className="wl-bar">
                <div className="wl-row">
                  <span className="wl-label">Wins</span>
                  <div className="wl-track">
                    <div
                      className="wl-fill wl-fill-win"
                      style={{ width: `${((summary.wins ?? 0) / (summary.total_games || 1)) * 100}%` }}
                    />
                  </div>
                  <span className="wl-value">{summary.wins ?? 0}</span>
                </div>
                <div className="wl-row">
                  <span className="wl-label">Losses</span>
                  <div className="wl-track">
                    <div
                      className="wl-fill wl-fill-loss"
                      style={{ width: `${((summary.losses ?? 0) / (summary.total_games || 1)) * 100}%` }}
                    />
                  </div>
                  <span className="wl-value">{summary.losses ?? 0}</span>
                </div>
                <div className="wl-row">
                  <span className="wl-label">Draws</span>
                  <div className="wl-track">
                    <div
                      className="wl-fill wl-fill-draw"
                      style={{ width: `${((summary.draws ?? 0) / (summary.total_games || 1)) * 100}%` }}
                    />
                  </div>
                  <span className="wl-value">{summary.draws ?? 0}</span>
                </div>
              </div>
              <div className="date-range">
                <span className="date-chip">{summary.date_from ? summary.date_from.slice(0, 10) : '-'}</span>
                <span className="date-to">TO</span>
                <span className="date-chip">{summary.date_to ? summary.date_to.slice(0, 10) : '-'}</span>
              </div>
            </div>
          ) : (
            <p className="result-line">No summary available.</p>
          )}
        </section>

        <section className="player-rating">
          <div className="section-head">
            <h2>RATING</h2>
            <p>Select a time-control mode and inspect the rating scatter over time.</p>
          </div>

          <div className="rating-controls">
            <button
              className="btn btn-secondary btn-inline"
              type="button"
              onClick={() => setShowModeDropdown((prev) => !prev)}
              disabled={!modeEntries.length}
            >
              time controls
            </button>
            {selectedMode ? <span className="mode-selected">Selected: {selectedMode}</span> : null}
            {modeChart?.range?.type ? (
              <span className="mode-selected">
                Range: {modeChart.range.type === 'years' ? `${modeChart.range.years} years` : modeChart.range.type}
              </span>
            ) : null}
          </div>

          {showModeDropdown && modeEntries.length ? (
            <div className="modes-dropdown">
              {modeEntries.map(([mode, stats]) => (
                <button
                  key={mode}
                  type="button"
                  className={`mode-option ${selectedMode === mode ? 'active' : ''}`}
                  onClick={() => handleModeSelect(mode)}
                >
                  <span>{mode}</span>
                  <span>{stats.n_games} games</span>
                  <span>W:{stats.as_white} B:{stats.as_black}</span>
                  <span>{stats.oldest_rating} -> {stats.newest_rating}</span>
                </button>
              ))}
            </div>
          ) : null}

          {ratingLoading ? <p className="result-line">Loading chart...</p> : null}
          {ratingError ? <p className="result-line">{ratingError}</p> : null}
          {!ratingLoading && !ratingError && modeChart ? <ScatterChart chart={modeChart} /> : null}

          <div className="rating-range-controls">
            <button className="btn btn-secondary btn-inline" type="button" onClick={() => handleRangeSelect('six_months')}>
              Last 6 months
            </button>
            <button className="btn btn-secondary btn-inline" type="button" onClick={() => handleRangeSelect('one_year')}>
              Last year
            </button>
            <div className="years-picker">
              <input
                className="text-input years-input"
                type="number"
                min="1"
                max="20"
                value={rangeYears}
                onChange={(event) => setRangeYears(event.target.value)}
              />
              <button className="btn btn-secondary btn-inline" type="button" onClick={handleApplyYears}>
                N years
              </button>
            </div>
            <button className="btn btn-secondary btn-inline" type="button" onClick={() => handleRangeSelect('all')}>
              Whole time
            </button>
          </div>
        </section>

        <section className="player-recent">
          <div className="section-head">
            <h2>Last 20 Games</h2>
            <p>Date | Hour | Result | Explore</p>
          </div>
          <div className="recent-list">
            {recent?.games?.length ? (
              recent.games.map((game, index) => {
                const [datePart, timePart] = game.played_at.split(' ')
                const resultClass =
                  game.result === 'win' ? 'result-win' : game.result === 'loss' ? 'result-loss' : 'result-draw'

                return (
                  <div key={`${game.link || index}-${game.played_at}`} className="recent-line player-recent-line">
                    <span>{datePart}</span>
                    <span className="time-chip">{timePart}</span>
                    <span className={resultClass}>{game.result}</span>
                    <a className="btn btn-secondary btn-inline recent-explore" href={`/games/${game.link || ''}`}>
                      Explore
                    </a>
                  </div>
                )
              })
            ) : (
              <p className="result-line">No games loaded.</p>
            )}
          </div>
        </section>
      </main>
      <Footer />
    </div>
  )
}

export default Players
