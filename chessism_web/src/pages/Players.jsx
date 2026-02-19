import { useEffect, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import { Chessboard } from 'react-chessboard'
import { Chess } from 'chess.js'
import { API_BASE_URL } from '../config'

const MOVES_PAGE_SIZE = 5
const OPENINGS_PAGE_SIZE = 5
const TOP_MOVE_LIMIT = 10
const OPENING_N_MOVES_MIN = 3
const OPENING_N_MOVES_MAX = 10
const OPENING_N_MOVES_DEFAULT = 3
const OPENING_RESULTS = ['win', 'loss', 'draw']

const formatNumber = (value) => {
  const num = Number(value ?? 0)
  if (Number.isNaN(num)) return '0'
  return num.toLocaleString('en-US')
}

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

async function fetchModeGames(playerName, mode) {
  const response = await fetch(
    `${API_BASE_URL}/games/${encodeURIComponent(playerName)}/mode_games?mode=${encodeURIComponent(mode)}`
  )
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchPlayerHours(playerName) {
  const response = await fetch(`${API_BASE_URL}/games/${encodeURIComponent(playerName)}/hours_played`)
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

async function fetchPlayerTopMoves(playerName, mode, moveColor = 'white', page = 1, pageSize = MOVES_PAGE_SIZE, maxMove = TOP_MOVE_LIMIT) {
  const params = new URLSearchParams({
    player_color: moveColor,
    page: String(page),
    page_size: String(pageSize),
    max_move: String(maxMove)
  })
  const response = await fetch(
    `${API_BASE_URL}/games/${encodeURIComponent(playerName)}/time_controls/${encodeURIComponent(mode)}/top_moves?${params.toString()}`
  )
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  return payload
}

async function fetchPlayerTopOpenings(playerName, mode, resultFilter, nMoves = OPENING_N_MOVES_DEFAULT, page = 1, pageSize = OPENINGS_PAGE_SIZE) {
  const params = new URLSearchParams({
    result_filter: resultFilter,
    page: String(page),
    page_size: String(pageSize),
    n_moves: String(nMoves)
  })
  const response = await fetch(
    `${API_BASE_URL}/games/${encodeURIComponent(playerName)}/time_controls/${encodeURIComponent(mode)}/top_openings?${params.toString()}`
  )
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  return payload
}

async function fetchPlayerResults(playerName, mode) {
  const response = await fetch(
    `${API_BASE_URL}/games/${encodeURIComponent(playerName)}/time_controls/${encodeURIComponent(mode)}/results`
  )
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  return payload
}

async function fetchPlayerLengths(playerName, mode) {
  const response = await fetch(
    `${API_BASE_URL}/games/${encodeURIComponent(playerName)}/time_controls/${encodeURIComponent(mode)}/lengths`
  )
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  return payload
}

async function fetchPlayerActivityTrend(playerName, mode) {
  const response = await fetch(
    `${API_BASE_URL}/games/${encodeURIComponent(playerName)}/time_controls/${encodeURIComponent(mode)}/activity_trend`
  )
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
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

function CompactBarChart({ labels, values, title = '' }) {
  const safeLabels = Array.isArray(labels) ? labels : []
  const safeValues = Array.isArray(values) ? values : []
  const maxValue = Math.max(1, ...safeValues.map((v) => Number(v || 0)))
  return (
    <div className="analytics-bars-wrap">
      {title ? <h3>{title}</h3> : null}
      <div className="analytics-bars-scroll">
        <div className="analytics-bars">
          {safeLabels.map((label, idx) => {
            const value = Number(safeValues[idx] || 0)
            const percent = Math.max(2, Math.round((value / maxValue) * 100))
            return (
              <div key={`bar-${label}-${idx}`} className="analytics-bar-col">
                <span className="analytics-bar-value">{formatNumber(value)}</span>
                <div className="analytics-bar-track">
                  <div className="analytics-bar-fill" style={{ height: `${percent}%` }} />
                </div>
                <span className="analytics-bar-label">{label}</span>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

function HeatStrip({ title, labels, values, singleRow = false }) {
  const safeLabels = Array.isArray(labels) ? labels : []
  const safeValues = Array.isArray(values) ? values : []
  const maxValue = Math.max(1, ...safeValues.map((v) => Number(v || 0)))
  return (
    <div className="heat-strip-wrap">
      <h3>{title}</h3>
      <div className="heat-strip-scroll">
        <div
          className={`heat-strip-grid ${singleRow ? 'single-row' : ''}`}
          style={singleRow ? { '--heat-cols': safeLabels.length || 24 } : undefined}
        >
          {safeLabels.map((label, idx) => {
            const value = Number(safeValues[idx] || 0)
            const ratio = Math.max(0, Math.min(1, value / maxValue))
            const red = Math.round(240 - (240 - 63) * ratio)
            const green = Math.round(113 + (208 - 113) * ratio)
            const blue = Math.round(103 + (137 - 103) * ratio)
            return (
              <div key={`heat-${label}-${idx}`} className="heat-cell-wrap">
                <div className="heat-cell" style={{ background: `rgb(${red} ${green} ${blue})` }}>
                  {label}
                </div>
                <span className="heat-cell-value">{formatNumber(value)}</span>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

const fenFromHalfMoves = (halfMoves, targetPly) => {
  const game = new Chess()
  const safePly = Math.max(0, Math.min(Number(targetPly || 0), (halfMoves || []).length))
  for (let idx = 0; idx < safePly; idx += 1) {
    const moved = game.move(halfMoves[idx])
    if (!moved) break
  }
  return game.fen()
}

function Players() {
  const [playerName, setPlayerName] = useState('')
  const [activePlayer, setActivePlayer] = useState('')
  const [loading, setLoading] = useState(false)
  const [message, setMessage] = useState('')
  const [profile, setProfile] = useState(null)
  const [recent, setRecent] = useState(null)
  const [modeGames, setModeGames] = useState(null)
  const [playerHours, setPlayerHours] = useState(null)
  const [error, setError] = useState('')

  const [modesStats, setModesStats] = useState({})
  const [selectedMode, setSelectedMode] = useState('')
  const [modeChart, setModeChart] = useState(null)
  const [ratingModalOpen, setRatingModalOpen] = useState(false)
  const [ratingLoading, setRatingLoading] = useState(false)
  const [ratingError, setRatingError] = useState('')
  const [rangeType, setRangeType] = useState('all')
  const [rangeYears, setRangeYears] = useState('2')
  const [moveColor, setMoveColor] = useState('white')
  const [topMovesData, setTopMovesData] = useState(null)
  const [topMovesLoading, setTopMovesLoading] = useState(false)
  const [topMovesError, setTopMovesError] = useState('')
  const [resultsData, setResultsData] = useState(null)
  const [resultsLoading, setResultsLoading] = useState(false)
  const [resultsError, setResultsError] = useState('')
  const [lengthsData, setLengthsData] = useState(null)
  const [lengthsLoading, setLengthsLoading] = useState(false)
  const [lengthsError, setLengthsError] = useState('')
  const [activityData, setActivityData] = useState(null)
  const [activityLoading, setActivityLoading] = useState(false)
  const [activityError, setActivityError] = useState('')
  const [openingStates, setOpeningStates] = useState({
    win: { nMoves: String(OPENING_N_MOVES_DEFAULT), rows: [], loading: false, error: '', requested: false, activeTop: null, activePly: 0 },
    loss: { nMoves: String(OPENING_N_MOVES_DEFAULT), rows: [], loading: false, error: '', requested: false, activeTop: null, activePly: 0 },
    draw: { nMoves: String(OPENING_N_MOVES_DEFAULT), rows: [], loading: false, error: '', requested: false, activeTop: null, activePly: 0 }
  })

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

  const loadTopMoves = async (mode, nextMoveColor = moveColor, nextPage = 1) => {
    if (!activePlayer || !mode) return
    setTopMovesLoading(true)
    setTopMovesError('')
    try {
      const payload = await fetchPlayerTopMoves(activePlayer, mode, nextMoveColor, nextPage, MOVES_PAGE_SIZE, TOP_MOVE_LIMIT)
      setTopMovesData(payload)
    } catch (err) {
      setTopMovesData(null)
      setTopMovesError(err instanceof Error ? err.message : 'Failed to load top moves.')
    } finally {
      setTopMovesLoading(false)
    }
  }

  const loadResults = async (mode) => {
    if (!activePlayer || !mode) return
    setResultsLoading(true)
    setResultsError('')
    try {
      const payload = await fetchPlayerResults(activePlayer, mode)
      setResultsData(payload)
    } catch (err) {
      setResultsData(null)
      setResultsError(err instanceof Error ? err.message : 'Failed to load results.')
    } finally {
      setResultsLoading(false)
    }
  }

  const loadLengths = async (mode) => {
    if (!activePlayer || !mode) return
    setLengthsLoading(true)
    setLengthsError('')
    try {
      const payload = await fetchPlayerLengths(activePlayer, mode)
      setLengthsData(payload)
    } catch (err) {
      setLengthsData(null)
      setLengthsError(err instanceof Error ? err.message : 'Failed to load lengths.')
    } finally {
      setLengthsLoading(false)
    }
  }

  const loadActivity = async (mode) => {
    if (!activePlayer || !mode) return
    setActivityLoading(true)
    setActivityError('')
    try {
      const payload = await fetchPlayerActivityTrend(activePlayer, mode)
      setActivityData(payload)
    } catch (err) {
      setActivityData(null)
      setActivityError(err instanceof Error ? err.message : 'Failed to load activity trend.')
    } finally {
      setActivityLoading(false)
    }
  }

  const loadOpenings = async (mode, resultFilter) => {
    const state = openingStates[resultFilter]
    const nMoves = Math.max(OPENING_N_MOVES_MIN, Math.min(OPENING_N_MOVES_MAX, Number.parseInt(state?.nMoves, 10) || OPENING_N_MOVES_DEFAULT))
    setOpeningStates((prev) => ({
      ...prev,
      [resultFilter]: { ...prev[resultFilter], loading: true, error: '', requested: true }
    }))
    try {
      const payload = await fetchPlayerTopOpenings(activePlayer, mode, resultFilter, nMoves, 1, OPENINGS_PAGE_SIZE)
      const rows = Array.isArray(payload?.rows) ? payload.rows : []
      const activeTop = rows.length ? Number(rows[0].top || 1) : null
      const firstHalfMoves = rows.length ? (rows[0].half_moves || []) : []
      setOpeningStates((prev) => ({
        ...prev,
        [resultFilter]: {
          ...prev[resultFilter],
          loading: false,
          rows,
          activeTop,
          activePly: firstHalfMoves.length > 0 ? 1 : 0
        }
      }))
    } catch (err) {
      setOpeningStates((prev) => ({
        ...prev,
        [resultFilter]: {
          ...prev[resultFilter],
          loading: false,
          rows: [],
          activeTop: null,
          activePly: 0,
          error: err instanceof Error ? err.message : 'Failed to load openings.'
        }
      }))
    }
  }

  const handleExplore = async (overrideName = null) => {
    const name = String(overrideName ?? playerName).trim().toLowerCase()
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

      const [modes, hoursPayload] = await Promise.all([
        fetchModesStats(normalizedPlayer),
        fetchPlayerHours(normalizedPlayer)
      ])

      setRecent(null)
      setModeGames(null)
      setPlayerHours(hoursPayload)
      setModesStats(modes || {})
      setSelectedMode('')
      setMoveColor('white')
      setTopMovesData(null)
      setTopMovesError('')
      setResultsData(null)
      setResultsError('')
      setLengthsData(null)
      setLengthsError('')
      setActivityData(null)
      setActivityError('')
      setOpeningStates({
        win: { nMoves: String(OPENING_N_MOVES_DEFAULT), rows: [], loading: false, error: '', requested: false, activeTop: null, activePly: 0 },
        loss: { nMoves: String(OPENING_N_MOVES_DEFAULT), rows: [], loading: false, error: '', requested: false, activeTop: null, activePly: 0 },
        draw: { nMoves: String(OPENING_N_MOVES_DEFAULT), rows: [], loading: false, error: '', requested: false, activeTop: null, activePly: 0 }
      })
      setModeChart(null)
      setRangeType('all')
      setRangeYears('2')
    } catch (err) {
      const messageText = err instanceof Error ? err.message : 'Request failed.'
      setError(messageText)
      setProfile(null)
      setRecent(null)
      setModeGames(null)
      setPlayerHours(null)
      setModesStats({})
      setSelectedMode('')
      setMoveColor('white')
      setTopMovesData(null)
      setTopMovesError('')
      setResultsData(null)
      setResultsError('')
      setLengthsData(null)
      setLengthsError('')
      setActivityData(null)
      setActivityError('')
      setOpeningStates({
        win: { nMoves: String(OPENING_N_MOVES_DEFAULT), rows: [], loading: false, error: '', requested: false, activeTop: null, activePly: 0 },
        loss: { nMoves: String(OPENING_N_MOVES_DEFAULT), rows: [], loading: false, error: '', requested: false, activeTop: null, activePly: 0 },
        draw: { nMoves: String(OPENING_N_MOVES_DEFAULT), rows: [], loading: false, error: '', requested: false, activeTop: null, activePly: 0 }
      })
      setModeChart(null)
      setRatingError('')
      setRangeType('all')
      setRangeYears('2')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (typeof window === 'undefined') return
    const playerFromQuery = new URLSearchParams(window.location.search).get('player')
    const normalized = String(playerFromQuery || '').trim().toLowerCase()
    if (!normalized) return
    setPlayerName(normalized)
    handleExplore(normalized)
  }, [])

  useEffect(() => {
    const handleEsc = (event) => {
      if (event.key === 'Escape') {
        setRatingModalOpen(false)
      }
    }
    window.addEventListener('keydown', handleEsc)
    return () => window.removeEventListener('keydown', handleEsc)
  }, [])

  const handleModeSelect = async (mode) => {
    if (!activePlayer) return
    setSelectedMode(mode)
    setRatingError('')
    try {
      const [gamesPayload] = await Promise.all([
        fetchModeGames(activePlayer, mode),
        loadModeChart(mode, rangeType, rangeYears)
      ])
      setModeGames(gamesPayload)
      setRecent({
        games: Array.isArray(gamesPayload?.games) ? gamesPayload.games.slice(0, 20) : []
      })
      await Promise.all([
        loadTopMoves(mode, moveColor, 1),
        loadResults(mode),
        loadLengths(mode),
        loadActivity(mode)
      ])
      setOpeningStates((prev) => ({
        win: { ...prev.win, rows: [], requested: false, error: '', activeTop: null, activePly: 0 },
        loss: { ...prev.loss, rows: [], requested: false, error: '', activeTop: null, activePly: 0 },
        draw: { ...prev.draw, rows: [], requested: false, error: '', activeTop: null, activePly: 0 }
      }))
    } catch (err) {
      setModeGames(null)
      setRecent(null)
      setRatingError(err instanceof Error ? err.message : 'Failed to load mode data.')
    }
  }

  const handleMoveColorChange = async (nextColor) => {
    setMoveColor(nextColor)
    if (!selectedMode) return
    await loadTopMoves(selectedMode, nextColor, 1)
  }

  const handleTopMovesPrevious = async () => {
    if (!selectedMode) return
    const currentPage = Number(topMovesData?.page || 1)
    if (currentPage <= 1) return
    await loadTopMoves(selectedMode, moveColor, currentPage - 1)
  }

  const handleTopMovesNext = async () => {
    if (!selectedMode) return
    const currentPage = Number(topMovesData?.page || 1)
    const totalPages = Number(topMovesData?.total_pages || 0)
    if (!totalPages || currentPage >= totalPages) return
    await loadTopMoves(selectedMode, moveColor, currentPage + 1)
  }

  const handleOpeningMovesInput = (resultFilter, value) => {
    setOpeningStates((prev) => ({
      ...prev,
      [resultFilter]: { ...prev[resultFilter], nMoves: value }
    }))
  }

  const handleLoadOpenings = async (resultFilter) => {
    if (!selectedMode) return
    await loadOpenings(selectedMode, resultFilter)
  }

  const handleSelectOpeningTop = (resultFilter, top) => {
    const rows = openingStates[resultFilter]?.rows || []
    const row = rows.find((item) => Number(item.top) === Number(top))
    const halfMoves = row?.half_moves || []
    setOpeningStates((prev) => ({
      ...prev,
      [resultFilter]: {
        ...prev[resultFilter],
        activeTop: Number(top),
        activePly: halfMoves.length > 0 ? 1 : 0
      }
    }))
  }

  const handleOpeningStep = (resultFilter, direction) => {
    setOpeningStates((prev) => {
      const state = prev[resultFilter]
      const rows = state?.rows || []
      const activeTop = state?.activeTop
      const row = rows.find((item) => Number(item.top) === Number(activeTop))
      const totalHalfMoves = row?.half_moves?.length || 0
      const currentPly = Number(state?.activePly || 0)
      const nextPly = direction === 'next'
        ? Math.min(totalHalfMoves, currentPly + 1)
        : Math.max(1, currentPly - 1)
      return {
        ...prev,
        [resultFilter]: {
          ...state,
          activePly: nextPly
        }
      }
    })
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

  const joinedDisplay =
    profile?.joined && Number(profile.joined) > 0
      ? new Date(Number(profile.joined) * 1000)
          .toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: '2-digit', timeZone: 'UTC' })
          .replace(',', '')
          .toUpperCase()
      : null
  const profileRows = profile
    ? [
        ['Player', profile.player_name],
        ['Name', profile.name],
        ['Title', profile.title],
        ['Country', profile.country],
        ['Location', profile.location],
        ['Followers', profile.followers],
        ['Joined', joinedDisplay],
        ['Status', profile.status]
      ].filter(([, value]) => value !== null && value !== undefined && value !== '')
    : []
  const modeCounts = {
    bullet: Number(modesStats?.bullet?.n_games || 0),
    blitz: Number(modesStats?.blitz?.n_games || 0),
    rapid: Number(modesStats?.rapid?.n_games || 0)
  }
  const winsRate = Number(modeGames?.score_rate || 0)
  const winsRateColor = `hsl(${Math.round(Math.max(0, Math.min(1, winsRate)) * 120)} 78% 46%)`
  const profileTitle = activePlayer || 'player profile'
  const profileAvatar = String(profile?.avatar || '').trim()
  const profileInitial = String(profile?.player_name || '?').trim().charAt(0).toUpperCase() || '?'
  const ratingTitle = `${selectedMode || 'time control'} raiting`
  const ratingStats = {
    wins: Number(modeGames?.wins || 0),
    draws: Number(modeGames?.draws || 0),
    losses: Number(modeGames?.losses || 0)
  }
  const formatRatingStatCount = (value) => String(Math.max(0, Number(value) || 0)).padStart(4, '0')
  const renderRatingStatDigits = (value) =>
    formatRatingStatCount(value)
      .split('')
      .map((digit, idx) => (
        <span key={`${value}-${idx}-${digit}`} className="players-rating-digit">
          {digit}
        </span>
      ))
  const topMoveRows = Array.isArray(topMovesData?.rows) ? topMovesData.rows : []
  const topMoveMax = Math.max(1, ...topMoveRows.map((row) => Number(row?.times_played || 0)))

  const openRatingModal = () => {
    if (modeChart) setRatingModalOpen(true)
  }

  const closeRatingModal = () => setRatingModalOpen(false)

  const openingViews = OPENING_RESULTS.map((resultFilter) => {
    const state = openingStates[resultFilter]
    const rows = Array.isArray(state?.rows) ? state.rows : []
    const activeTop = state?.activeTop
    const activeRow = rows.find((row) => Number(row.top) === Number(activeTop)) || rows[0] || null
    const totalHalfMoves = activeRow?.half_moves?.length || 0
    const activePly = Math.max(0, Math.min(Number(state?.activePly || 0), totalHalfMoves))
    const boardFen = activeRow ? fenFromHalfMoves(activeRow.half_moves || [], activePly) : new Chess().fen()
    return {
      resultFilter,
      state,
      rows,
      activeRow,
      activeTop: activeRow ? Number(activeRow.top || 1) : null,
      activePly,
      totalHalfMoves,
      boardFen
    }
  })

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="games-main">
        {message ? <p className="result-line">{message}</p> : null}
        {error ? <p className="result-line">{error}</p> : null}

        <section className="games-mode-detail players-profile-section">
          <div className="section-head">
            <h2 className="games-action-title">{profileTitle}</h2>
          </div>
          <div className="players-profile-layout">
            <div className="players-profile-avatar-box">
              {profileAvatar ? (
                <img className="players-profile-avatar" src={profileAvatar} alt={`${profileTitle} avatar`} />
              ) : (
                <span className="players-profile-avatar-fallback">{profileInitial}</span>
              )}
            </div>
            {profileRows.length ? (
              <>
                <div className="profile-grid">
                  {profileRows.map(([label, value]) => (
                    <div key={label} className="profile-item">
                      <span>{label}</span>
                      <strong>{String(value)}</strong>
                    </div>
                  ))}
                </div>
                <div className="players-hours-text">
                  <p>total hours played: {Math.round(Number(playerHours?.total_hours || 0))}</p>
                  <p>
                    bullet: {Math.round(Number(playerHours?.bullet_hours || 0))} | blitz: {Math.round(Number(playerHours?.blitz_hours || 0))} | rapid: {Math.round(Number(playerHours?.rapid_hours || 0))}
                  </p>
                </div>
              </>
            ) : (
              <p className="result-line">No profile loaded.</p>
            )}
          </div>
        </section>

        <section className="games-time-wrap">
          <h2 className="games-hero-badge time-control-badge badge-hover">Time Control</h2>
          <section className="games-time-controls" aria-label="Player time controls">
            <div className="games-generalities-grid games-time-controls-grid">
              <button
                className={`games-generality-card games-mode-button games-link-card ${selectedMode === 'bullet' ? 'active' : ''}`}
                type="button"
                onClick={() => handleModeSelect('bullet')}
                disabled={!activePlayer}
              >
                <h3>bullet</h3>
                <span className="time-control-icon bullet-cartridge-icon" aria-hidden="true">
                  <span className="bullet-cartridge-shape" />
                </span>
                <p>{modeCounts.bullet.toLocaleString('en-US')}</p>
              </button>
              <button
                className={`games-generality-card games-mode-button games-link-card ${selectedMode === 'blitz' ? 'active' : ''}`}
                type="button"
                onClick={() => handleModeSelect('blitz')}
                disabled={!activePlayer}
              >
                <h3>blitz</h3>
                <span className="time-control-icon time-control-icon-blitz" aria-hidden="true">⚡</span>
                <p>{modeCounts.blitz.toLocaleString('en-US')}</p>
              </button>
              <button
                className={`games-generality-card games-mode-button games-link-card ${selectedMode === 'rapid' ? 'active' : ''}`}
                type="button"
                onClick={() => handleModeSelect('rapid')}
                disabled={!activePlayer}
              >
                <h3>rapid</h3>
                <span className="time-control-icon time-control-icon-rapid" aria-hidden="true">⏱</span>
                <p>{modeCounts.rapid.toLocaleString('en-US')}</p>
              </button>
            </div>
          </section>
        </section>

        {selectedMode ? (
          <section className="games-mode-detail players-panel">
            <div className="section-head players-rating-head">
              <h2 className="games-action-title">{ratingTitle}</h2>
              <span className="stat-chip players-rating-stats-chip" style={{ background: winsRateColor }}>
                <span className="players-rating-stats-label">wins:</span>
                <span className="players-rating-stats-value">{renderRatingStatDigits(ratingStats.wins)}</span>
                <span className="players-rating-stats-label">losses:</span>
                <span className="players-rating-stats-value">{renderRatingStatDigits(ratingStats.losses)}</span>
                <span className="players-rating-stats-label">draws:</span>
                <span className="players-rating-stats-value">{renderRatingStatDigits(ratingStats.draws)}</span>
              </span>
            </div>

            {ratingLoading ? <p className="result-line">Loading chart...</p> : null}
            {ratingError ? <p className="result-line">{ratingError}</p> : null}
            {!ratingLoading && !ratingError && modeChart ? (
              <div className="rating-clickable" onClick={openRatingModal}>
                <ScatterChart chart={modeChart} />
              </div>
            ) : null}

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
        ) : null}
        {ratingModalOpen ? (
          <div className="rating-modal-backdrop" onClick={closeRatingModal}>
            <div className="rating-modal" role="dialog" aria-label={`${selectedMode} rating enlarged`} onClick={(e) => e.stopPropagation()}>
              <ScatterChart chart={modeChart} />
            </div>
          </div>
        ) : null}

        {selectedMode ? (
          <section className="games-mode-detail" aria-label={`${selectedMode} top moves`}>
            <div className="section-head mode-detail-head">
              <h2 className="games-action-title">{selectedMode} top moves</h2>
              <div className="mode-color-switch">
                <button
                  className={`mode-color-btn mode-color-white ${moveColor === 'white' ? 'active' : ''}`}
                  type="button"
                  onClick={() => handleMoveColorChange('white')}
                >
                  white
                </button>
                <button
                  className={`mode-color-btn mode-color-black ${moveColor === 'black' ? 'active' : ''}`}
                  type="button"
                  onClick={() => handleMoveColorChange('black')}
                >
                  black
                </button>
              </div>
            </div>
            <div className="mode-moves-table">
              {topMovesLoading ? <p className="result-line">Loading top moves...</p> : null}
              {topMovesError ? <p className="result-line">{topMovesError}</p> : null}
              {!topMovesLoading && !topMovesError ? (
                <>
                  <div className="mode-moves-chart">
                    {topMoveRows.map((row) => {
                      const y = Number(row?.times_played || 0)
                      const percent = Math.max(2, Math.round((y / topMoveMax) * 100))
                      return (
                        <div key={`player-mode-${selectedMode}-move-${row.move_number}`} className="mode-moves-chart-col">
                          <span className="mode-moves-chart-value">{formatNumber(y)}</span>
                          <div className="mode-moves-chart-bar-wrap">
                            <div className="mode-moves-chart-bar" style={{ height: `${percent}%` }} />
                          </div>
                          <span className="mode-moves-chart-x">#{row.move_number}</span>
                          <span className="mode-moves-chart-tick">{row.move}</span>
                        </div>
                      )
                    })}
                  </div>
                  <div className="mode-moves-pagination">
                    <button className="btn btn-secondary" type="button" onClick={handleTopMovesPrevious} disabled={topMovesLoading || Number(topMovesData?.page || 1) <= 1}>
                      previous 5
                    </button>
                    <span>page {topMovesData?.page || 1} / {topMovesData?.total_pages || 0}</span>
                    <button className="btn btn-secondary" type="button" onClick={handleTopMovesNext} disabled={topMovesLoading || !topMovesData?.total_pages || Number(topMovesData?.page || 1) >= Number(topMovesData?.total_pages || 0)}>
                      next 5
                    </button>
                  </div>
                </>
              ) : null}
            </div>
          </section>
        ) : null}

        {selectedMode ? (
          <>
            {openingViews.map((view) => (
              <section key={`openings-${view.resultFilter}`} className="games-openings-detail" aria-label={`${selectedMode} openings when ${view.resultFilter}`}>
                <div className="section-head">
                  <h2 className="games-action-title">{selectedMode} openings when {view.resultFilter}</h2>
                </div>
                <div className="openings-layout">
                  <div className="openings-board-row">
                    <div className="react-board-wrap" style={{ width: '300px' }}>
                      <Chessboard
                        id={`player-openings-board-${view.resultFilter}`}
                        position={view.boardFen}
                        arePiecesDraggable={false}
                        boardWidth={286}
                      />
                    </div>
                  </div>
                  <div className="openings-query-row">
                    <label htmlFor={`player-openings-n-moves-${view.resultFilter}`}>moves</label>
                    <select
                      id={`player-openings-n-moves-${view.resultFilter}`}
                      className="text-input openings-n-moves-select"
                      value={view.state?.nMoves || String(OPENING_N_MOVES_DEFAULT)}
                      onChange={(event) => handleOpeningMovesInput(view.resultFilter, event.target.value)}
                    >
                      {Array.from({ length: OPENING_N_MOVES_MAX - OPENING_N_MOVES_MIN + 1 }, (_, idx) => {
                        const val = OPENING_N_MOVES_MIN + idx
                        return (
                          <option key={`player-n-moves-${view.resultFilter}-${val}`} value={val}>
                            {val}
                          </option>
                        )
                      })}
                    </select>
                    <button className="btn btn-secondary" type="button" onClick={() => handleLoadOpenings(view.resultFilter)} disabled={view.state?.loading}>
                      {view.state?.loading ? 'Loading...' : 'go'}
                    </button>
                  </div>
                  <div className="mode-moves-table openings-list-table">
                    {view.state?.loading ? <p className="result-line">Loading openings...</p> : null}
                    {view.state?.error ? <p className="result-line">{view.state.error}</p> : null}
                    {!view.state?.requested && !view.state?.loading ? <p className="result-line">Select moves and click go.</p> : null}
                    {view.state?.requested && !view.state?.loading && !view.state?.error ? (
                      view.rows.length ? (
                        view.rows.map((row) => {
                          const totalHalfMoves = row.half_moves.length
                          const currentPly = view.activeTop === row.top ? view.activePly : 1
                          const currentMove = currentPly > 0 ? row.half_moves[currentPly - 1] : '-'
                          return (
                            <div key={`player-opening-${view.resultFilter}-${row.top}`} className={`mode-moves-row opening-step-row ${view.activeTop === row.top ? 'active' : ''}`}>
                              <button className={`opening-top-btn ${view.activeTop === row.top ? 'active' : ''}`} type="button" onClick={() => handleSelectOpeningTop(view.resultFilter, row.top)}>
                                top_{row.top}
                              </button>
                              <button className="btn btn-secondary opening-arrow-btn" type="button" onClick={() => handleOpeningStep(view.resultFilter, 'prev')} disabled={view.activeTop !== row.top || currentPly <= 1}>
                                {'<'}
                              </button>
                              <span className="opening-move-text">{currentMove}</span>
                              <button className="btn btn-secondary opening-arrow-btn" type="button" onClick={() => handleOpeningStep(view.resultFilter, 'next')} disabled={view.activeTop !== row.top || currentPly <= 0 || currentPly >= totalHalfMoves}>
                                {'>'}
                              </button>
                            </div>
                          )
                        })
                      ) : (
                        <p className="result-line">No openings found.</p>
                      )
                    ) : null}
                  </div>
                </div>
              </section>
            ))}
          </>
        ) : null}

        {selectedMode ? (
          <section className="games-mode-detail" aria-label={`${selectedMode} results`}>
            <div className="section-head">
              <h2 className="games-action-title">{selectedMode} results</h2>
            </div>
            <div className="mode-moves-table analytics-table-wrap">
              {resultsLoading ? <p className="result-line">Loading results...</p> : null}
              {resultsError ? <p className="result-line">{resultsError}</p> : null}
              {!resultsLoading && !resultsError ? (
                <div className="wl-bar">
                  <div className="wl-row">
                    <span className="wl-label">Wins</span>
                    <div className="wl-track">
                      <div className="wl-fill wl-fill-win" style={{ width: `${((resultsData?.wins || 0) / (resultsData?.total_games || 1)) * 100}%` }} />
                    </div>
                    <span className="wl-value">{formatNumber(resultsData?.wins || 0)}</span>
                  </div>
                  <div className="wl-row">
                    <span className="wl-label">Losses</span>
                    <div className="wl-track">
                      <div className="wl-fill wl-fill-loss" style={{ width: `${((resultsData?.losses || 0) / (resultsData?.total_games || 1)) * 100}%` }} />
                    </div>
                    <span className="wl-value">{formatNumber(resultsData?.losses || 0)}</span>
                  </div>
                  <div className="wl-row">
                    <span className="wl-label">Draws</span>
                    <div className="wl-track">
                      <div className="wl-fill wl-fill-draw" style={{ width: `${((resultsData?.draws || 0) / (resultsData?.total_games || 1)) * 100}%` }} />
                    </div>
                    <span className="wl-value">{formatNumber(resultsData?.draws || 0)}</span>
                  </div>
                </div>
              ) : null}
            </div>
          </section>
        ) : null}

        {selectedMode ? (
          <section className="games-mode-detail" aria-label={`${selectedMode} lengths`}>
            <div className="section-head">
              <h2 className="games-action-title">{selectedMode} lengths</h2>
            </div>
            <div className="mode-moves-table analytics-table-wrap">
              {lengthsLoading ? <p className="result-line">Loading lengths...</p> : null}
              {lengthsError ? <p className="result-line">{lengthsError}</p> : null}
              {!lengthsLoading && !lengthsError ? (
                <>
                  <div className="length-stats-grid">
                    <span className="opening-meta-chip">games: {formatNumber(lengthsData?.total_games || 0)}</span>
                    <span className="opening-meta-chip length-meta-right">avg moves: {formatNumber(Math.round(Number(lengthsData?.summary?.avg_n_moves || 0)))}</span>
                  </div>
                  <div className="length-chart-spacer">
                    <CompactBarChart labels={lengthsData?.n_moves_hist?.x || []} values={lengthsData?.n_moves_hist?.y || []} />
                  </div>
                  <p className="result-line duration-label">moves per game</p>
                  <CompactBarChart labels={lengthsData?.time_elapsed_hist?.x || []} values={lengthsData?.time_elapsed_hist?.y || []} />
                  <p className="result-line duration-label">Minutes</p>
                </>
              ) : null}
            </div>
          </section>
        ) : null}

        {selectedMode ? (
          <section className="games-mode-detail" aria-label={`${selectedMode} activity trend`}>
            <div className="section-head">
              <h2 className="games-action-title">{selectedMode} activity trend</h2>
            </div>
            <div className="mode-moves-table analytics-table-wrap">
              {activityLoading ? <p className="result-line">Loading activity trend...</p> : null}
              {activityError ? <p className="result-line">{activityError}</p> : null}
              {!activityLoading && !activityError ? (
                <div className="activity-heat-stack">
                  <HeatStrip title="games by month" labels={activityData?.month_heat?.labels || []} values={activityData?.month_heat?.values || []} />
                  <HeatStrip title="games by weekday" labels={activityData?.weekday_heat?.labels || []} values={activityData?.weekday_heat?.values || []} />
                  <HeatStrip title="games by hour" labels={activityData?.hour_heat?.labels || []} values={activityData?.hour_heat?.values || []} singleRow />
                </div>
              ) : null}
            </div>
          </section>
        ) : null}

        <section className="games-mode-detail players-panel">
          <div className="section-head">
            <h2 className="games-action-title">last 20 games</h2>
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
    </div>
  )
}

export default Players
