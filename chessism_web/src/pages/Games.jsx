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
const CLUSTER_KEYS = ['cluster_1', 'cluster_2', 'cluster_3']
const DEFAULT_CLUSTER_KEY = 'cluster_2'
const CLUSTER_TO_JENKS_KEY = {
  cluster_1: 'jenks_1',
  cluster_2: 'jenks_2',
  cluster_3: 'jenks_3'
}
const CLUSTER_COLORS = {
  cluster_1: '#f07167',
  cluster_2: '#f4c057',
  cluster_3: '#3fd089'
}
const OPENINGS_BOARD_WIDTH = 286
const TIME_CONTROLS_CACHE_VERSION = 'v11'
const MOVE_WORDS = [
  'one',
  'two',
  'three',
  'four',
  'five',
  'six',
  'seven',
  'eight',
  'nine',
  'ten',
  'eleven',
  'twelve',
  'thirteen',
  'fourteen',
  'fifteen',
  'sixteen',
  'seventeen',
  'eighteen',
  'nineteen',
  'twenty'
]

const formatNumber = (value) => {
  const num = Number(value ?? 0)
  if (Number.isNaN(num)) return '0'
  return num.toLocaleString('en-US')
}

const readCached = (key) => {
  try {
    if (typeof window === 'undefined') return null
    const raw = window.localStorage.getItem(key)
    if (!raw) return null
    return JSON.parse(raw)
  } catch {
    return null
  }
}

const writeCached = (key, value) => {
  try {
    if (typeof window === 'undefined') return
    window.localStorage.setItem(key, JSON.stringify(value))
  } catch {
    // Ignore cache write failures (quota/private mode)
  }
}

async function fetchDatabaseGeneralities() {
  const candidates = [
    `${API_BASE_URL}/games/_database_generalities`,
    `${API_BASE_URL}/games/database/generalities`,
    `${API_BASE_URL}/games/generalities`
  ]

  for (const url of candidates) {
    const response = await fetch(url)
    const payload = await response.json().catch(() => ({}))
    if (response.ok) {
      return payload
    }
    if (response.status !== 404) {
      throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
    }
  }

  throw new Error('Not Found')
}

async function fetchPlayerGameCount(playerName) {
  const response = await fetch(`${API_BASE_URL}/games/${encodeURIComponent(playerName)}/count`)
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchRecentGamesPage(playerName, page, pageSize = 10) {
  const response = await fetch(
    `${API_BASE_URL}/games/${encodeURIComponent(playerName)}/recent?page=${page}&page_size=${pageSize}`
  )
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

async function fetchTimeControlCounts() {
  const response = await fetch(`${API_BASE_URL}/games/time_controls`)
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchTimeControlTopMoves(
  mode,
  moveColor = 'white',
  minRating = null,
  maxRating = null,
  page = 1,
  pageSize = MOVES_PAGE_SIZE,
  maxMove = TOP_MOVE_LIMIT
) {
  const params = new URLSearchParams({
    player_color: moveColor,
    page: String(page),
    page_size: String(pageSize),
    max_move: String(maxMove)
  })
  if (Number.isFinite(minRating) && Number.isFinite(maxRating)) {
    params.set('min_rating', String(minRating))
    params.set('max_rating', String(maxRating))
  }
  const response = await fetch(
    `${API_BASE_URL}/games/time_controls/${encodeURIComponent(mode)}/top_moves?${params.toString()}`
  )
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchTimeControlTopOpenings(
  mode,
  minRating = null,
  maxRating = null,
  nMoves = OPENING_N_MOVES_DEFAULT,
  page = 1,
  pageSize = OPENINGS_PAGE_SIZE
) {
  const safeNMoves = Math.max(
    OPENING_N_MOVES_MIN,
    Math.min(OPENING_N_MOVES_MAX, Number.parseInt(nMoves, 10) || OPENING_N_MOVES_DEFAULT)
  )
  const params = new URLSearchParams({
    page: String(page),
    page_size: String(pageSize),
    n_moves: String(safeNMoves)
  })
  if (Number.isFinite(minRating) && Number.isFinite(maxRating)) {
    params.set('min_rating', String(minRating))
    params.set('max_rating', String(maxRating))
  }
  const response = await fetch(
    `${API_BASE_URL}/games/time_controls/${encodeURIComponent(mode)}/top_openings?${params.toString()}`
  )
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchTimeControlRatingChart(mode) {
  const response = await fetch(
    `${API_BASE_URL}/games/rating_time_control_chart?time_control=${encodeURIComponent(mode)}`
  )
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

const moveWordToNumber = (value) => {
  const idx = MOVE_WORDS.indexOf(String(value || '').toLowerCase())
  return idx >= 0 ? idx + 1 : Number.NaN
}

const moveNumberToKey = (moveNumber) => {
  const index = Number(moveNumber || 0)
  if (index >= 1 && index <= MOVE_WORDS.length) {
    return `move_${MOVE_WORDS[index - 1]}`
  }
  return `move_${index}`
}

const buildMovesMapFromHalfMoves = (halfMoves) => {
  const out = {}
  const totalFullMoves = Math.ceil((halfMoves || []).length / 2)
  for (let moveIdx = 1; moveIdx <= totalFullMoves; moveIdx += 1) {
    const white = halfMoves[(moveIdx - 1) * 2] || '--'
    const black = halfMoves[(moveIdx - 1) * 2 + 1] || '--'
    out[moveNumberToKey(moveIdx)] = `${white},${black}`
  }
  return out
}

const halfMovesFromMovesMap = (movesMap) => {
  if (!movesMap || typeof movesMap !== 'object') return []
  return Object.entries(movesMap)
    .sort((a, b) => {
      const aKey = String(a[0] || '')
      const bKey = String(b[0] || '')
      const aRaw = aKey.startsWith('move_') ? aKey.slice(5) : aKey
      const bRaw = bKey.startsWith('move_') ? bKey.slice(5) : bKey
      const aNum = Number.parseInt(aRaw, 10) || moveWordToNumber(aRaw)
      const bNum = Number.parseInt(bRaw, 10) || moveWordToNumber(bRaw)
      return aNum - bNum
    })
    .flatMap(([, pairValue]) => {
      const [white, black] = String(pairValue || '')
        .split(',')
        .map((part) => part.trim())
      return [white, black].filter((move) => move && move !== '--')
    })
}

const normalizeOpeningRows = (payload) => {
  if (Array.isArray(payload?.rows) && payload.rows.length > 0) {
    return payload.rows.map((row, idx) => {
      const top = Number(row?.top || idx + 1)
      const halfMovesFromArray = Array.isArray(row?.half_moves)
        ? row.half_moves.map((token) => String(token || '').trim()).filter(Boolean)
        : []
      const halfMovesFromString = String(row?.opening || '')
        .trim()
        .split(/\s+/)
        .filter(Boolean)
      const movesMap =
        row?.moves && typeof row.moves === 'object'
          ? row.moves
          : buildMovesMapFromHalfMoves(halfMovesFromArray.length > 0 ? halfMovesFromArray : halfMovesFromString)
      const halfMovesFromMap = halfMovesFromMovesMap(movesMap)
      const halfMoves = halfMovesFromArray.length > 0
        ? halfMovesFromArray
        : halfMovesFromString.length > 0
          ? halfMovesFromString
          : halfMovesFromMap
      return {
        top,
        key: String(row?.key || `most_common_opening_for_this_time_control_${top}`),
        moves: movesMap,
        half_moves: halfMoves,
        times_played: Number(row?.times_played || 0),
        n_games_for_this_opening: Number(row?.n_games_for_this_opening || row?.times_played || 0),
        mean_rating_for_this_opening: Number(row?.mean_rating_for_this_opening || 0)
      }
    })
  }

  if (payload?.openings && typeof payload.openings === 'object') {
    return Object.entries(payload.openings).map(([key, value], idx) => {
      const top = idx + 1
      const raw = value && typeof value === 'object' ? value : {}
      const meanRating = Number(raw.mean_rating_for_this_opening || 0)
      const nGames = Number(raw.n_games_for_this_opening || 0)
      const movesMap = Object.entries(raw).reduce((acc, [moveKey, moveValue]) => {
        if (moveKey === 'mean_rating_for_this_opening') return acc
        if (moveKey === 'n_games_for_this_opening') return acc
        acc[moveKey] = moveValue
        return acc
      }, {})
      return {
        top,
        key,
        moves: movesMap,
        half_moves: halfMovesFromMovesMap(movesMap),
        times_played: 0,
        n_games_for_this_opening: nGames,
        mean_rating_for_this_opening: meanRating
      }
    })
  }

  return []
}

const fenAfterHalfMoves = (halfMoves, targetPly) => {
  const game = new Chess()
  const safePly = Math.max(0, Math.min(Number(targetPly || 0), (halfMoves || []).length))
  for (let idx = 0; idx < safePly; idx += 1) {
    const moved = game.move(halfMoves[idx])
    if (!moved) break
  }
  return game.fen()
}

const getClusterRangeFromBins = (bins, clusterKey) => {
  const jenksKey = CLUSTER_TO_JENKS_KEY[clusterKey]
  const range = bins && jenksKey ? bins[jenksKey] : null
  if (!range || typeof range !== 'object') return null
  const minRating = Number(range.min_rating)
  const maxRating = Number(range.max_rating)
  if (!Number.isFinite(minRating) || !Number.isFinite(maxRating)) return null
  if (minRating <= 0 && maxRating <= 0) return null
  return { minRating, maxRating }
}

const hasAnyValidJenksBin = (bins) => {
  if (!bins || typeof bins !== 'object') return false
  return ['jenks_1', 'jenks_2', 'jenks_3'].some((jenksKey) => {
    const range = bins[jenksKey]
    if (!range || typeof range !== 'object') return false
    const minRating = Number(range.min_rating)
    const maxRating = Number(range.max_rating)
    return Number.isFinite(minRating) && Number.isFinite(maxRating) && !(minRating <= 0 && maxRating <= 0)
  })
}

const getPointCluster = (rating, bins) => {
  const value = Number(rating)
  if (!Number.isFinite(value) || !bins) return DEFAULT_CLUSTER_KEY
  for (const clusterKey of CLUSTER_KEYS) {
    const range = getClusterRangeFromBins(bins, clusterKey)
    if (!range) continue
    if (value >= range.minRating && value <= range.maxRating) {
      return clusterKey
    }
  }
  return DEFAULT_CLUSTER_KEY
}

function TimeControlRatingsScatterChart({ mode, chart }) {
  const xValues = Array.isArray(chart?.x) ? chart.x.map((value) => Number(value || 0)) : []
  const yValues = Array.isArray(chart?.y) ? chart.y.map((value) => Number(value || 0)) : []
  const bins = chart?.bins && typeof chart.bins === 'object' ? chart.bins : {}

  if (!xValues.length || xValues.length !== yValues.length) {
    return <p className="result-line">No ratings data for this time control.</p>
  }

  const width = 820
  const height = 280
  const padLeft = 44
  const padRight = 18
  const padTop = 16
  const padBottom = 30
  const minX = Math.min(...xValues)
  const maxX = Math.max(...xValues)
  const minY = 0
  const maxY = Math.max(...yValues, 1)
  const xRange = Math.max(1, maxX - minX)
  const yRange = Math.max(1, maxY - minY)
  const plotWidth = width - padLeft - padRight
  const plotHeight = height - padTop - padBottom

  const points = xValues.map((xValue, idx) => {
    const yValue = yValues[idx]
    const x = padLeft + ((xValue - minX) / xRange) * plotWidth
    const y = padTop + ((maxY - yValue) / yRange) * plotHeight
    const clusterKey = getPointCluster(xValue, bins)
    return { x, y, xValue, yValue, clusterKey }
  })

  const xTicks = Array.from({ length: 5 }, (_, idx) => Math.round(minX + (idx * (xRange / 4))))
  const yTicks = Array.from({ length: 5 }, (_, idx) => Math.round(minY + (idx * (yRange / 4))))

  return (
    <div className="rating-chart-shell">
      <h3>raitings for: {mode}</h3>
      <div className="rating-cluster-legend">
        {CLUSTER_KEYS.map((clusterKey) => (
          <span key={`legend-${clusterKey}`} className="rating-cluster-item">
            <span
              className="rating-cluster-dot"
              style={{ background: CLUSTER_COLORS[clusterKey] || '#f4c057' }}
              aria-hidden="true"
            />
            {clusterKey}
          </span>
        ))}
      </div>
      <svg viewBox={`0 0 ${width} ${height}`} className="rating-chart" role="img" aria-label={`ratings for ${mode}`}>
        <line x1={padLeft} y1={padTop} x2={padLeft} y2={height - padBottom} className="axis-line" />
        <line x1={padLeft} y1={height - padBottom} x2={width - padRight} y2={height - padBottom} className="axis-line" />
        {yTicks.map((tick, index) => {
          const y = padTop + ((maxY - tick) / yRange) * plotHeight
          return (
            <g key={`tc-y-tick-${tick}-${index}`}>
              <line x1={padLeft - 5} y1={y} x2={padLeft} y2={y} className="axis-line" />
              <text x={padLeft - 9} y={y + 4} className="axis-tick-label" textAnchor="end">
                {tick}
              </text>
            </g>
          )
        })}
        {xTicks.map((tick, index) => {
          const x = padLeft + ((tick - minX) / xRange) * plotWidth
          return (
            <g key={`tc-x-tick-${tick}-${index}`}>
              <line x1={x} y1={height - padBottom} x2={x} y2={height - padBottom + 5} className="axis-line" />
              <text x={x} y={height - padBottom + 18} className="axis-tick-label" textAnchor="middle">
                {tick}
              </text>
            </g>
          )
        })}
        {points.map((point, index) => (
          <circle
            key={`tc-point-${point.xValue}-${point.yValue}-${index}`}
            cx={point.x}
            cy={point.y}
            r="3.2"
            className="scatter-point"
            style={{ fill: CLUSTER_COLORS[point.clusterKey] || '#f4c057' }}
          />
        ))}
      </svg>
    </div>
  )
}

function Games() {
  const [countPlayer, setCountPlayer] = useState('')
  const [countResult, setCountResult] = useState('')
  const [selectedPlayer, setSelectedPlayer] = useState('')
  const [recentPage, setRecentPage] = useState(1)
  const [recentData, setRecentData] = useState(null)
  const [recentError, setRecentError] = useState('')
  const [summaryData, setSummaryData] = useState(null)
  const [summaryError, setSummaryError] = useState('')
  const [summaryLoading, setSummaryLoading] = useState(false)

  const [countLoading, setCountLoading] = useState(false)
  const [recentLoading, setRecentLoading] = useState(false)
  const [generalitiesLoading, setGeneralitiesLoading] = useState(true)
  const [generalitiesError, setGeneralitiesError] = useState('')
  const [generalities, setGeneralities] = useState(null)
  const [timeControlCounts, setTimeControlCounts] = useState({ bullet: 0, blitz: 0, rapid: 0 })
  const [timeControlsLoading, setTimeControlsLoading] = useState(true)
  const [timeControlsError, setTimeControlsError] = useState('')
  const [selectedMode, setSelectedMode] = useState('')
  const [selectedMoveColor, setSelectedMoveColor] = useState('white')
  const [selectedMovesCluster, setSelectedMovesCluster] = useState(DEFAULT_CLUSTER_KEY)
  const [selectedOpeningsCluster, setSelectedOpeningsCluster] = useState(DEFAULT_CLUSTER_KEY)
  const [modeRows, setModeRows] = useState([])
  const [modePage, setModePage] = useState(1)
  const [modeTotalPages, setModeTotalPages] = useState(0)
  const [modeLoading, setModeLoading] = useState(false)
  const [modeError, setModeError] = useState('')
  const [ratingChart, setRatingChart] = useState(null)
  const [ratingLoading, setRatingLoading] = useState(false)
  const [ratingError, setRatingError] = useState('')
  const [openingRows, setOpeningRows] = useState([])
  const [openingLoading, setOpeningLoading] = useState(false)
  const [openingError, setOpeningError] = useState('')
  const [openingNMovesInput, setOpeningNMovesInput] = useState('')
  const [openingRequested, setOpeningRequested] = useState(false)
  const [openingNMoves, setOpeningNMoves] = useState(OPENING_N_MOVES_DEFAULT)
  const [boardFen, setBoardFen] = useState('start')
  const [openingPlyByTop, setOpeningPlyByTop] = useState({})
  const [openingMoveAnimByTop, setOpeningMoveAnimByTop] = useState({})
  const [activeOpeningTop, setActiveOpeningTop] = useState(null)

  useEffect(() => {
    let cancelled = false

    const loadDashboardData = async () => {
      setGeneralitiesLoading(true)
      setGeneralitiesError('')
      setTimeControlsLoading(true)
      setTimeControlsError('')
      const [generalitiesResult, timeControlsResult] = await Promise.allSettled([
        fetchDatabaseGeneralities(),
        fetchTimeControlCounts()
      ])

      if (cancelled) return

      if (generalitiesResult.status === 'fulfilled') {
        setGeneralities(generalitiesResult.value)
      } else {
        setGeneralities(null)
        setGeneralitiesError(generalitiesResult.reason instanceof Error ? generalitiesResult.reason.message : 'Request failed.')
      }

      if (timeControlsResult.status === 'fulfilled') {
        setTimeControlCounts({
          bullet: Number(timeControlsResult.value?.bullet || 0),
          blitz: Number(timeControlsResult.value?.blitz || 0),
          rapid: Number(timeControlsResult.value?.rapid || 0)
        })
      } else {
        setTimeControlCounts({ bullet: 0, blitz: 0, rapid: 0 })
        setTimeControlsError(timeControlsResult.reason instanceof Error ? timeControlsResult.reason.message : 'Request failed.')
      }

      setGeneralitiesLoading(false)
      setTimeControlsLoading(false)
    }

    loadDashboardData()

    return () => {
      cancelled = true
    }
  }, [])

  const loadModeMoves = async (mode, page, moveColor = 'white', ratingRange = null) => {
    const minRating = Number.isFinite(ratingRange?.minRating) ? Number(ratingRange.minRating) : ''
    const maxRating = Number.isFinite(ratingRange?.maxRating) ? Number(ratingRange.maxRating) : ''
    const cacheKey = `games:time_controls:${TIME_CONTROLS_CACHE_VERSION}:moves:${mode}:color:${moveColor}:page:${page}:size:${MOVES_PAGE_SIZE}:max:${TOP_MOVE_LIMIT}:min:${minRating}:max:${maxRating}`
    const cached = readCached(cacheKey)
    if (cached) {
      setModeLoading(false)
      setModeRows(Array.isArray(cached.rows) ? cached.rows : [])
      setModePage(Number(cached.page || page))
      setModeTotalPages(Number(cached.total_pages || 0))
      setModeError('')
      return
    }

    setModeLoading(true)
    setModeError('')
    try {
      const payload = await fetchTimeControlTopMoves(
        mode,
        moveColor,
        Number.isFinite(ratingRange?.minRating) ? ratingRange.minRating : null,
        Number.isFinite(ratingRange?.maxRating) ? ratingRange.maxRating : null,
        page,
        MOVES_PAGE_SIZE,
        TOP_MOVE_LIMIT
      )
      const normalized = {
        mode,
        move_color: moveColor,
        min_rating: payload?.min_rating ?? null,
        max_rating: payload?.max_rating ?? null,
        page: Number(payload?.page || page),
        total_pages: Number(payload?.total_pages || 0),
        rows: Array.isArray(payload?.rows) ? payload.rows : []
      }
      writeCached(cacheKey, normalized)
      setModeRows(normalized.rows)
      setModePage(normalized.page)
      setModeTotalPages(normalized.total_pages)
    } catch (error) {
      setModeRows([])
      setModeTotalPages(0)
      setModeError(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      setModeLoading(false)
    }
  }

  const loadModeRatings = async (mode) => {
    const cacheKey = `games:time_controls:${TIME_CONTROLS_CACHE_VERSION}:ratings:${mode}`
    const cached = readCached(cacheKey)
    if (cached && hasAnyValidJenksBin(cached?.bins)) {
      setRatingLoading(false)
      setRatingError('')
      setRatingChart(cached)
      return cached
    }

    setRatingLoading(true)
    setRatingError('')
    try {
      const payload = await fetchTimeControlRatingChart(mode)
      const normalizedBins = {}
      for (const jenksKey of ['jenks_1', 'jenks_2', 'jenks_3']) {
        const rawRange = payload?.bins?.[jenksKey]
        const minRating = Number(rawRange?.min_rating)
        const maxRating = Number(rawRange?.max_rating)
        if (Number.isFinite(minRating) && Number.isFinite(maxRating)) {
          normalizedBins[jenksKey] = {
            min_rating: Math.min(minRating, maxRating),
            max_rating: Math.max(minRating, maxRating)
          }
        }
      }
      const normalized = {
        x: Array.isArray(payload?.x) ? payload.x : [],
        y: Array.isArray(payload?.y) ? payload.y : [],
        time_control: String(payload?.time_control || mode),
        bins: normalizedBins
      }
      writeCached(cacheKey, normalized)
      setRatingChart(normalized)
      return normalized
    } catch (error) {
      setRatingChart(null)
      setRatingError(error instanceof Error ? error.message : 'Request failed.')
      return null
    } finally {
      setRatingLoading(false)
    }
  }

  const loadModeOpenings = async (mode, nMoves, ratingRange = null) => {
    const safeNMoves = Math.max(
      OPENING_N_MOVES_MIN,
      Math.min(OPENING_N_MOVES_MAX, Number.parseInt(nMoves, 10) || OPENING_N_MOVES_DEFAULT)
    )
    const minRating = Number.isFinite(ratingRange?.minRating) ? Number(ratingRange.minRating) : ''
    const maxRating = Number.isFinite(ratingRange?.maxRating) ? Number(ratingRange.maxRating) : ''
    const cacheKey = `games:time_controls:${TIME_CONTROLS_CACHE_VERSION}:openings:${mode}:page:1:size:${OPENINGS_PAGE_SIZE}:n_moves:${safeNMoves}:min:${minRating}:max:${maxRating}`
    setOpeningRequested(true)
    setOpeningNMoves(safeNMoves)
    const cached = readCached(cacheKey)
    if (cached) {
      const normalizedRows = normalizeOpeningRows(cached)
      setOpeningLoading(false)
      setOpeningRows(normalizedRows)
      setOpeningPlyByTop(
        normalizedRows.reduce((acc, row) => {
          acc[row.top] = row.half_moves.length > 0 ? 1 : 0
          return acc
        }, {})
      )
      setOpeningMoveAnimByTop({})
      setOpeningError('')
      return
    }

    setOpeningLoading(true)
    setOpeningError('')
    try {
      const payload = await fetchTimeControlTopOpenings(
        mode,
        Number.isFinite(ratingRange?.minRating) ? ratingRange.minRating : null,
        Number.isFinite(ratingRange?.maxRating) ? ratingRange.maxRating : null,
        safeNMoves,
        1,
        OPENINGS_PAGE_SIZE
      )
      const normalizedRows = normalizeOpeningRows(payload)
      const normalized = {
        mode,
        min_rating: payload?.min_rating ?? null,
        max_rating: payload?.max_rating ?? null,
        n_moves: safeNMoves,
        rows: normalizedRows
      }
      writeCached(cacheKey, normalized)
      setOpeningRows(normalizedRows)
      setOpeningPlyByTop(
        normalizedRows.reduce((acc, row) => {
          acc[row.top] = row.half_moves.length > 0 ? 1 : 0
          return acc
        }, {})
      )
      setOpeningMoveAnimByTop({})
    } catch (error) {
      setOpeningRows([])
      setOpeningPlyByTop({})
      setOpeningMoveAnimByTop({})
      setOpeningError(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      setOpeningLoading(false)
    }
  }

  const handleSelectMode = async (mode) => {
    const defaultCluster = DEFAULT_CLUSTER_KEY
    setSelectedMode(mode)
    setSelectedMoveColor('white')
    setSelectedMovesCluster(defaultCluster)
    setSelectedOpeningsCluster(defaultCluster)
    setModeRows([])
    setModePage(1)
    setModeTotalPages(0)
    setOpeningRows([])
    setOpeningPlyByTop({})
    setOpeningMoveAnimByTop({})
    setActiveOpeningTop(null)
    setOpeningRequested(false)
    setOpeningError('')
    setOpeningLoading(false)
    setOpeningNMovesInput('')
    setOpeningNMoves(OPENING_N_MOVES_DEFAULT)
    setRatingChart(null)
    setRatingError('')
    setRatingLoading(false)
    setBoardFen('start')
    const chartPayload = await loadModeRatings(mode)
    const defaultRange = getClusterRangeFromBins(chartPayload?.bins, defaultCluster)
    await loadModeMoves(mode, 1, 'white', defaultRange)
  }

  const handleLoadOpenings = async (clusterKeyOverride = null) => {
    if (!selectedMode) return
    const requestedCluster =
      typeof clusterKeyOverride === 'string' && CLUSTER_KEYS.includes(clusterKeyOverride)
        ? clusterKeyOverride
        : selectedOpeningsCluster
    const parsed = Number.parseInt(openingNMovesInput, 10)
    if (!Number.isFinite(parsed) || parsed < OPENING_N_MOVES_MIN || parsed > OPENING_N_MOVES_MAX) {
      setOpeningError(`n_moves must be between ${OPENING_N_MOVES_MIN} and ${OPENING_N_MOVES_MAX}.`)
      return
    }
    const selectedRange = getClusterRangeFromBins(ratingChart?.bins, requestedCluster)
    if (!selectedRange) {
      setOpeningError('Cluster range is not ready yet. Wait for ratings chart to load, then try again.')
      return
    }
    setOpeningRows([])
    setOpeningPlyByTop({})
    setOpeningMoveAnimByTop({})
    setActiveOpeningTop(null)
    setBoardFen('start')
    await loadModeOpenings(selectedMode, parsed, selectedRange)
  }

  const handleMovesClusterChange = async (clusterKey) => {
    if (!selectedMode || clusterKey === selectedMovesCluster) return
    setSelectedMovesCluster(clusterKey)
    setModeRows([])
    setModePage(1)
    setModeTotalPages(0)
    const selectedRange = getClusterRangeFromBins(ratingChart?.bins, clusterKey)
    await loadModeMoves(selectedMode, 1, selectedMoveColor, selectedRange)
  }

  const handleOpeningsClusterChange = (clusterKey) => {
    if (!selectedMode || clusterKey === selectedOpeningsCluster) return
    setSelectedOpeningsCluster(clusterKey)
    setOpeningError('')
  }

  const handleMoveColorChange = async (moveColor) => {
    if (!selectedMode || moveColor === selectedMoveColor) return
    setSelectedMoveColor(moveColor)
    setModeRows([])
    setModePage(1)
    setModeTotalPages(0)
    const selectedRange = getClusterRangeFromBins(ratingChart?.bins, selectedMovesCluster)
    await loadModeMoves(selectedMode, 1, moveColor, selectedRange)
  }

  const handleMovesPrevious = async () => {
    if (!selectedMode || modeLoading || modePage <= 1) return
    const selectedRange = getClusterRangeFromBins(ratingChart?.bins, selectedMovesCluster)
    await loadModeMoves(selectedMode, modePage - 1, selectedMoveColor, selectedRange)
  }

  const handleMovesNext = async () => {
    if (!selectedMode || modeLoading || modeTotalPages <= 0 || modePage >= modeTotalPages) return
    const selectedRange = getClusterRangeFromBins(ratingChart?.bins, selectedMovesCluster)
    await loadModeMoves(selectedMode, modePage + 1, selectedMoveColor, selectedRange)
  }

  const handleOpeningStep = (top, direction) => {
    const row = openingRows.find((item) => item.top === top)
    if (!row) return
    const totalHalfMoves = row.half_moves.length
    if (totalHalfMoves <= 0) return

    const currentPly = Number(openingPlyByTop[top] || 1)
    const nextPly =
      direction === 'next'
        ? Math.min(totalHalfMoves, currentPly + 1)
        : Math.max(1, currentPly - 1)

    setOpeningPlyByTop((prev) => ({ ...prev, [top]: nextPly }))
    setOpeningMoveAnimByTop((prev) => {
      const nextSeq = Number(prev[top]?.seq || 0) + 1
      return {
        ...prev,
        [top]: {
          dir: direction === 'next' ? 'from-right' : 'from-left',
          seq: nextSeq
        }
      }
    })
    setActiveOpeningTop(top)
    setBoardFen(fenAfterHalfMoves(row.half_moves, nextPly))
  }

  const handleSelectOpening = (top) => {
    const row = openingRows.find((item) => item.top === top)
    if (!row) return
    const totalHalfMoves = row.half_moves.length
    const currentPly = Number(openingPlyByTop[top] || (totalHalfMoves > 0 ? 1 : 0))
    const safePly = Math.max(0, Math.min(currentPly, totalHalfMoves))
    setOpeningPlyByTop((prev) => ({ ...prev, [top]: safePly }))
    setActiveOpeningTop(top)
    setBoardFen(fenAfterHalfMoves(row.half_moves, safePly))
  }

  const loadRecentGames = async (player, page) => {
    setRecentLoading(true)
    setRecentError('')
    try {
      const payload = await fetchRecentGamesPage(player, page, 10)
      setRecentData(payload)
      setRecentPage(payload.page || page)
    } catch (error) {
      setRecentError(error instanceof Error ? error.message : 'Request failed.')
      setRecentData(null)
    } finally {
      setRecentLoading(false)
    }
  }

  const handleCount = async () => {
    const player = countPlayer.trim()
    if (!player) {
      setCountResult('Enter a player name first.')
      return
    }

    setCountLoading(true)
    setCountResult('')
    try {
      const payload = await fetchPlayerGameCount(player)
      const total = payload.total_games ?? 0
      setCountResult(`${payload.player_name || player}: ${total} games in the system.`)
      setSelectedPlayer(payload.player_name || player)
      await loadRecentGames(payload.player_name || player, 1)
      setSummaryLoading(true)
      setSummaryError('')
      const summary = await fetchGameSummary(payload.player_name || player)
      setSummaryData(summary)
    } catch (error) {
      setCountResult(error instanceof Error ? error.message : 'Request failed.')
      setSelectedPlayer('')
      setRecentData(null)
      setRecentError('')
      setSummaryData(null)
      setSummaryError(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      setCountLoading(false)
      setSummaryLoading(false)
    }
  }

  const handlePrevious = async () => {
    if (!selectedPlayer || recentPage <= 1 || recentLoading) return
    await loadRecentGames(selectedPlayer, recentPage - 1)
  }

  const handleNext = async () => {
    if (!selectedPlayer || recentLoading || !recentData) return
    const totalPages = recentData.total_pages || 0
    if (recentPage >= totalPages) return
    await loadRecentGames(selectedPlayer, recentPage + 1)
  }

  const activeOpeningRow = openingRows.find((row) => row.top === activeOpeningTop) || null
  const activeOpeningMeanRating =
    activeOpeningRow && Number.isFinite(Number(activeOpeningRow.mean_rating_for_this_opening))
      ? Number(activeOpeningRow.mean_rating_for_this_opening)
      : null
  const activeOpeningGames =
    activeOpeningRow && Number.isFinite(Number(activeOpeningRow.n_games_for_this_opening))
      ? Number(activeOpeningRow.n_games_for_this_opening)
      : null

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="games-main">
          <div className="games-hero-wrap">
            <h1 className="games-hero-badge">Games</h1>
            <section className="games-hero">
              <div className="games-generalities-grid">
              <article className="games-generality-card">
                <h3>Number of games</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(generalities?.n_games_in_db)}
                </p>
              </article>
              <a
                className="games-generality-card games-link-card"
                href="/main_characters"
                data-note="players with all their games in"
              >
                <h3>Main characters</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(generalities?.main_characters)}
                </p>
              </a>
              <a
                className="games-generality-card games-link-card"
                href="/secondary_character"
                data-note="the opponents of our main players"
              >
                <h3>Secondary characters</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(generalities?.secondary_characters)}
                </p>
              </a>
              <a
                className="games-generality-card games-link-card"
                href="/positions"
                data-note="stats about the positions"
              >
                <h3>Number of positions</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(generalities?.n_positions)}
                </p>
              </a>
              <a
                className="games-generality-card games-link-card"
                href="/positions"
                data-note="stats about the positions"
              >
                <h3>Scored fens</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(generalities?.scored_fens)}
                </p>
              </a>
              <a
                className="games-generality-card games-link-card"
                href="/positions"
                data-note="stats about the positions"
              >
                <h3>Unscored fens</h3>
                <p>
                  {generalitiesLoading
                    ? 'Loading...'
                    : formatNumber(Math.max(0, (generalities?.n_positions ?? 0) - (generalities?.scored_fens ?? 0)))}
                </p>
              </a>
            </div>
            {generalitiesError ? <p className="result-line">{generalitiesError}</p> : null}
            </section>
          </div>

          <div className="games-time-wrap">
            <h2 className="games-hero-badge time-control-badge">Time Control</h2>
            <section className="games-time-controls" aria-label="Time controls">
              <div className="games-generalities-grid games-time-controls-grid">
              <button
                className="games-generality-card games-mode-button games-link-card"
                type="button"
                onClick={() => handleSelectMode('bullet')}
              >
                <h3>bullet</h3>
                <span className="time-control-icon bullet-cartridge-icon" aria-hidden="true">
                  <span className="bullet-cartridge-shape" />
                </span>
                <p>{timeControlsLoading ? 'Loading...' : formatNumber(timeControlCounts.bullet)}</p>
              </button>
              <button
                className="games-generality-card games-mode-button games-link-card"
                type="button"
                onClick={() => handleSelectMode('blitz')}
              >
                <h3>blitz</h3>
                <span className="time-control-icon time-control-icon-blitz" aria-hidden="true">⚡</span>
                <p>{timeControlsLoading ? 'Loading...' : formatNumber(timeControlCounts.blitz)}</p>
              </button>
              <button
                className="games-generality-card games-mode-button games-link-card"
                type="button"
                onClick={() => handleSelectMode('rapid')}
              >
                <h3>rapid</h3>
                <span className="time-control-icon time-control-icon-rapid" aria-hidden="true">⏱</span>
                <p>{timeControlsLoading ? 'Loading...' : formatNumber(timeControlCounts.rapid)}</p>
              </button>
            </div>
            {timeControlsError ? <p className="result-line">{timeControlsError}</p> : null}
            </section>
          </div>

          {selectedMode ? (
            <section className="games-mode-detail" aria-label={`${selectedMode} ratings`}>
              {ratingLoading ? <p className="result-line">Loading ratings chart...</p> : null}
              {ratingError ? <p className="result-line">{ratingError}</p> : null}
              {!ratingLoading && !ratingError ? (
                <TimeControlRatingsScatterChart mode={selectedMode} chart={ratingChart} />
              ) : null}
            </section>
          ) : null}

          {selectedMode ? (
            <section className="games-mode-detail" aria-label={`${selectedMode} top moves`}>
              <div className="section-head mode-detail-head">
                <h2 className="games-action-title">{selectedMode}</h2>
                <div className="mode-detail-controls">
                  <div className="mode-cluster-switch">
                    {CLUSTER_KEYS.map((clusterKey) => (
                      <button
                        key={`moves-cluster-${clusterKey}`}
                        className={`mode-color-btn mode-cluster-btn ${selectedMovesCluster === clusterKey ? 'active' : ''}`}
                        type="button"
                        onClick={() => handleMovesClusterChange(clusterKey)}
                      >
                        {clusterKey}
                      </button>
                    ))}
                  </div>
                  <div className="mode-color-switch">
                    <button
                      className={`mode-color-btn mode-color-white ${selectedMoveColor === 'white' ? 'active' : ''}`}
                      type="button"
                      onClick={() => handleMoveColorChange('white')}
                    >
                      white
                    </button>
                    <button
                      className={`mode-color-btn mode-color-black ${selectedMoveColor === 'black' ? 'active' : ''}`}
                      type="button"
                      onClick={() => handleMoveColorChange('black')}
                    >
                      black
                    </button>
                  </div>
                </div>
              </div>
              <div className="mode-moves-table">
                <div className="mode-moves-row mode-moves-header mode-moves-chart-header">
                  <span>top moves chart</span>
                  <span className={`mode-chart-side mode-chart-side-${selectedMoveColor}`}>{selectedMoveColor}</span>
                </div>
                {modeLoading ? (
                  <p className="result-line">Loading top moves...</p>
                ) : null}
                {modeError ? (
                  <p className="result-line">{modeError}</p>
                ) : null}
                {!modeLoading && !modeError ? (
                  <div className="mode-moves-chart">
                    {(() => {
                      const maxTimes = Math.max(...modeRows.map((row) => Number(row.times_played || 0)), 1)
                      return modeRows.map((row) => {
                        const y = Number(row.times_played || 0)
                        const percent = Math.max(6, Math.round((y / maxTimes) * 100))
                        return (
                          <div key={`mode-${selectedMode}-move-${row.move_number}`} className="mode-moves-chart-col">
                            <span className="mode-moves-chart-value">{formatNumber(y)}</span>
                            <div className="mode-moves-chart-bar-wrap">
                              <div className="mode-moves-chart-bar" style={{ height: `${percent}%` }} />
                            </div>
                            <span className="mode-moves-chart-x">#{row.move_number}</span>
                            <span className="mode-moves-chart-tick">{row.move}</span>
                          </div>
                        )
                      })
                    })()}
                  </div>
                ) : null}
                <div className="mode-moves-pagination">
                  <button
                    className="btn btn-secondary"
                    type="button"
                    onClick={handleMovesPrevious}
                    disabled={modeLoading || modePage <= 1}
                  >
                    previous 5
                  </button>
                  <span>page {modePage} / {modeTotalPages || 0}</span>
                  <button
                    className="btn btn-secondary"
                    type="button"
                    onClick={handleMovesNext}
                    disabled={modeLoading || modeTotalPages <= 0 || modePage >= modeTotalPages}
                  >
                    next 5
                  </button>
                </div>
              </div>
            </section>
          ) : null}

          {selectedMode ? (
            <section className="games-openings-detail" aria-label={`${selectedMode} top openings`}>
              <div className="section-head section-head-centered">
                <h2 className="games-action-title">{selectedMode} openings</h2>
              </div>
              <div className="openings-cluster-row">
                {CLUSTER_KEYS.map((clusterKey) => (
                  <button
                    key={`openings-cluster-${clusterKey}`}
                    className={`mode-color-btn mode-cluster-btn ${selectedOpeningsCluster === clusterKey ? 'active' : ''}`}
                    type="button"
                    onClick={() => handleOpeningsClusterChange(clusterKey)}
                  >
                    {clusterKey}
                  </button>
                ))}
              </div>
              <div className="openings-layout">
                <div className="openings-board-row">
                  <div
                    className="react-board-wrap"
                    style={{ width: `${Math.round(OPENINGS_BOARD_WIDTH * 1.05)}px` }}
                  >
                    <Chessboard
                      id="openings-board"
                      position={boardFen}
                      arePiecesDraggable={false}
                      boardWidth={OPENINGS_BOARD_WIDTH}
                    />
                  </div>
                </div>
                <div className="openings-query-row">
                  <label htmlFor="openings-n-moves">moves</label>
                  <select
                    id="openings-n-moves"
                    className="text-input openings-n-moves-select"
                    value={openingNMovesInput}
                    onChange={(event) => setOpeningNMovesInput(event.target.value)}
                  >
                    <option value="" disabled>
                      min: {OPENING_N_MOVES_MIN} max: {OPENING_N_MOVES_MAX}
                    </option>
                    {Array.from({ length: OPENING_N_MOVES_MAX - OPENING_N_MOVES_MIN + 1 }, (_, idx) => {
                      const val = OPENING_N_MOVES_MIN + idx
                      return (
                        <option key={`n-moves-${val}`} value={val}>
                          {val}
                        </option>
                      )
                    })}
                  </select>
                  <button
                    className="btn btn-secondary"
                    type="button"
                    onClick={() => handleLoadOpenings()}
                    disabled={openingLoading}
                  >
                    {openingLoading ? 'Loading...' : 'go'}
                  </button>
                </div>
                <div className="opening-meta-row">
                  <span className="opening-meta-chip">
                    mean rating: {activeOpeningMeanRating !== null ? formatNumber(activeOpeningMeanRating) : '--'}
                  </span>
                  <span className="opening-meta-chip">
                    number of games: {activeOpeningGames !== null ? formatNumber(activeOpeningGames) : '--'}
                  </span>
                </div>
                <div className="mode-moves-table openings-list-table">
                  {openingLoading ? <p className="result-line">Loading openings...</p> : null}
                  {openingError ? <p className="result-line">{openingError}</p> : null}
                  {!openingRequested && !openingLoading ? (
                    <p className="result-line">Select moves and click go.</p>
                  ) : null}
                  {openingRequested && !openingLoading && !openingError ? (
                    openingRows.length ? (
                      openingRows.map((row) => {
                        const totalHalfMoves = row.half_moves.length
                        const currentPly = Number(openingPlyByTop[row.top] || (totalHalfMoves > 0 ? 1 : 0))
                        const currentMove = currentPly > 0 ? row.half_moves[currentPly - 1] : '-'
                        const moveAnimState = openingMoveAnimByTop[row.top] || null
                        const moveAnimClass =
                          moveAnimState?.dir === 'from-right'
                            ? 'opening-move-enter-from-right'
                            : moveAnimState?.dir === 'from-left'
                              ? 'opening-move-enter-from-left'
                              : ''
                        return (
                          <div
                            key={`opening-${selectedMode}-${row.top}`}
                            className={`mode-moves-row opening-step-row ${activeOpeningTop === row.top ? 'active' : ''}`}
                          >
                            <button
                              className={`opening-top-btn ${activeOpeningTop === row.top ? 'active' : ''}`}
                              type="button"
                              onClick={() => handleSelectOpening(row.top)}
                            >
                              top_{row.top}
                            </button>
                            <button
                              className="btn btn-secondary opening-arrow-btn"
                              type="button"
                              onClick={() => handleOpeningStep(row.top, 'prev')}
                              disabled={currentPly <= 1}
                            >
                              {'<'}
                            </button>
                            <span
                              key={`opening-move-${row.top}-${currentPly}-${moveAnimState?.seq || 0}`}
                              className={`opening-move-text ${moveAnimClass}`}
                            >
                              {currentMove}
                            </span>
                            <button
                              className="btn btn-secondary opening-arrow-btn"
                              type="button"
                              onClick={() => handleOpeningStep(row.top, 'next')}
                              disabled={currentPly <= 0 || currentPly >= totalHalfMoves}
                            >
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
          ) : null}

          <section className="games-count" aria-label="Games explorer">
            <div className="section-head">
              <h2>Games Explorer</h2>
              <p>Check how many games exist, then view stats and recent games.</p>
            </div>
            <div className="count-row">
              <div className="count-controls">
                <label htmlFor="count-player">Player name</label>
                <input
                  id="count-player"
                  className="text-input"
                  type="text"
                  value={countPlayer}
                  onChange={(event) => setCountPlayer(event.target.value)}
                  onKeyDown={(event) => event.key === 'Enter' && handleCount()}
                  placeholder="e.g. gothamchess"
                />
                <button className="btn btn-secondary" type="button" onClick={handleCount} disabled={countLoading}>
                  {countLoading ? 'Checking...' : 'Check Count'}
                </button>
                <p className="result-line strong">{countResult || '-'}</p>
              </div>

              {summaryLoading ? <p className="result-line">Loading stats...</p> : null}
              {summaryError ? <p className="result-line">{summaryError}</p> : null}
              {summaryData ? (
                <div className="games-stats">
                  <div className="stat-chip stat-win">Wins: {summaryData.wins ?? 0}</div>
                  <div className="stat-chip stat-loss">Losses: {summaryData.losses ?? 0}</div>
                  <div className="stat-chip stat-draw">Draws: {summaryData.draws ?? 0}</div>
                  <div className="stat-chip">From: {summaryData.date_from ? summaryData.date_from.slice(0, 10) : '—'}</div>
                  <div className="stat-chip">To: {summaryData.date_to ? summaryData.date_to.slice(0, 10) : '—'}</div>
                  <div className="stat-chip">Modes: {summaryData.time_controls?.length || 0}</div>
                </div>
              ) : null}
            </div>
          </section>

          {selectedPlayer ? (
            <section className="games-recent" aria-label="Recent player games">
              <div className="section-head">
                <h2>Recent Games</h2>
                <p>Last 10 games for {selectedPlayer}. Use pagination to browse the next ten.</p>
              </div>

              <div className="recent-pagination">
                <button className="btn btn-secondary" type="button" onClick={handlePrevious} disabled={recentLoading || recentPage <= 1}>
                  Previous 10
                </button>
                <span>
                  Page {recentData?.page || recentPage} / {recentData?.total_pages || 0}
                </span>
                <button
                  className="btn btn-secondary"
                  type="button"
                  onClick={handleNext}
                  disabled={recentLoading || !recentData || recentPage >= (recentData.total_pages || 0)}
                >
                  Next 10
                </button>
              </div>

              {recentLoading ? <p className="result-line">Loading games...</p> : null}
              {recentError ? <p className="result-line">{recentError}</p> : null}

              {!recentLoading && !recentError ? (
                <div className="recent-list">
                  {recentData?.games?.length ? (
                    recentData.games.map((game, index) => {
                      const [datePart, timePart] = game.played_at.split(' ')
                      const [year, monthNum, day] = datePart.split('-')
                      const month = new Intl.DateTimeFormat('en', { month: 'short' }).format(
                        new Date(parseInt(year, 10), parseInt(monthNum, 10) - 1, 1)
                      ).toUpperCase()
                      const resultClass =
                        game.result === 'win' ? 'result-win' : game.result === 'loss' ? 'result-loss' : 'result-draw'

                      return (
                        <a
                          key={`${game.link || index}-${game.played_at}`}
                          className="recent-line"
                          href={`/games/${game.link || ''}`}
                          aria-label={`Open game ${game.link || ''}`}
                        >
                          <span className="recent-date date-chip">{year}</span>
                          <span className="recent-date date-chip">{month}</span>
                          <span className="recent-date date-chip">{day}</span>
                          <span className="time-chip">{timePart}</span>
                          <span className={`color-chip ${game.color === 'white' ? 'color-white' : 'color-black'}`}>
                            {game.color}
                          </span>
                          <span className={resultClass}>{game.result}</span>
                          <span className="recent-open">Open</span>
                        </a>
                      )
                    })
                  ) : (
                    <p className="recent-line">No games found.</p>
                  )}
                </div>
              ) : null}
            </section>
          ) : null}
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default Games
