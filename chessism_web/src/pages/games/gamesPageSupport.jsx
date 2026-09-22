import { Chess } from 'chess.js'
import { getJson } from '../../services/apiClient'
import { formatNumber } from '../../utils/formatters'

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

const hexToRgb = (hexColor) => {
  const normalized = String(hexColor || '').replace('#', '')
  if (normalized.length !== 6) {
    return { r: 244, g: 192, b: 87 }
  }
  const value = Number.parseInt(normalized, 16)
  if (Number.isNaN(value)) {
    return { r: 244, g: 192, b: 87 }
  }
  return {
    r: (value >> 16) & 255,
    g: (value >> 8) & 255,
    b: value & 255
  }
}

const CLUSTER_RGB = {
  cluster_1: hexToRgb(CLUSTER_COLORS.cluster_1),
  cluster_2: hexToRgb(CLUSTER_COLORS.cluster_2),
  cluster_3: hexToRgb(CLUSTER_COLORS.cluster_3)
}

const OPENINGS_BOARD_WIDTH = 286
const TIME_CONTROLS_CACHE_VERSION = 'v12'
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
  return getJson('/games/database/generalities')
}

async function fetchPlayerGameCount(playerName) {
  return getJson(`/games/${encodeURIComponent(playerName)}/count`)
}

async function fetchRecentGamesPage(playerName, page, pageSize = 10) {
  return getJson(
    `/games/${encodeURIComponent(playerName)}/recent?page=${page}&page_size=${pageSize}`
  )
}

async function fetchGameSummary(playerName) {
  return getJson(`/games/${encodeURIComponent(playerName)}/summary`)
}

async function fetchTimeControlCounts() {
  return getJson('/games/time_controls')
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
  return getJson(
    `/games/time_controls/${encodeURIComponent(mode)}/top_moves?${params.toString()}`
  )
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
  return getJson(
    `/games/time_controls/${encodeURIComponent(mode)}/top_openings?${params.toString()}`
  )
}

async function fetchTimeControlRatingChart(mode) {
  return getJson(
    `/games/rating_time_control_chart?time_control=${encodeURIComponent(mode)}`
  )
}

async function fetchTimeControlResultColorMatrix(mode, minRating = null, maxRating = null) {
  const params = new URLSearchParams()
  if (Number.isFinite(minRating) && Number.isFinite(maxRating)) {
    params.set('min_rating', String(minRating))
    params.set('max_rating', String(maxRating))
  }
  return getJson(
    `/games/time_controls/${encodeURIComponent(mode)}/result_color_matrix?${params.toString()}`
  )
}

async function fetchTimeControlGameLengthAnalytics(mode, minRating = null, maxRating = null) {
  const params = new URLSearchParams()
  if (Number.isFinite(minRating) && Number.isFinite(maxRating)) {
    params.set('min_rating', String(minRating))
    params.set('max_rating', String(maxRating))
  }
  return getJson(
    `/games/time_controls/${encodeURIComponent(mode)}/game_length_analytics?${params.toString()}`
  )
}

async function fetchTimeControlActivityTrend(mode, minRating = null, maxRating = null) {
  const params = new URLSearchParams()
  if (Number.isFinite(minRating) && Number.isFinite(maxRating)) {
    params.set('min_rating', String(minRating))
    params.set('max_rating', String(maxRating))
  }
  return getJson(
    `/games/time_controls/${encodeURIComponent(mode)}/activity_trend?${params.toString()}`
  )
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

function TimeControlRatingsScatterChart({ mode, chart, size = 'normal' }) {
  const xValues = Array.isArray(chart?.x) ? chart.x.map((value) => Number(value || 0)) : []
  const yValues = Array.isArray(chart?.y) ? chart.y.map((value) => Number(value || 0)) : []
  const bins = chart?.bins && typeof chart.bins === 'object' ? chart.bins : {}

  if (!xValues.length || xValues.length !== yValues.length) {
    return <p className="result-line">No ratings data for this time control.</p>
  }

  const isLarge = size === 'large'
  const width = isLarge ? 1100 : 820
  const height = isLarge ? 360 : 250
  const padLeft = 44
  const padRight = 24
  const padTop = isLarge ? 20 : 16
  const padBottom = isLarge ? 48 : 38
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
        <text
          x={padLeft - 40}
          y={padTop + plotHeight / 2}
          className="axis-tick-label axis-title axis-title-y"
          textAnchor="middle"
          transform={`rotate(-90 ${padLeft - 40} ${padTop + plotHeight / 2})`}
        >
          # of players
        </text>
        <text
          x={padLeft + plotWidth / 2}
          y={height - padBottom + 26}
          className="axis-tick-label axis-title axis-title-x"
          textAnchor="middle"
        >
          raitings
        </text>
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

const formatPercent = (value) => `${(Number(value || 0) * 100).toFixed(1)}%`
const formatSeconds = (value) => (Number.isFinite(Number(value)) ? `${Number(value).toFixed(1)}s` : '--')
const getClusterSelectStyle = (clusterKey) => {
  const color = CLUSTER_COLORS[clusterKey] || CLUSTER_COLORS.cluster_2
  return { backgroundColor: color, borderColor: color, color: '#000' }
}

const getHeatCellStyle = (value, maxValue) => {
  const safeValue = Number(value || 0)
  const safeMax = Math.max(1, Number(maxValue || 1))
  const ratio = Math.max(0, Math.min(1, safeValue / safeMax))
  const useLowHalf = ratio <= 0.5
  const start = useLowHalf ? CLUSTER_RGB.cluster_1 : CLUSTER_RGB.cluster_2
  const end = useLowHalf ? CLUSTER_RGB.cluster_2 : CLUSTER_RGB.cluster_3
  const t = useLowHalf ? ratio * 2 : (ratio - 0.5) * 2
  const r = Math.round(start.r + (end.r - start.r) * t)
  const g = Math.round(start.g + (end.g - start.g) * t)
  const b = Math.round(start.b + (end.b - start.b) * t)
  const alpha = (0.58 + ratio * 0.32).toFixed(2)
  return {
    backgroundColor: `rgba(${r}, ${g}, ${b}, ${alpha})`,
    borderColor: `rgba(${r}, ${g}, ${b}, 0.75)`
  }
}

function CompactBarChart({ title, labels, values }) {
  const maxValue = Math.max(...(Array.isArray(values) ? values.map((v) => Number(v || 0)) : [0]), 1)
  const safeLabels = Array.isArray(labels) ? labels : []
  const safeValues = Array.isArray(values) ? values : []

  if (!safeLabels.length || safeLabels.length !== safeValues.length) {
    return <p className="result-line">No chart data.</p>
  }

  return (
    <div className="analytics-bars-wrap">
      {title ? <h3>{title}</h3> : null}
      <div className="analytics-bars-scroll">
        <div className="analytics-bars">
          {safeLabels.map((label, idx) => {
            const value = Number(safeValues[idx] || 0)
            const pct = Math.max(6, Math.round((value / maxValue) * 100))
            return (
              <div key={`${title}-${label}-${idx}`} className="analytics-bar-col">
                <span className="analytics-bar-value">{formatNumber(value)}</span>
                <div className="analytics-bar-track">
                  <div className="analytics-bar-fill" style={{ height: `${pct}%` }} />
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
  const maxValue = Math.max(...safeValues.map((v) => Number(v || 0)), 1)

  if (!safeLabels.length || safeLabels.length !== safeValues.length) {
    return <p className="result-line">No heat data.</p>
  }

  return (
    <div className="heat-strip-wrap">
      <h3>{title}</h3>
      <div
        className={`heat-strip-grid${singleRow ? ' single-row' : ''}`}
        style={singleRow ? { '--heat-cols': safeLabels.length } : undefined}
      >
        {safeLabels.map((label, idx) => {
          const value = Number(safeValues[idx] || 0)
          return (
            <div key={`${title}-${label}-${idx}`} className="heat-cell-wrap">
              <div className="heat-cell" style={getHeatCellStyle(value, maxValue)} title={`${label}: ${value}`}>
                {label}
              </div>
              <div className="heat-cell-value">{formatNumber(value)}</div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

function ClusterSelect({ value, onChange, idPrefix }) {
  return (
    <select
      className="cluster-select"
      style={getClusterSelectStyle(value)}
      value={value}
      onChange={(event) => onChange(event.target.value)}
    >
      {CLUSTER_KEYS.map((clusterKey) => (
        <option key={`${idPrefix}-${clusterKey}`} value={clusterKey}>
          {clusterKey}
        </option>
      ))}
    </select>
  )
}

const handleBadgeHoverMove = (event) => {
  const rect = event.currentTarget.getBoundingClientRect()
  const x = ((event.clientX - rect.left) / rect.width) * 100
  const y = ((event.clientY - rect.top) / rect.height) * 100
  event.currentTarget.style.setProperty('--hover-x', `${x}%`)
  event.currentTarget.style.setProperty('--hover-y', `${y}%`)
}

const handleBadgeHoverLeave = (event) => {
  event.currentTarget.style.removeProperty('--hover-x')
  event.currentTarget.style.removeProperty('--hover-y')
}

export {
  CLUSTER_COLORS,
  CLUSTER_KEYS,
  CLUSTER_RGB,
  CLUSTER_TO_JENKS_KEY,
  ClusterSelect,
  CompactBarChart,
  DEFAULT_CLUSTER_KEY,
  HeatStrip,
  MOVES_PAGE_SIZE,
  OPENINGS_BOARD_WIDTH,
  OPENINGS_PAGE_SIZE,
  OPENING_N_MOVES_DEFAULT,
  OPENING_N_MOVES_MAX,
  OPENING_N_MOVES_MIN,
  TIME_CONTROLS_CACHE_VERSION,
  TOP_MOVE_LIMIT,
  TimeControlRatingsScatterChart,
  fetchDatabaseGeneralities,
  fetchGameSummary,
  fetchPlayerGameCount,
  fetchRecentGamesPage,
  fetchTimeControlActivityTrend,
  fetchTimeControlCounts,
  fetchTimeControlGameLengthAnalytics,
  fetchTimeControlRatingChart,
  fetchTimeControlResultColorMatrix,
  fetchTimeControlTopMoves,
  fetchTimeControlTopOpenings,
  formatNumber,
  formatPercent,
  formatSeconds,
  getClusterRangeFromBins,
  getClusterSelectStyle,
  getHeatCellStyle,
  getPointCluster,
  halfMovesFromMovesMap,
  handleBadgeHoverLeave,
  handleBadgeHoverMove,
  hasAnyValidJenksBin,
  moveNumberToKey,
  normalizeOpeningRows,
  readCached,
  writeCached,
}
