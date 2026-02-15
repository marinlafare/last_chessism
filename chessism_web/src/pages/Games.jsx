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
const OPENING_DEPTH_MOVES = 10
const OPENINGS_BOARD_WIDTH = 286
const TIME_CONTROLS_CACHE_VERSION = 'v7'
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
  page = 1,
  pageSize = MOVES_PAGE_SIZE,
  maxMove = TOP_MOVE_LIMIT
) {
  const response = await fetch(
    `${API_BASE_URL}/games/time_controls/${encodeURIComponent(mode)}/top_moves?move_color=${encodeURIComponent(moveColor)}&page=${page}&page_size=${pageSize}&max_move=${maxMove}`
  )
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchTimeControlTopOpenings(mode, page = 1, pageSize = OPENINGS_PAGE_SIZE) {
  const response = await fetch(
    `${API_BASE_URL}/games/time_controls/${encodeURIComponent(mode)}/top_openings?page=${page}&page_size=${pageSize}&opening_depth_moves=${OPENING_DEPTH_MOVES}`
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
  const [modeRows, setModeRows] = useState([])
  const [modePage, setModePage] = useState(1)
  const [modeTotalPages, setModeTotalPages] = useState(0)
  const [modeLoading, setModeLoading] = useState(false)
  const [modeError, setModeError] = useState('')
  const [openingRows, setOpeningRows] = useState([])
  const [openingLoading, setOpeningLoading] = useState(false)
  const [openingError, setOpeningError] = useState('')
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

  const loadModeMoves = async (mode, page, moveColor = 'white') => {
    const cacheKey = `games:time_controls:${TIME_CONTROLS_CACHE_VERSION}:moves:${mode}:color:${moveColor}:page:${page}:size:${MOVES_PAGE_SIZE}:max:${TOP_MOVE_LIMIT}`
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
      const payload = await fetchTimeControlTopMoves(mode, moveColor, page, MOVES_PAGE_SIZE, TOP_MOVE_LIMIT)
      const normalized = {
        mode,
        move_color: moveColor,
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

  const loadModeOpenings = async (mode) => {
    const cacheKey = `games:time_controls:${TIME_CONTROLS_CACHE_VERSION}:openings:${mode}:page:1:size:${OPENINGS_PAGE_SIZE}:depth:${OPENING_DEPTH_MOVES}`
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
      const payload = await fetchTimeControlTopOpenings(mode, 1, OPENINGS_PAGE_SIZE)
      const normalizedRows = normalizeOpeningRows(payload)
      const normalized = {
        mode,
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
    setSelectedMode(mode)
    setSelectedMoveColor('white')
    setModeRows([])
    setModePage(1)
    setModeTotalPages(0)
    setOpeningRows([])
    setOpeningPlyByTop({})
    setOpeningMoveAnimByTop({})
    setActiveOpeningTop(null)
    setBoardFen('start')
    await Promise.all([
      loadModeMoves(mode, 1, 'white'),
      loadModeOpenings(mode)
    ])
  }

  const handleMoveColorChange = async (moveColor) => {
    if (!selectedMode || moveColor === selectedMoveColor) return
    setSelectedMoveColor(moveColor)
    setModeRows([])
    setModePage(1)
    setModeTotalPages(0)
    await loadModeMoves(selectedMode, 1, moveColor)
  }

  const handleMovesPrevious = async () => {
    if (!selectedMode || modeLoading || modePage <= 1) return
    await loadModeMoves(selectedMode, modePage - 1, selectedMoveColor)
  }

  const handleMovesNext = async () => {
    if (!selectedMode || modeLoading || modeTotalPages <= 0 || modePage >= modeTotalPages) return
    await loadModeMoves(selectedMode, modePage + 1, selectedMoveColor)
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
          <section className="games-hero">
            <h1>Games</h1>
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

          <section className="games-action-row" aria-label="Add or update games">
            <div className="section-head">
              <h2 className="games-action-title">add/update</h2>
            </div>
            <a className="btn btn-primary tooltip-btn" href="/download_new_games" data-note="download or update new games">
              more games
            </a>
          </section>

          <section className="games-time-controls" aria-label="Time controls">
            <div className="section-head section-head-centered">
              <h2 className="games-action-title">time controls</h2>
            </div>
            <div className="games-generalities-grid games-time-controls-grid">
              <button
                className="games-generality-card games-mode-button"
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
                className="games-generality-card games-mode-button"
                type="button"
                onClick={() => handleSelectMode('blitz')}
              >
                <h3>blitz</h3>
                <span className="time-control-icon time-control-icon-blitz" aria-hidden="true">⚡</span>
                <p>{timeControlsLoading ? 'Loading...' : formatNumber(timeControlCounts.blitz)}</p>
              </button>
              <button
                className="games-generality-card games-mode-button"
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

          {selectedMode ? (
            <section className="games-mode-detail" aria-label={`${selectedMode} top moves`}>
              <div className="section-head mode-detail-head">
                <h2 className="games-action-title">{selectedMode}</h2>
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
                  {!openingLoading && !openingError ? (
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
