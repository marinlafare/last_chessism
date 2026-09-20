import { useEffect, useState } from 'react'
import {
  CLUSTER_KEYS, DEFAULT_CLUSTER_KEY, MOVES_PAGE_SIZE, OPENINGS_PAGE_SIZE,
  OPENING_N_MOVES_DEFAULT, OPENING_N_MOVES_MAX, OPENING_N_MOVES_MIN,
  TIME_CONTROLS_CACHE_VERSION, TOP_MOVE_LIMIT, fetchDatabaseGeneralities,
  fetchGameSummary, fetchPlayerGameCount, fetchRecentGamesPage,
  fetchTimeControlActivityTrend, fetchTimeControlCounts,
  fetchTimeControlGameLengthAnalytics, fetchTimeControlRatingChart,
  fetchTimeControlResultColorMatrix, fetchTimeControlTopMoves,
  fetchTimeControlTopOpenings, getClusterRangeFromBins, hasAnyValidJenksBin,
  normalizeOpeningRows, readCached, writeCached,
} from './gamesPageSupport'

export function useGamesPage() {
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
  const [selectedResultsCluster, setSelectedResultsCluster] = useState(DEFAULT_CLUSTER_KEY)
  const [selectedLengthsCluster, setSelectedLengthsCluster] = useState(DEFAULT_CLUSTER_KEY)
  const [selectedActivityCluster, setSelectedActivityCluster] = useState(DEFAULT_CLUSTER_KEY)
  const [selectedOpeningsCluster, setSelectedOpeningsCluster] = useState(DEFAULT_CLUSTER_KEY)
  const [modeRows, setModeRows] = useState([])
  const [modePage, setModePage] = useState(1)
  const [modeTotalPages, setModeTotalPages] = useState(0)
  const [modeLoading, setModeLoading] = useState(false)
  const [modeError, setModeError] = useState('')
  const [ratingChart, setRatingChart] = useState(null)
  const [ratingLoading, setRatingLoading] = useState(false)
  const [ratingError, setRatingError] = useState('')
  const [resultMatrix, setResultMatrix] = useState(null)
  const [resultMatrixLoading, setResultMatrixLoading] = useState(false)
  const [resultMatrixError, setResultMatrixError] = useState('')
  const [gameLengthData, setGameLengthData] = useState(null)
  const [gameLengthLoading, setGameLengthLoading] = useState(false)
  const [gameLengthError, setGameLengthError] = useState('')
  const [activityTrendData, setActivityTrendData] = useState(null)
  const [activityTrendLoading, setActivityTrendLoading] = useState(false)
  const [activityTrendError, setActivityTrendError] = useState('')
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
  const [ratingModalOpen, setRatingModalOpen] = useState(false)

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

  useEffect(() => {
    const handleEsc = (event) => {
      if (event.key === 'Escape') {
        setRatingModalOpen(false)
      }
    }
    window.addEventListener('keydown', handleEsc)
    return () => window.removeEventListener('keydown', handleEsc)
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

  const loadResultColorMatrix = async (mode, ratingRange = null) => {
    const minRating = Number.isFinite(ratingRange?.minRating) ? Number(ratingRange.minRating) : ''
    const maxRating = Number.isFinite(ratingRange?.maxRating) ? Number(ratingRange.maxRating) : ''
    const cacheKey = `games:time_controls:${TIME_CONTROLS_CACHE_VERSION}:result_matrix:${mode}:min:${minRating}:max:${maxRating}`
    const cached = readCached(cacheKey)
    if (cached) {
      setResultMatrix(cached)
      setResultMatrixError('')
      setResultMatrixLoading(false)
      return
    }

    setResultMatrixLoading(true)
    setResultMatrixError('')
    try {
      const payload = await fetchTimeControlResultColorMatrix(
        mode,
        Number.isFinite(ratingRange?.minRating) ? ratingRange.minRating : null,
        Number.isFinite(ratingRange?.maxRating) ? ratingRange.maxRating : null
      )
      writeCached(cacheKey, payload)
      setResultMatrix(payload)
    } catch (error) {
      setResultMatrix(null)
      setResultMatrixError(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      setResultMatrixLoading(false)
    }
  }

  const loadGameLengthAnalytics = async (mode, ratingRange = null) => {
    const minRating = Number.isFinite(ratingRange?.minRating) ? Number(ratingRange.minRating) : ''
    const maxRating = Number.isFinite(ratingRange?.maxRating) ? Number(ratingRange.maxRating) : ''
    const cacheKey = `games:time_controls:${TIME_CONTROLS_CACHE_VERSION}:game_length:${mode}:min:${minRating}:max:${maxRating}`
    const cached = readCached(cacheKey)
    if (cached) {
      setGameLengthData(cached)
      setGameLengthError('')
      setGameLengthLoading(false)
      return
    }

    setGameLengthLoading(true)
    setGameLengthError('')
    try {
      const payload = await fetchTimeControlGameLengthAnalytics(
        mode,
        Number.isFinite(ratingRange?.minRating) ? ratingRange.minRating : null,
        Number.isFinite(ratingRange?.maxRating) ? ratingRange.maxRating : null
      )
      writeCached(cacheKey, payload)
      setGameLengthData(payload)
    } catch (error) {
      setGameLengthData(null)
      setGameLengthError(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      setGameLengthLoading(false)
    }
  }

  const loadActivityTrend = async (mode, ratingRange = null) => {
    const minRating = Number.isFinite(ratingRange?.minRating) ? Number(ratingRange.minRating) : ''
    const maxRating = Number.isFinite(ratingRange?.maxRating) ? Number(ratingRange.maxRating) : ''
    const cacheKey = `games:time_controls:${TIME_CONTROLS_CACHE_VERSION}:activity:${mode}:min:${minRating}:max:${maxRating}`
    const cached = readCached(cacheKey)
    if (cached) {
      setActivityTrendData(cached)
      setActivityTrendError('')
      setActivityTrendLoading(false)
      return
    }

    setActivityTrendLoading(true)
    setActivityTrendError('')
    try {
      const payload = await fetchTimeControlActivityTrend(
        mode,
        Number.isFinite(ratingRange?.minRating) ? ratingRange.minRating : null,
        Number.isFinite(ratingRange?.maxRating) ? ratingRange.maxRating : null
      )
      writeCached(cacheKey, payload)
      setActivityTrendData(payload)
    } catch (error) {
      setActivityTrendData(null)
      setActivityTrendError(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      setActivityTrendLoading(false)
    }
  }

  const handleSelectMode = async (mode) => {
    const defaultCluster = DEFAULT_CLUSTER_KEY
    setSelectedMode(mode)
    setSelectedMoveColor('white')
    setSelectedMovesCluster(defaultCluster)
    setSelectedResultsCluster(defaultCluster)
    setSelectedLengthsCluster(defaultCluster)
    setSelectedActivityCluster(defaultCluster)
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
    setResultMatrix(null)
    setResultMatrixError('')
    setResultMatrixLoading(false)
    setGameLengthData(null)
    setGameLengthError('')
    setGameLengthLoading(false)
    setActivityTrendData(null)
    setActivityTrendError('')
    setActivityTrendLoading(false)
    setBoardFen('start')
    const chartPayload = await loadModeRatings(mode)
    const defaultRange = getClusterRangeFromBins(chartPayload?.bins, defaultCluster)
    await Promise.all([
      loadModeMoves(mode, 1, 'white', defaultRange),
      loadResultColorMatrix(mode, defaultRange),
      loadGameLengthAnalytics(mode, defaultRange),
      loadActivityTrend(mode, defaultRange)
    ])
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

  const handleResultsClusterChange = async (clusterKey) => {
    if (!selectedMode || clusterKey === selectedResultsCluster) return
    setSelectedResultsCluster(clusterKey)
    const selectedRange = getClusterRangeFromBins(ratingChart?.bins, clusterKey)
    await loadResultColorMatrix(selectedMode, selectedRange)
  }

  const handleLengthsClusterChange = async (clusterKey) => {
    if (!selectedMode || clusterKey === selectedLengthsCluster) return
    setSelectedLengthsCluster(clusterKey)
    const selectedRange = getClusterRangeFromBins(ratingChart?.bins, clusterKey)
    await loadGameLengthAnalytics(selectedMode, selectedRange)
  }

  const handleActivityClusterChange = async (clusterKey) => {
    if (!selectedMode || clusterKey === selectedActivityCluster) return
    setSelectedActivityCluster(clusterKey)
    const selectedRange = getClusterRangeFromBins(ratingChart?.bins, clusterKey)
    await loadActivityTrend(selectedMode, selectedRange)
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

  const openRatingModal = () => {
    if (!ratingChart || ratingLoading || ratingError) return
    setRatingModalOpen(true)
  }

  const closeRatingModal = () => setRatingModalOpen(false)

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

  return {
    activeOpeningGames, activeOpeningMeanRating, activeOpeningRow, activeOpeningTop,
    activityTrendData, activityTrendError, activityTrendLoading, boardFen,
    closeRatingModal, countLoading, countPlayer, countResult, gameLengthData,
    gameLengthError, gameLengthLoading, generalities, generalitiesError,
    generalitiesLoading, handleActivityClusterChange, handleCount,
    handleLengthsClusterChange, handleLoadOpenings, handleMoveColorChange,
    handleMovesClusterChange, handleMovesNext, handleMovesPrevious, handleNext,
    handleOpeningStep, handleOpeningsClusterChange, handlePrevious,
    handleResultsClusterChange, handleSelectMode, handleSelectOpening, modeError,
    modeLoading, modePage, modeRows, modeTotalPages, openRatingModal,
    openingError, openingLoading, openingMoveAnimByTop, openingNMoves,
    openingNMovesInput, openingPlyByTop, openingRequested, openingRows, ratingChart,
    ratingError, ratingLoading, ratingModalOpen, recentData, recentError, recentLoading,
    recentPage, resultMatrix, resultMatrixError, resultMatrixLoading,
    selectedActivityCluster, selectedLengthsCluster, selectedMode, selectedMoveColor,
    selectedMovesCluster, selectedOpeningsCluster, selectedPlayer,
    selectedResultsCluster, setCountPlayer, setOpeningNMovesInput, summaryData,
    summaryError, summaryLoading, timeControlCounts, timeControlsError,
    timeControlsLoading,
  }
}
