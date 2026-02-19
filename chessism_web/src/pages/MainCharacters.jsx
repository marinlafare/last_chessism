import { useEffect, useMemo, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import { API_BASE_URL } from '../config'

const MAIN_CHARACTER_LIMIT = 5000
const TOP_PLAYERS_PAGE_SIZE = 6
const EMPTY_COUNTS = { bullet: 0, blitz: 0, rapid: 0 }
const COUNTS_CACHE_KEY = 'main_characters_time_controls_v1'
const COUNTS_CACHE_TTL_MS = 1000 * 60 * 60 * 6

const parseJsonSafely = async (response) => response.json().catch(() => ({}))

const throwIfNotOk = (response, payload) => {
  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }
}

const fetchJsonOrThrow = async (url, options = undefined) => {
  const response = await fetch(url, options)
  const payload = await parseJsonSafely(response)
  throwIfNotOk(response, payload)
  return payload
}

const fetchMainCharacterTimeControls = async () =>
  fetchJsonOrThrow(`${API_BASE_URL}/players/main_characters/time_controls`)

const fetchTopMainCharacters = async (timeControl, limit = MAIN_CHARACTER_LIMIT) =>
  fetchJsonOrThrow(
    `${API_BASE_URL}/players/main_characters/top?time_control=${encodeURIComponent(timeControl)}&limit=${limit}`
  )

const updatePlayerGames = async (playerName) =>
  fetchJsonOrThrow(`${API_BASE_URL}/games/update`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ player_name: playerName })
  })

const createPlayerGames = async (playerName) =>
  fetchJsonOrThrow(`${API_BASE_URL}/games`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ player_name: playerName })
  })

const readCachedCounts = () => {
  try {
    if (typeof window === 'undefined') return null
    const raw = window.localStorage.getItem(COUNTS_CACHE_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw)
    const savedAt = Number(parsed?.saved_at || 0)
    const counts = parsed?.counts
    const isFresh = savedAt > 0 && Date.now() - savedAt <= COUNTS_CACHE_TTL_MS
    if (!isFresh || !counts || typeof counts !== 'object') return null
    return {
      bullet: Number(counts?.bullet || 0),
      blitz: Number(counts?.blitz || 0),
      rapid: Number(counts?.rapid || 0)
    }
  } catch {
    return null
  }
}

const writeCachedCounts = (counts) => {
  try {
    if (typeof window === 'undefined') return
    window.localStorage.setItem(
      COUNTS_CACHE_KEY,
      JSON.stringify({
        saved_at: Date.now(),
        counts
      })
    )
  } catch {
    // Ignore localStorage errors.
  }
}

const formatNumber = (value) => {
  const numeric = Number(value ?? 0)
  if (Number.isNaN(numeric)) return '0'
  return numeric.toLocaleString('en-US')
}

const getPrimaryDisplayName = (player) => String(player?.player_name || '').trim() || '--'

const getSecondaryDisplayName = (player) => {
  const fullName = String(player?.full_name || '').trim()
  return fullName || 'Name unavailable'
}

const classifyUpdateMessage = (message) => {
  const text = String(message || '').toLowerCase()
  if (
    text.includes('no new games') ||
    text.includes('already up to date') ||
    text.includes('all months in db already') ||
    text.includes('all games already at db')
  ) {
    return 'all_ready'
  }
  if (text.includes('error') || text.includes('failed')) {
    return 'error'
  }
  return 'updated'
}

const getRankColor = (rank, totalPlayers) => {
  const safeTotal = Math.max(1, Number(totalPlayers) || 1)
  if (safeTotal === 1) return 'hsl(120 72% 42%)'
  const safeRank = Math.min(safeTotal, Math.max(1, Number(rank) || 1))
  const ratio = (safeRank - 1) / (safeTotal - 1)
  const hue = Math.round(120 * (1 - ratio))
  return `hsl(${hue} 72% 42%)`
}

function MainCharacterCard({ player, rank, totalPlayers, onUpdate, updating, updateState }) {
  if (!player) return null

  const primaryName = getPrimaryDisplayName(player)
  const lastGameDate = String(player?.last_game_date || '').trim() || '--'
  const isLongName = primaryName.length > 15
  const rankBackground = getRankColor(rank, totalPlayers)
  const buttonLabel = updating
    ? 'updating...'
    : updateState === 'all_ready'
      ? 'all games ready'
      : updateState === 'updated'
        ? 'updated'
        : updateState === 'error'
          ? 'retry'
          : 'update'

  const profileHref = `/players?player=${encodeURIComponent(String(player?.player_name || ''))}`

  const handleCardNavigate = () => {
    if (typeof window !== 'undefined') {
      window.location.href = profileHref
    }
  }

  const handleCardKeyDown = (event) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault()
      handleCardNavigate()
    }
  }

  const handleUpdateClick = (event) => {
    event.stopPropagation()
    onUpdate?.(player?.player_name)
  }

  return (
    <article
      className="main-character-card main-character-card-clickable"
      role="link"
      tabIndex={0}
      onClick={handleCardNavigate}
      onKeyDown={handleCardKeyDown}
    >
      <div className="main-character-avatar-link">
        {player.avatar ? (
          <img
            className="main-character-avatar"
            src={player.avatar}
            alt={`${primaryName} avatar`}
          />
        ) : (
          <span className="main-character-avatar-fallback">
            {primaryName.charAt(0).toUpperCase()}
          </span>
        )}
      </div>
      <div className="main-character-card-body">
        <div className="main-character-card-head">
          <span className="main-character-rank" style={{ background: rankBackground }}>
            top_{rank}
          </span>
          <button
            className="btn btn-secondary main-character-update-btn"
            type="button"
            onClick={handleUpdateClick}
            disabled={updating || updateState === 'all_ready'}
          >
            {buttonLabel}
          </button>
        </div>
        <h3 className={`main-character-player-name ${isLongName ? 'long-name' : ''}`}>{primaryName}</h3>
        <p>{getSecondaryDisplayName(player)}</p>
        <div className="main-character-meta-stack">
          <span className="stat-chip">last rating: {formatNumber(player.rating)}</span>
          <span className="stat-chip main-character-last-game-chip">
            <span className="main-character-last-game-label">last game:</span>
            <span className="main-character-last-game-date">{lastGameDate}</span>
          </span>
        </div>
      </div>
    </article>
  )
}

function MainCharacterBubbleChart({ players, hoveredPlayerName, onHoverPlayer }) {
  const points = useMemo(
    () =>
      (players || [])
        .map((player) => ({
          ...player,
          x: Number(player?.rating ?? player?.last_rating ?? player?.avg_game_rating ?? 0),
          y: Number(player?.score_rate ?? 0),
          games: Number(player?.n_games || 0)
        }))
        .filter(
          (player) =>
            Number.isFinite(player.x) &&
            Number.isFinite(player.y) &&
            Number.isFinite(player.games) &&
            player.games > 0
        ),
    [players]
  )
  const pointsForRender = useMemo(
    () => [...points].sort((a, b) => Number(b.games || 0) - Number(a.games || 0)),
    [points]
  )

  if (!points.length) {
    return <p className="result-line">No bubble chart data for this time control.</p>
  }

  const width = 960
  const height = 360
  const padLeft = 66
  const padRight = 24
  const padTop = 18
  const padBottom = 58
  const plotWidth = width - padLeft - padRight
  const plotHeight = height - padTop - padBottom

  const rawXMin = Math.min(...points.map((point) => point.x))
  const rawXMax = Math.max(...points.map((point) => point.x))
  const xPadding = Math.max(20, Math.round((rawXMax - rawXMin) * 0.08))
  const xMin = rawXMin - xPadding
  const xMax = rawXMax + xPadding
  const xRange = Math.max(1, xMax - xMin)

  const yMin = 0
  const yMax = 1
  const yRange = yMax - yMin

  const gamesMin = Math.min(...points.map((point) => point.games))
  const gamesMax = Math.max(...points.map((point) => point.games))
  const gamesRange = Math.max(1, gamesMax - gamesMin)

  const xScale = (value) =>
    xMax === xMin
      ? padLeft + plotWidth / 2
      : padLeft + ((value - xMin) / xRange) * plotWidth

  const yScale = (value) =>
    padTop + ((yMax - Math.max(yMin, Math.min(yMax, value))) / yRange) * plotHeight

  const bubbleRadius = (games) =>
    gamesMax === gamesMin
      ? 10
      : 6 + ((games - gamesMin) / gamesRange) * 14

  const bubbleColor = (scoreRate) => {
    const bounded = Math.max(0, Math.min(1, Number(scoreRate || 0)))
    const hue = Math.round(bounded * 120)
    return `hsl(${hue} 78% 46%)`
  }

  const ticks = [0, 0.25, 0.5, 0.75, 1]
  const xTickValues = [...new Set(ticks.map((ratio) => Math.round(xMin + ratio * xRange)))]
  const yTickValues = ticks

  return (
    <div className="main-character-scatter-shell">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="main-character-scatter"
        role="img"
        aria-label="main characters bubble chart by rating and score rate"
      >
        {yTickValues.map((tick, idx) => {
          const y = yScale(tick)
          return (
            <line
              key={`y-grid-${tick}-${idx}`}
              x1={padLeft}
              y1={y}
              x2={width - padRight}
              y2={y}
              className="main-character-scatter-grid-line"
            />
          )
        })}

        <line x1={padLeft} y1={padTop} x2={padLeft} y2={height - padBottom} className="axis-line" />
        <line x1={padLeft} y1={height - padBottom} x2={width - padRight} y2={height - padBottom} className="axis-line" />

        {xTickValues.map((tick, idx) => (
          <g key={`x-tick-${tick}-${idx}`}>
            <line
              x1={xScale(tick)}
              y1={height - padBottom}
              x2={xScale(tick)}
              y2={height - padBottom + 5}
              className="axis-line"
            />
            <text x={xScale(tick)} y={height - padBottom + 18} className="axis-tick-label" textAnchor="middle">
              {formatNumber(tick)}
            </text>
          </g>
        ))}

        {yTickValues.map((tick, idx) => (
          <g key={`y-tick-${tick}-${idx}`}>
            <line x1={padLeft - 5} y1={yScale(tick)} x2={padLeft} y2={yScale(tick)} className="axis-line" />
            <text x={padLeft - 9} y={yScale(tick) + 4} className="axis-tick-label" textAnchor="end">
              {`${Math.round(tick * 100)}%`}
            </text>
          </g>
        ))}

        <text
          x={padLeft + plotWidth / 2}
          y={height - 12}
          className="axis-title"
          textAnchor="middle"
        >
          last rating
        </text>
        <text
          x={18}
          y={padTop + plotHeight / 2}
          className="axis-title"
          textAnchor="middle"
          transform={`rotate(-90 18 ${padTop + plotHeight / 2})`}
        >
          wins
        </text>

        {pointsForRender.map((point) => {
          const isActive = hoveredPlayerName === point.player_name
          return (
            <circle
              key={`bubble-${point.player_name}`}
              cx={xScale(point.x)}
              cy={yScale(point.y)}
              r={isActive ? bubbleRadius(point.games) + 3 : bubbleRadius(point.games)}
              className={`main-character-scatter-point ${isActive ? 'active' : ''}`}
              style={{ fill: isActive ? '#ffffff' : bubbleColor(point.y) }}
              onMouseEnter={() => onHoverPlayer(point.player_name)}
              onFocus={() => onHoverPlayer(point.player_name)}
              tabIndex={0}
            />
          )
        })}
      </svg>
      <p className="main-character-hover-note">Bubble size = games played. Hover a point to inspect that player.</p>
    </div>
  )
}

function MainCharacters() {
  const [timeControlCounts, setTimeControlCounts] = useState(EMPTY_COUNTS)
  const [countsLoading, setCountsLoading] = useState(true)
  const [countsError, setCountsError] = useState('')

  const [selectedMode, setSelectedMode] = useState('')
  const [topPayload, setTopPayload] = useState(null)
  const [topLoading, setTopLoading] = useState(false)
  const [topError, setTopError] = useState('')
  const [hoveredPlayerName, setHoveredPlayerName] = useState('')
  const [topPlayersOffset, setTopPlayersOffset] = useState(0)
  const [updatingPlayers, setUpdatingPlayers] = useState({})
  const [updateStateByPlayer, setUpdateStateByPlayer] = useState({})
  const [updateMessage, setUpdateMessage] = useState('')
  const [addingPlayer, setAddingPlayer] = useState(false)
  const [showAddPlayerForm, setShowAddPlayerForm] = useState(false)
  const [addPlayerName, setAddPlayerName] = useState('')

  useEffect(() => {
    let alive = true

    const cached = readCachedCounts()
    if (cached) {
      setTimeControlCounts(cached)
      setCountsLoading(false)
      return () => {
        alive = false
      }
    }

    const loadCounts = async () => {
      setCountsLoading(true)
      setCountsError('')
      try {
        const payload = await fetchMainCharacterTimeControls()
        if (!alive) return
        const counts = {
          bullet: Number(payload?.bullet || 0),
          blitz: Number(payload?.blitz || 0),
          rapid: Number(payload?.rapid || 0)
        }
        setTimeControlCounts(counts)
        writeCachedCounts(counts)
      } catch (error) {
        if (!alive) return
        setTimeControlCounts(EMPTY_COUNTS)
        setCountsError(error instanceof Error ? error.message : 'Failed to load time controls.')
      } finally {
        if (alive) {
          setCountsLoading(false)
        }
      }
    }

    loadCounts()
    return () => {
      alive = false
    }
  }, [])

  const isGlobalUpdateBusy = useMemo(
    () => Object.values(updatingPlayers).some(Boolean),
    [updatingPlayers]
  )
  const isPipelineBusy = addingPlayer || isGlobalUpdateBusy

  const handleBadgeHoverMove = (event) => {
    const target = event.currentTarget
    const rect = target.getBoundingClientRect()
    const x = ((event.clientX - rect.left) / rect.width) * 100
    const y = ((event.clientY - rect.top) / rect.height) * 100
    target.style.setProperty('--hover-x', `${x}%`)
    target.style.setProperty('--hover-y', `${y}%`)
  }

  const handleBadgeHoverLeave = (event) => {
    const target = event.currentTarget
    target.style.setProperty('--hover-x', '50%')
    target.style.setProperty('--hover-y', '50%')
  }

  const handleSelectMode = async (mode) => {
    setSelectedMode(mode)
    setTopPayload(null)
    setTopError('')
    setTopLoading(true)
    setHoveredPlayerName('')
    setTopPlayersOffset(0)
    setUpdateMessage('')
    setShowAddPlayerForm(false)
    setAddPlayerName('')

    try {
      const payload = await fetchTopMainCharacters(mode, MAIN_CHARACTER_LIMIT)
      setTopPayload(payload)
    } catch (error) {
      setTopPayload(null)
      setTopError(error instanceof Error ? error.message : 'Failed to load main characters.')
    } finally {
      setTopLoading(false)
    }
  }

  const handleUpdatePlayer = async (playerName) => {
    const normalized = String(playerName || '').trim().toLowerCase()
    if (!normalized) return
    if (updateStateByPlayer[normalized] === 'all_ready') return
    if (isPipelineBusy) return

    setUpdateMessage('')
    setUpdateStateByPlayer((previous) => ({ ...previous, [normalized]: '' }))
    setUpdatingPlayers((previous) => ({ ...previous, [normalized]: true }))

    try {
      const payload = await updatePlayerGames(normalized)
      const message = payload?.message || `${normalized}: update started.`
      const resultState = classifyUpdateMessage(message)
      setUpdateStateByPlayer((previous) => ({ ...previous, [normalized]: resultState }))
      setUpdateMessage(resultState === 'all_ready' ? 'all games ready' : message)

      if (resultState === 'updated' && selectedMode) {
        setTopLoading(true)
        const refreshed = await fetchTopMainCharacters(selectedMode, MAIN_CHARACTER_LIMIT)
        setTopPayload(refreshed)
      }
    } catch (error) {
      setUpdateStateByPlayer((previous) => ({ ...previous, [normalized]: 'error' }))
      setUpdateMessage(error instanceof Error ? error.message : 'Failed to update games.')
    } finally {
      setUpdatingPlayers((previous) => ({ ...previous, [normalized]: false }))
      setTopLoading(false)
    }
  }

  const handleAddPlayer = async () => {
    if (isPipelineBusy) return
    const normalized = String(addPlayerName || '').trim().toLowerCase()
    if (!normalized) return

    setAddingPlayer(true)
    setUpdateMessage('')
    try {
      const payload = await createPlayerGames(normalized)
      setUpdateMessage(payload?.message || `Download started for ${normalized}.`)
      setAddPlayerName('')
      setShowAddPlayerForm(false)
      if (selectedMode) {
        setTopLoading(true)
        const refreshed = await fetchTopMainCharacters(selectedMode, MAIN_CHARACTER_LIMIT)
        setTopPayload(refreshed)
      }
      const countsPayload = await fetchMainCharacterTimeControls()
      const counts = {
        bullet: Number(countsPayload?.bullet || 0),
        blitz: Number(countsPayload?.blitz || 0),
        rapid: Number(countsPayload?.rapid || 0)
      }
      setTimeControlCounts(counts)
      writeCachedCounts(counts)
    } catch (error) {
      setUpdateMessage(error instanceof Error ? error.message : 'Failed to add player.')
    } finally {
      setAddingPlayer(false)
      setTopLoading(false)
    }
  }

  const topPlayers = useMemo(() => {
    if (!topPayload || !Array.isArray(topPayload.players)) {
      return []
    }
    return topPayload.players
  }, [topPayload])

  const sortedByRating = useMemo(
    () =>
      [...topPlayers].sort((a, b) => {
        const aRating = Number(a?.rating || 0)
        const bRating = Number(b?.rating || 0)
        if (aRating !== bRating) return bRating - aRating
        const aGames = Number(a?.n_games || 0)
        const bGames = Number(b?.n_games || 0)
        if (aGames !== bGames) return bGames - aGames
        return String(a?.player_name || '').localeCompare(String(b?.player_name || ''))
      }),
    [topPlayers]
  )

  const rankByName = useMemo(() => {
    const output = new Map()
    sortedByRating.forEach((player, idx) => {
      output.set(String(player?.player_name || ''), idx + 1)
    })
    return output
  }, [sortedByRating])

  const hoveredPlayer = useMemo(() => {
    if (!hoveredPlayerName) return null
    return sortedByRating.find((player) => player.player_name === hoveredPlayerName) || null
  }, [sortedByRating, hoveredPlayerName])
  const hoveredPlayerIndex = useMemo(
    () => sortedByRating.findIndex((player) => player.player_name === hoveredPlayerName),
    [sortedByRating, hoveredPlayerName]
  )

  useEffect(() => {
    if (!sortedByRating.length) {
      setHoveredPlayerName('')
      return
    }
    setHoveredPlayerName((currentName) =>
      sortedByRating.some((player) => player.player_name === currentName)
        ? currentName
        : String(sortedByRating[0]?.player_name || '')
    )
  }, [sortedByRating])

  useEffect(() => {
    setTopPlayersOffset((previous) => {
      const maxStart = Math.max(0, sortedByRating.length - TOP_PLAYERS_PAGE_SIZE)
      return Math.min(previous, maxStart)
    })
  }, [sortedByRating])

  const topPlayersPage = useMemo(
    () => sortedByRating.slice(topPlayersOffset, topPlayersOffset + TOP_PLAYERS_PAGE_SIZE),
    [sortedByRating, topPlayersOffset]
  )

  const handleTopPlayersPrevious = () => {
    setTopPlayersOffset((previous) => Math.max(0, previous - TOP_PLAYERS_PAGE_SIZE))
  }

  const handleTopPlayersNext = () => {
    const maxStart = Math.max(0, sortedByRating.length - TOP_PLAYERS_PAGE_SIZE)
    setTopPlayersOffset((previous) => Math.min(maxStart, previous + TOP_PLAYERS_PAGE_SIZE))
  }
  const handleHoveredPrevious = () => {
    if (!sortedByRating.length) return
    const current = hoveredPlayerIndex >= 0 ? hoveredPlayerIndex : 0
    const previousIndex = (current - 1 + sortedByRating.length) % sortedByRating.length
    setHoveredPlayerName(String(sortedByRating[previousIndex]?.player_name || ''))
  }

  const handleHoveredNext = () => {
    if (!sortedByRating.length) return
    const current = hoveredPlayerIndex >= 0 ? hoveredPlayerIndex : 0
    const nextIndex = (current + 1) % sortedByRating.length
    setHoveredPlayerName(String(sortedByRating[nextIndex]?.player_name || ''))
  }

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="games-main">
          <div className="games-hero-wrap">
            <h1
              className="games-hero-badge games-main-title badge-hover"
              onMouseMove={handleBadgeHoverMove}
              onMouseLeave={handleBadgeHoverLeave}
            >
              Main Characters
            </h1>
          </div>

          <div className="games-time-wrap">
            <h2
              className="games-hero-badge time-control-badge badge-hover"
              onMouseMove={handleBadgeHoverMove}
              onMouseLeave={handleBadgeHoverLeave}
            >
              Time Control
            </h2>
            <section className="games-time-controls" aria-label="Main character time controls">
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
                  <p>{countsLoading ? 'Loading...' : formatNumber(timeControlCounts.bullet)}</p>
                </button>
                <button
                  className="games-generality-card games-mode-button games-link-card"
                  type="button"
                  onClick={() => handleSelectMode('blitz')}
                >
                  <h3>blitz</h3>
                  <span className="time-control-icon time-control-icon-blitz" aria-hidden="true">⚡</span>
                  <p>{countsLoading ? 'Loading...' : formatNumber(timeControlCounts.blitz)}</p>
                </button>
                <button
                  className="games-generality-card games-mode-button games-link-card"
                  type="button"
                  onClick={() => handleSelectMode('rapid')}
                >
                  <h3>rapid</h3>
                  <span className="time-control-icon time-control-icon-rapid" aria-hidden="true">⏱</span>
                  <p>{countsLoading ? 'Loading...' : formatNumber(timeControlCounts.rapid)}</p>
                </button>
              </div>
              {countsError ? <p className="result-line">{countsError}</p> : null}
            </section>
          </div>

          {selectedMode ? (
            <section className="games-mode-detail" aria-label={`${selectedMode} players bubble chart`}>
              <div className="section-head analytics-section-head">
                <h2 className="games-action-title">{selectedMode}</h2>
                <div className="main-character-add-player-wrap">
                  {showAddPlayerForm ? (
                    <div className="main-character-add-player-pop">
                      <input
                        className="text-input main-character-add-player-input"
                        type="text"
                        value={addPlayerName}
                        onChange={(event) => setAddPlayerName(event.target.value)}
                        onKeyDown={(event) => {
                          if (event.key === 'Enter') handleAddPlayer()
                          if (event.key === 'Escape') {
                            setShowAddPlayerForm(false)
                            setAddPlayerName('')
                          }
                        }}
                        placeholder="player name"
                        disabled={isPipelineBusy}
                      />
                      <button
                        className="btn btn-secondary"
                        type="button"
                        onClick={handleAddPlayer}
                        disabled={isPipelineBusy || !String(addPlayerName || '').trim()}
                      >
                        {addingPlayer ? 'adding...' : 'go'}
                      </button>
                    </div>
                  ) : null}
                  <button
                    className="btn btn-secondary"
                    type="button"
                    onClick={() => setShowAddPlayerForm((previous) => !previous)}
                    disabled={isPipelineBusy}
                  >
                    add player
                  </button>
                </div>
              </div>
              {topLoading ? <p className="result-line">Loading main characters...</p> : null}
              {topError ? <p className="result-line">{topError}</p> : null}
              {!topLoading && !topError ? (
                sortedByRating.length ? (
                  <>
                    <MainCharacterBubbleChart
                      players={sortedByRating}
                      hoveredPlayerName={hoveredPlayerName}
                      onHoverPlayer={setHoveredPlayerName}
                    />
                    <div className="main-character-hover-grid">
                      {hoveredPlayer ? (
                        <div className="main-character-hover-nav">
                          <button
                            className="btn btn-secondary main-character-hover-nav-btn"
                            type="button"
                            onClick={handleHoveredNext}
                            aria-label="Previous player"
                          >
                            {'<'}
                          </button>
                          <MainCharacterCard
                            player={hoveredPlayer}
                            rank={rankByName.get(hoveredPlayer.player_name) || 1}
                            totalPlayers={sortedByRating.length}
                            onUpdate={handleUpdatePlayer}
                            updating={Boolean(
                              updatingPlayers[String(hoveredPlayer.player_name || '').toLowerCase()] ||
                              isPipelineBusy
                            )}
                            updateState={
                              updateStateByPlayer[String(hoveredPlayer.player_name || '').toLowerCase()] || ''
                            }
                          />
                          <button
                            className="btn btn-secondary main-character-hover-nav-btn"
                            type="button"
                            onClick={handleHoveredPrevious}
                            aria-label="Next player"
                          >
                            {'>'}
                          </button>
                        </div>
                      ) : (
                        <p className="result-line">Hover a point to inspect that player.</p>
                      )}
                    </div>
                    {updateMessage ? <p className="result-line">{updateMessage}</p> : null}
                  </>
                ) : (
                  <p className="result-line">No main characters found for this time control.</p>
                )
              ) : null}
            </section>
          ) : null}

              {selectedMode && !topLoading && !topError && sortedByRating.length ? (
            <section className="games-mode-detail" aria-label={`${selectedMode} top players`}>
              <div className="section-head">
                <h2 className="games-action-title">{selectedMode} top players</h2>
              </div>
              <div className="main-character-top-layout">
                <button
                  className="btn btn-secondary main-character-page-btn"
                  type="button"
                  onClick={handleTopPlayersPrevious}
                  disabled={topPlayersOffset <= 0}
                >
                  {'<'}
                </button>
                <div className="main-character-grid top-players-grid">
                  {topPlayersPage.map((player) => (
                  <MainCharacterCard
                    key={`top-player-card-${player.player_name}`}
                    player={player}
                    rank={rankByName.get(player.player_name) || 1}
                    totalPlayers={sortedByRating.length}
                    onUpdate={handleUpdatePlayer}
                    updating={Boolean(
                      updatingPlayers[String(player.player_name || '').toLowerCase()] || isPipelineBusy
                      )}
                      updateState={updateStateByPlayer[String(player.player_name || '').toLowerCase()] || ''}
                    />
                  ))}
                </div>
                <button
                  className="btn btn-secondary main-character-page-btn"
                  type="button"
                  onClick={handleTopPlayersNext}
                  disabled={topPlayersOffset + TOP_PLAYERS_PAGE_SIZE >= sortedByRating.length}
                >
                  {'>'}
                </button>
              </div>
            </section>
          ) : null}
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default MainCharacters
