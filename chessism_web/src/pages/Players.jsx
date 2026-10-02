import { useEffect, useRef, useState } from 'react'
import './players/players.css'
import SideRail from '../components/layout/SideRail'
import PlayerAnalysisWorkspace from './players/PlayerAnalysisWorkspace'
import { formatNumber } from '../utils/formatters'
import {
  PLAYER_DELETE_JOB_STORAGE_KEY,
  UPDATE_JOB_STORAGE_KEY,
  formatStatusMessage,
  isTerminalJobStatus,
  loadStoredJob,
  storeJob
} from '../services/gameJobService'
import {
  deletePlayer,
  downloadPlayerGames,
  fetchPlayerDeletionPreview,
  fetchPlayerHours,
  fetchPlayerJobStatus,
  fetchPlayerNavigation,
  fetchPlayerNeighbors,
  fetchPlayerPositionStats,
  fetchPlayerProfile,
  updatePlayerGames,
} from './players/playerApi'

function GameUpdateProgress({ status, message }) {
  const progress = status?.progress
  if (!progress) {
    return message ? <p className="result-line players-update-message">{message}</p> : null
  }

  const phase = progress.phase || status?.status || 'working'
  const total = Math.max(1, Number(progress.total || 1))
  const processed = Math.min(total, Number(progress.processed || 0))
  const percent = phase === 'complete'
    ? 100
    : Math.min(100, Math.round((processed / total) * 100))

  return (
    <div className="job-progress players-update-progress" aria-live="polite">
      <div className="job-progress-head">
        <strong>{phase}</strong>
        <span>{percent}%</span>
      </div>
      <div className="job-progress-track">
        <div
          className={`job-progress-fill ${phase === 'failed' ? 'failed' : ''}`}
          style={{ width: `${percent}%` }}
        />
      </div>
      <p className="result-line">{progress.detail || message || `${processed} of ${total}`}</p>
    </div>
  )
}

function Players() {
  const [activePlayer, setActivePlayer] = useState('')
  const [loading, setLoading] = useState(false)
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')
  const [profile, setProfile] = useState(null)
  const [playerHours, setPlayerHours] = useState(null)
  const [playerPositionStats, setPlayerPositionStats] = useState(null)
  const [playerNeighbors, setPlayerNeighbors] = useState(null)
  const [playerMenuOpen, setPlayerMenuOpen] = useState(false)
  const [playerMenuPlayers, setPlayerMenuPlayers] = useState([])
  const [playerMenuLoading, setPlayerMenuLoading] = useState(false)
  const [playerMenuError, setPlayerMenuError] = useState('')
  const [gamesUpdateLoading, setGamesUpdateLoading] = useState(false)
  const [gamesUpdateJob, setGamesUpdateJob] = useState(() => loadStoredJob(UPDATE_JOB_STORAGE_KEY))
  const [gamesUpdateStatus, setGamesUpdateStatus] = useState(null)
  const [gamesUpdateMessage, setGamesUpdateMessage] = useState('')
  const [deletePreview, setDeletePreview] = useState(null)
  const [deletePreviewLoading, setDeletePreviewLoading] = useState(false)
  const [deleteConfirmation, setDeleteConfirmation] = useState('')
  const [deleteError, setDeleteError] = useState('')
  const [deleteSubmitting, setDeleteSubmitting] = useState(false)
  const [deleteJob, setDeleteJob] = useState(() => loadStoredJob(PLAYER_DELETE_JOB_STORAGE_KEY))
  const [deleteStatus, setDeleteStatus] = useState(null)
  const [deleteMessage, setDeleteMessage] = useState('')
  const playerPickerRef = useRef(null)

  const loadPlayer = async (requestedName) => {
    const name = String(requestedName || '').trim().toLowerCase()
    if (!name) {
      setError('No player was selected.')
      return
    }

    setLoading(true)
    setError('')
    setMessage('')

    try {
      let playerProfile = await fetchPlayerProfile(name)

      if ((playerProfile?.joined === null || playerProfile?.joined === 0) && !playerProfile?.deleted_at) {
        const download = await downloadPlayerGames(name)
        setMessage(download.message || 'Downloading games and updating profile…')
        playerProfile = await fetchPlayerProfile(name)
      }

      const normalizedPlayer = String(playerProfile?.player_name || name).toLowerCase()
      setActivePlayer(normalizedPlayer)
      setProfile(playerProfile)

      if (playerProfile?.deleted_at) {
        setPlayerHours(null)
        setPlayerPositionStats(null)
        setPlayerNeighbors(null)
        setMessage(`${normalizedPlayer} was deleted and is retained only as a shared-game shell.`)
        return
      }

      const [hoursPayload, positionPayload, neighborPayload] = await Promise.all([
        fetchPlayerHours(normalizedPlayer),
        fetchPlayerPositionStats(normalizedPlayer),
        fetchPlayerNeighbors(normalizedPlayer)
      ])

      setPlayerHours(hoursPayload)
      setPlayerPositionStats(positionPayload)
      setPlayerNeighbors(neighborPayload)
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : 'Unable to load player.')
      setProfile(null)
      setPlayerHours(null)
      setPlayerPositionStats(null)
      setPlayerNeighbors(null)
    } finally {
      setLoading(false)
    }
  }

  const handleGamesUpdate = async () => {
    if (!activePlayer || gamesUpdateLoading || gamesUpdateJob?.jobId) return

    setGamesUpdateLoading(true)
    setGamesUpdateMessage('')
    setGamesUpdateStatus(null)
    try {
      const payload = await updatePlayerGames(activePlayer)
      if (payload.job_id) {
        const job = {
          jobId: payload.job_id,
          playerName: payload.player_name || activePlayer
        }
        storeJob(UPDATE_JOB_STORAGE_KEY, job)
        setGamesUpdateJob(job)
      }
      setGamesUpdateMessage(payload.message || `Games update started for ${activePlayer}.`)
    } catch (updateError) {
      setGamesUpdateMessage(updateError instanceof Error ? updateError.message : 'Unable to start games update.')
    } finally {
      setGamesUpdateLoading(false)
    }
  }

  const closeDeletePreview = () => {
    if (deleteSubmitting) return
    setDeletePreview(null)
    setDeleteConfirmation('')
    setDeleteError('')
  }

  const handleOpenDeletePreview = async () => {
    if (!activePlayer || deletePreviewLoading || deleteJob?.jobId) return
    setDeletePreviewLoading(true)
    setDeleteError('')
    setDeleteConfirmation('')
    try {
      const preview = await fetchPlayerDeletionPreview(activePlayer)
      setDeletePreview(preview)
    } catch (previewError) {
      setDeleteError(previewError instanceof Error ? previewError.message : 'Unable to preview player deletion.')
    } finally {
      setDeletePreviewLoading(false)
    }
  }

  const handleConfirmDeletePlayer = async () => {
    if (!deletePreview || deleteSubmitting) return
    setDeleteSubmitting(true)
    setDeleteError('')
    try {
      const payload = await deletePlayer(activePlayer, {
        confirmation: deleteConfirmation,
        expected_exclusive_games: Number(deletePreview.exclusive_games || 0),
        expected_shared_games: Number(deletePreview.shared_games || 0),
      })
      const redirectPlayer = [playerNeighbors?.next_player, playerNeighbors?.previous_player]
        .find((candidate) => candidate && candidate.toLowerCase() !== activePlayer.toLowerCase()) || ''
      const job = {
        jobId: payload.job_id,
        playerName: payload.player_name || activePlayer,
        redirectPlayer
      }
      storeJob(PLAYER_DELETE_JOB_STORAGE_KEY, job)
      setDeleteJob(job)
      setDeleteStatus(null)
      setDeleteMessage(payload.message || `Deletion queued for ${activePlayer}.`)
      setDeletePreview(null)
      setDeleteConfirmation('')
    } catch (deleteRequestError) {
      setDeleteError(deleteRequestError instanceof Error ? deleteRequestError.message : 'Unable to queue player deletion.')
    } finally {
      setDeleteSubmitting(false)
    }
  }

  const navigateToPlayer = (playerName) => {
    const nextPlayer = String(playerName || '').trim().toLowerCase()
    if (!nextPlayer || nextPlayer === activePlayer.toLowerCase()) return

    setPlayerMenuOpen(false)
    const nextUrl = `/players?player=${encodeURIComponent(nextPlayer)}`
    window.history.pushState(null, '', nextUrl)
    window.dispatchEvent(new PopStateEvent('popstate'))
  }

  const togglePlayerMenu = async () => {
    if (playerMenuOpen) {
      setPlayerMenuOpen(false)
      return
    }
    setPlayerMenuOpen(true)
    if (playerMenuPlayers.length || playerMenuLoading) return

    setPlayerMenuLoading(true)
    setPlayerMenuError('')
    try {
      const payload = await fetchPlayerNavigation()
      setPlayerMenuPlayers(
        (Array.isArray(payload?.players) ? payload.players : [])
          .map((name) => String(name || '').trim().toLowerCase())
          .filter(Boolean)
          .sort((left, right) => left.localeCompare(right))
      )
    } catch (navigationError) {
      setPlayerMenuError(
        navigationError instanceof Error ? navigationError.message : 'Unable to load players.'
      )
    } finally {
      setPlayerMenuLoading(false)
    }
  }

  useEffect(() => {
    const playerFromQuery = new URLSearchParams(window.location.search).get('player')
    loadPlayer(playerFromQuery)
  }, [])

  useEffect(() => {
    if (!playerMenuOpen) return undefined

    const closeOnOutsideClick = (event) => {
      if (!playerPickerRef.current?.contains(event.target)) setPlayerMenuOpen(false)
    }
    const closeOnEscape = (event) => {
      if (event.key === 'Escape') setPlayerMenuOpen(false)
    }
    document.addEventListener('pointerdown', closeOnOutsideClick)
    document.addEventListener('keydown', closeOnEscape)
    return () => {
      document.removeEventListener('pointerdown', closeOnOutsideClick)
      document.removeEventListener('keydown', closeOnEscape)
    }
  }, [playerMenuOpen])

  useEffect(() => {
    const job = gamesUpdateJob
    if (!job?.jobId) return undefined

    let cancelled = false
    let timerId

    const poll = async () => {
      try {
        const status = await fetchPlayerJobStatus(job.jobId)
        if (cancelled) return
        setGamesUpdateStatus(status)

        if (isTerminalJobStatus(status)) {
          const failed = status.progress?.phase === 'failed' || status.result?.success === false
          const resultMessage = formatStatusMessage(
            status.progress?.result || status.progress?.detail || status.result?.result
          )
          setGamesUpdateMessage(resultMessage || (failed ? 'Games update failed.' : 'Games update finished.'))
          storeJob(UPDATE_JOB_STORAGE_KEY, null)
          setGamesUpdateJob(null)

          if (!failed && activePlayer.toLowerCase() === String(job.playerName || '').toLowerCase()) {
            await loadPlayer(job.playerName)
          }
          return
        }

        timerId = window.setTimeout(poll, 1500)
      } catch (pollError) {
        if (cancelled) return
        const detail = pollError instanceof Error ? pollError.message : 'Unable to read update status.'
        setGamesUpdateStatus({ status: 'not_found', progress: { phase: 'failed', detail } })
        setGamesUpdateMessage(detail)
        storeJob(UPDATE_JOB_STORAGE_KEY, null)
        setGamesUpdateJob(null)
      }
    }

    poll()
    return () => {
      cancelled = true
      window.clearTimeout(timerId)
    }
  }, [gamesUpdateJob?.jobId, activePlayer])

  useEffect(() => {
    const job = deleteJob
    if (!job?.jobId) return undefined

    let cancelled = false
    let timerId

    const poll = async () => {
      try {
        const status = await fetchPlayerJobStatus(job.jobId)
        if (cancelled) return
        setDeleteStatus(status)
        if (isTerminalJobStatus(status)) {
          const failed = status.progress?.phase === 'failed' || status.result?.success === false
          const resultMessage = formatStatusMessage(
            status.progress?.result || status.progress?.detail || status.result?.result
          )
          setDeleteMessage(resultMessage || (failed ? 'Player deletion failed.' : 'Player deletion finished.'))
          storeJob(PLAYER_DELETE_JOB_STORAGE_KEY, null)
          setDeleteJob(null)
          if (!failed) {
            if (job.redirectPlayer) {
              navigateToPlayer(job.redirectPlayer)
            } else {
              window.history.pushState(null, '', '/main_characters')
              window.dispatchEvent(new PopStateEvent('popstate'))
            }
          }
          return
        }
        timerId = window.setTimeout(poll, 1500)
      } catch (pollError) {
        if (cancelled) return
        const detail = pollError instanceof Error ? pollError.message : 'Unable to read deletion status.'
        setDeleteStatus({ status: 'not_found', progress: { phase: 'failed', detail } })
        setDeleteMessage(detail)
        storeJob(PLAYER_DELETE_JOB_STORAGE_KEY, null)
        setDeleteJob(null)
      }
    }

    poll()
    return () => {
      cancelled = true
      window.clearTimeout(timerId)
    }
  }, [deleteJob?.jobId])

  const joinedDisplay = profile?.joined && Number(profile.joined) > 0
    ? new Date(Number(profile.joined) * 1000)
        .toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: '2-digit', timeZone: 'UTC' })
        .replace(',', '')
        .toUpperCase()
    : null
  const profileTitle = loading && !profile ? 'Loading player…' : activePlayer || 'Player profile'
  const profileAvatar = String(profile?.avatar || '').trim()
  const profileInitial = String(profile?.player_name || '?').trim().charAt(0).toUpperCase() || '?'
  const countryCode = String(profile?.country || '').trim().toUpperCase() || 'N/A'
  const followersDisplay = profile?.followers === null || profile?.followers === undefined
    ? 'N/A'
    : formatNumber(profile.followers)
  const totalPlayerGames = Number(playerPositionStats?.total_games || 0)
  const analyzedPlayerGames = Number(playerPositionStats?.analyzed_games || 0)
  const totalPlayerFens = Number(playerPositionStats?.total_fens ?? playerPositionStats?.total_positions ?? 0)
  const analyzedPlayerFens = Number(playerPositionStats?.analyzed_fens ?? playerPositionStats?.analyzed_positions ?? 0)
  const analyzedPlayerGamePercent = totalPlayerGames > 0
    ? Math.min(100, (analyzedPlayerGames / totalPlayerGames) * 100)
    : 0
  const analyzedPlayerFenPercent = totalPlayerFens > 0
    ? Math.min(100, (analyzedPlayerFens / totalPlayerFens) * 100)
    : 0
  const gamesUpdateActive = Boolean(gamesUpdateJob?.jobId && !isTerminalJobStatus(gamesUpdateStatus))
  const deleteActive = Boolean(deleteJob?.jobId && !isTerminalJobStatus(deleteStatus))
  const deleteConfirmationMatches = deleteConfirmation.trim().toLowerCase() === activePlayer.toLowerCase()
  const modeActivity = ['bullet', 'blitz', 'rapid'].map((mode) => ({
    mode,
    games: Number(playerHours?.[`${mode}_games`] || 0),
    hours: Math.round(Number(playerHours?.[`${mode}_hours`] || 0)),
  }))
  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <main className="games-main players-starting-point">
          {message ? <p className="result-line">{message}</p> : null}
          {error ? <p className="result-line">{error}</p> : null}

          <section className="players-top-grid">
            <div className="games-mode-detail players-profile-navigation-section">
              <div className="section-head">
                <div className="players-title-navigation">
                  <button
                    className="players-title-arrow"
                    type="button"
                    aria-label={`Previous player${playerNeighbors?.previous_player ? `: ${playerNeighbors.previous_player}` : ''}`}
                    title={playerNeighbors?.previous_player || 'Previous player'}
                    disabled={!playerNeighbors?.previous_player || loading}
                    onClick={() => navigateToPlayer(playerNeighbors?.previous_player)}
                  >
                    <svg viewBox="0 0 20 20" aria-hidden="true">
                      <path d="M12.5 4.5 7 10l5.5 5.5" />
                    </svg>
                  </button>
                  <div className="players-title-picker" ref={playerPickerRef}>
                    <button
                      className="games-action-title players-title-select"
                      type="button"
                      aria-haspopup="menu"
                      aria-expanded={playerMenuOpen}
                      aria-controls="player-navigation-menu"
                      disabled={loading}
                      onClick={togglePlayerMenu}
                    >
                      <span>{profileTitle}</span>
                    </button>
                    {playerMenuOpen ? (
                      <div className="players-title-menu" id="player-navigation-menu" role="menu">
                        {playerMenuLoading ? <p>Loading players…</p> : null}
                        {playerMenuError ? <p className="error">{playerMenuError}</p> : null}
                        {!playerMenuLoading && !playerMenuError ? playerMenuPlayers
                          .filter((name) => name !== activePlayer.toLowerCase())
                          .map((name) => (
                            <button
                              type="button"
                              role="menuitem"
                              key={name}
                              onClick={() => navigateToPlayer(name)}
                            >
                              {name}
                            </button>
                          )) : null}
                        {!playerMenuLoading && !playerMenuError && playerMenuPlayers.length <= 1 ? (
                          <p>No other players available.</p>
                        ) : null}
                      </div>
                    ) : null}
                  </div>
                  <button
                    className="players-title-arrow"
                    type="button"
                    aria-label={`Next player${playerNeighbors?.next_player ? `: ${playerNeighbors.next_player}` : ''}`}
                    title={playerNeighbors?.next_player || 'Next player'}
                    disabled={!playerNeighbors?.next_player || loading}
                    onClick={() => navigateToPlayer(playerNeighbors?.next_player)}
                  >
                    <svg viewBox="0 0 20 20" aria-hidden="true">
                      <path d="m7.5 4.5 5.5 5.5-5.5 5.5" />
                    </svg>
                  </button>
                </div>
                <div className="players-profile-actions">
                  <button
                    className="btn btn-primary btn-inline players-update-button"
                    type="button"
                    title="Fetch games from the player's latest saved month through the current month"
                    disabled={!activePlayer || loading || gamesUpdateLoading || gamesUpdateActive || deleteActive}
                    onClick={handleGamesUpdate}
                  >
                    {gamesUpdateLoading ? 'queueing…' : gamesUpdateActive ? 'updating…' : 'update'}
                  </button>
                  <button
                    className="btn btn-danger btn-inline players-delete-button"
                    type="button"
                    disabled={!activePlayer || loading || deletePreviewLoading || deleteActive || gamesUpdateActive || Boolean(profile?.deleted_at)}
                    onClick={handleOpenDeletePreview}
                  >
                    {deletePreviewLoading ? 'checking…' : deleteActive ? 'deleting…' : 'delete'}
                  </button>
                </div>
              </div>
            </div>

            <div className="games-mode-detail players-profile-section">
              <GameUpdateProgress status={gamesUpdateStatus} message={gamesUpdateMessage} />
              <GameUpdateProgress status={deleteStatus} message={deleteMessage || deleteError} />
              <div className="players-profile-layout">
                <div className="players-profile-avatar-box">
                  {profileAvatar ? (
                    <img className="players-profile-avatar" src={profileAvatar} alt={`${profileTitle} avatar`} />
                  ) : (
                    <span className="players-profile-avatar-fallback">{profileInitial}</span>
                  )}
                </div>
                {profile ? (
                  <>
                    <div className="players-profile-details">
                      <div className="players-profile-metadata">
                        {[
                          ['Player', profile.player_name || 'N/A'],
                          ['Country', countryCode],
                          ['Name', profile.name || 'N/A'],
                          ['Joined', joinedDisplay || 'N/A'],
                        ].map(([label, value]) => (
                          <div className="profile-item" key={label}>
                            <span>{label}</span>
                            <strong>{String(value)}</strong>
                          </div>
                        ))}
                        <div className="players-followers-card">
                          <span>Followers</span>
                          <strong>{followersDisplay}</strong>
                        </div>
                        <div className="players-mode-activity-row">
                          {modeActivity.map(({ mode, games, hours }) => (
                            <p className="players-mode-activity" key={mode}>
                              <strong>{mode}:</strong> {formatNumber(games)} | {formatNumber(hours)} hrs
                            </p>
                          ))}
                        </div>
                      </div>
                    </div>
                    <div className="players-position-summary" aria-label="Player game and FEN analysis coverage">
                      <div className="players-position-stat">
                        <span>Total games</span>
                        <strong>{formatNumber(totalPlayerGames)}</strong>
                        <small>games in database</small>
                      </div>
                      <div className="players-position-stat players-position-stat-analyzed">
                        <span>Games analyzed</span>
                        <strong>{formatNumber(analyzedPlayerGames)}</strong>
                        <small>{analyzedPlayerGamePercent.toFixed(1)}% fully analyzed</small>
                      </div>
                      <div className="players-position-stat">
                        <span>FENs</span>
                        <strong>{formatNumber(totalPlayerFens)}</strong>
                        <small>positions across games</small>
                      </div>
                      <div className="players-position-stat players-position-stat-analyzed">
                        <span>FENs analyzed</span>
                        <strong>{formatNumber(analyzedPlayerFens)}</strong>
                        <small>{analyzedPlayerFenPercent.toFixed(1)}% coverage</small>
                      </div>
                    </div>
                  </>
                ) : (
                  <p className="result-line">{loading ? 'Loading profile…' : 'No profile loaded.'}</p>
                )}
              </div>
            </div>

          </section>

          <PlayerAnalysisWorkspace
            playerName={activePlayer}
            disabled={!profile || loading || Boolean(profile?.deleted_at)}
          />
          {deletePreview ? (
            <div className="player-delete-backdrop" onClick={closeDeletePreview}>
              <div
                className="player-delete-dialog"
                role="alertdialog"
                aria-modal="true"
                aria-labelledby="player-delete-title"
                onClick={(event) => event.stopPropagation()}
              >
                <div className="player-delete-heading">
                  <div>
                    <span>Permanent database operation</span>
                    <h2 id="player-delete-title">Delete {activePlayer}?</h2>
                  </div>
                  <button className="btn btn-secondary btn-inline" type="button" onClick={closeDeletePreview}>Cancel</button>
                </div>
                <div className="player-delete-preview-grid">
                  <div className="danger">
                    <span>Games deleted</span>
                    <strong>{formatNumber(deletePreview.exclusive_games)}</strong>
                  </div>
                  <div>
                    <span>Shared games kept</span>
                    <strong>{formatNumber(deletePreview.shared_games)}</strong>
                  </div>
                  <div>
                    <span>Game/FEN links removed</span>
                    <strong>{formatNumber(deletePreview.fen_associations)}</strong>
                  </div>
                  <div>
                    <span>Analyzed links affected</span>
                    <strong>{formatNumber(deletePreview.analyzed_fen_associations)}</strong>
                  </div>
                </div>
                <div className="player-delete-preservation">
                  <strong>FEN rows and engine analysis deleted: 0</strong>
                  <span>Scores, WDL, PVs, continuations and tablebase results remain stored.</span>
                </div>
                <label className="player-delete-confirm-field">
                  <span>Type <strong>{activePlayer}</strong> to confirm</span>
                  <input
                    className="text-input"
                    value={deleteConfirmation}
                    onChange={(event) => setDeleteConfirmation(event.target.value)}
                    autoComplete="off"
                    spellCheck="false"
                  />
                </label>
                {deleteError ? <p className="player-delete-error">{deleteError}</p> : null}
                <div className="player-delete-actions">
                  <button className="btn btn-secondary" type="button" onClick={closeDeletePreview} disabled={deleteSubmitting}>Cancel</button>
                  <button
                    className="btn btn-danger"
                    type="button"
                    disabled={!deleteConfirmationMatches || deleteSubmitting}
                    onClick={handleConfirmDeletePlayer}
                  >
                    {deleteSubmitting ? 'Queueing deletion…' : `Delete ${formatNumber(deletePreview.exclusive_games)} games`}
                  </button>
                </div>
              </div>
            </div>
          ) : null}
        </main>
      </div>
    </div>
  )
}

export default Players
