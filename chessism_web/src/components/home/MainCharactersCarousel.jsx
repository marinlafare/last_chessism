import { useEffect, useState } from 'react'
import { API_BASE_URL } from '../../config'

const TIME_CONTROLS = ['blitz', 'rapid', 'bullet']
const PLAYER_LIMIT = 12

const formatNumber = (value) => {
  const numeric = Number(value ?? 0)
  if (!Number.isFinite(numeric)) return '0'
  return numeric.toLocaleString('en-US')
}

const getDisplayName = (player) => {
  const fullName = String(player?.full_name || '').trim()
  return fullName || String(player?.player_name || '').trim() || 'Unknown player'
}

const getPlayerRating = (player) => Number(player?.rating ?? player?.last_rating ?? player?.avg_game_rating ?? 0)

function MainCharactersCarousel() {
  const [selectedMode, setSelectedMode] = useState('blitz')
  const [players, setPlayers] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    const controller = new AbortController()

    const loadPlayers = async () => {
      setLoading(true)
      setError('')

      try {
        const response = await fetch(
          `${API_BASE_URL}/players/main_characters/top?time_control=${encodeURIComponent(selectedMode)}&limit=${PLAYER_LIMIT}`,
          { signal: controller.signal }
        )
        const payload = await response.json().catch(() => ({}))

        if (!response.ok) {
          throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
        }

        const sortedPlayers = Array.isArray(payload.players)
          ? [...payload.players].sort((a, b) => getPlayerRating(b) - getPlayerRating(a))
          : []
        setPlayers(sortedPlayers)
      } catch (err) {
        if (err.name !== 'AbortError') {
          setError(err.message || 'Main characters unavailable')
          setPlayers([])
        }
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false)
        }
      }
    }

    loadPlayers()
    return () => controller.abort()
  }, [selectedMode])

  return (
    <section className="main-characters-carousel" aria-labelledby="main-characters-title">
      <div className="carousel-head">
        <div>
          <p className="eyebrow">Player database</p>
          <h2 id="main-characters-title">Main Characters</h2>
        </div>
        <div className="carousel-actions">
          <div className="mode-tabs" aria-label="Time control">
            {TIME_CONTROLS.map((mode) => (
              <button
                className={`mode-tab ${selectedMode === mode ? 'active' : ''}`}
                type="button"
                key={mode}
                onClick={() => setSelectedMode(mode)}
              >
                {mode}
              </button>
            ))}
          </div>
        </div>
      </div>

      {error ? <div className="status-banner warn">{error}</div> : null}

      <div className="character-rating-list" aria-label={`${selectedMode} main character ratings`}>
        {loading
          ? Array.from({ length: 6 }).map((_, index) => (
              <div className="character-rating-row loading" key={`loading-${index}`} />
            ))
          : players.map((player, index) => {
              const playerName = String(player?.player_name || '').trim()
              const profileHref = `/players?player=${encodeURIComponent(playerName)}`

              return (
                <a className="character-rating-row" href={profileHref} key={playerName || index}>
                  <span className="character-rating-rank">#{index + 1}</span>
                  <span className="character-rating-name">{getDisplayName(player)}</span>
                  <strong className="character-rating-value">{formatNumber(getPlayerRating(player))}</strong>
                </a>
              )
            })}
      </div>

      <div className="carousel-footer">
        <span>{formatNumber(players.length)} loaded</span>
        <a className="btn btn-secondary btn-inline" href="/main_characters">Open table</a>
      </div>
    </section>
  )
}

export default MainCharactersCarousel
