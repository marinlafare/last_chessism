import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import { formatNumber } from '../../utils/formatters'
import { fetchTopMainCharacters } from './homeApi'
import { isPlainClick, normalizePlayer, playerHomeHref } from '../../services/navigation'

const TIME_CONTROLS = ['bullet', 'blitz', 'rapid']
const PLAYER_PAGE_SIZE = 50
const PLAYER_FETCH_LIMIT = 5000

const getDisplayName = (player) => {
  const fullName = String(player?.full_name || '').trim()
  return fullName || String(player?.player_name || '').trim() || 'Unknown player'
}

const getPlayerRating = (player) => Number(player?.rating ?? player?.last_rating ?? player?.avg_game_rating ?? 0)

function MainCharactersCarousel({ selectedPlayer = '', onSelectPlayer }) {
  const [selectedMode, setSelectedMode] = useState('bullet')
  const [players, setPlayers] = useState([])
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    const controller = new AbortController()

    const loadPlayers = async () => {
      setLoading(true)
      setError('')

      try {
        const payload = await fetchTopMainCharacters(
          selectedMode,
          PLAYER_FETCH_LIMIT,
          { signal: controller.signal },
        )
        const sortedPlayers = Array.isArray(payload.players)
          ? [...payload.players].sort((a, b) => getPlayerRating(b) - getPlayerRating(a))
          : []
        setPlayers(sortedPlayers)
        setPage(1)
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

  const totalPages = Math.max(1, Math.ceil(players.length / PLAYER_PAGE_SIZE))
  const safePage = Math.min(page, totalPages)
  const pageStart = (safePage - 1) * PLAYER_PAGE_SIZE
  const pagePlayers = players.slice(pageStart, pageStart + PLAYER_PAGE_SIZE)
  const pageEnd = pageStart + pagePlayers.length
  const hasPagination = players.length > PLAYER_PAGE_SIZE
  const rangeLabel = players.length > 0
    ? `${formatNumber(pageEnd)} / ${formatNumber(players.length)}`
    : '0 of 0'

  return (
    <section {...pageStyleTarget('characters.panel')} className="main-characters-carousel" aria-labelledby="main-characters-title">
      <div className="carousel-head">
        <div>
          <p {...pageStyleTarget('text')} className="eyebrow">Player database</p>
          <h2 {...pageStyleTarget('title')} id="main-characters-title">Main Characters</h2>
        </div>
        <div className="carousel-actions">
          <div className="mode-tabs" aria-label="Time control">
            {TIME_CONTROLS.map((mode) => (
              <button {...pageStyleTarget('button')}
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
          ? Array.from({ length: 10 }).map((_, index) => (
              <div className="character-rating-row loading" key={`loading-${index}`} />
            ))
          : pagePlayers.map((player, index) => {
              const playerName = String(player?.player_name || '').trim()
              const profileHref = playerHomeHref(playerName)
              const selected = normalizePlayer(playerName) === selectedPlayer
              const absoluteIndex = pageStart + index

              return (
                <a {...pageStyleTarget('link')} className={`character-rating-row${selected ? ' selected' : ''}`} href={profileHref}
                  key={playerName || index} aria-current={selected ? 'true' : undefined}
                  onClick={event => {
                    if (!onSelectPlayer || !isPlainClick(event)) return
                    event.preventDefault()
                    onSelectPlayer(playerName)
                  }}>
                  <span className="character-rating-rank">#{absoluteIndex + 1}</span>
                  <span className="character-rating-name">{getDisplayName(player)}</span>
                  <strong {...pageStyleTarget('value')} className="character-rating-value">{formatNumber(getPlayerRating(player))}</strong>
                </a>
              )
            })}
      </div>

      <div className="carousel-footer">
        <span>
          {loading
            ? 'Loading'
            : rangeLabel}
        </span>
        {hasPagination ? (
          <div className="character-pagination" aria-label="Main character pages">
            <button {...pageStyleTarget('button')}
              className="btn btn-secondary btn-inline"
              type="button"
              disabled={safePage <= 1}
              onClick={() => setPage((current) => Math.max(1, current - 1))}
            >
              Previous
            </button>
            <span>{safePage} / {totalPages}</span>
            <button {...pageStyleTarget('button')}
              className="btn btn-secondary btn-inline"
              type="button"
              disabled={safePage >= totalPages}
              onClick={() => setPage((current) => Math.min(totalPages, current + 1))}
            >
              Next
            </button>
          </div>
        ) : null}
      </div>
    </section>
  )
}

export default MainCharactersCarousel
