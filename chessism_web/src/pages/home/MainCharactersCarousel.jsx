import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import { formatNumber } from '../../utils/formatters'
import { fetchTopMainCharacters } from './homeApi'
import { isPlainClick, normalizePlayer, playerHomeHref } from '../../services/navigation'

const PLAYER_PAGE_SIZE = 50
const PLAYER_FETCH_LIMIT = 5000

const getDisplayName = (player) => {
  const fullName = String(player?.full_name || '').trim()
  return fullName || String(player?.player_name || '').trim() || 'Unknown player'
}

const getPlayerRating = (player) => Number(player?.rating ?? player?.last_rating ?? player?.avg_game_rating ?? 0)

function MainCharactersCarousel({ selectedPlayer = '', onSelectPlayer, titledOnly = true, onTitleFilterChange }) {
  const [players, setPlayers] = useState([])
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')

  useEffect(() => {
    const controller = new AbortController()

    const loadPlayers = async () => {
      setLoading(true)
      setError('')
      setPlayers([])
      setPage(1)

      try {
        const payload = await fetchTopMainCharacters(
          'blitz',
          PLAYER_FETCH_LIMIT,
          { signal: controller.signal, titleStatus: titledOnly ? 'titled' : 'untitled' },
        )
        if (controller.signal.aborted) return
        const sortedPlayers = Array.isArray(payload.players)
          ? [...payload.players].sort((a, b) => getPlayerRating(b) - getPlayerRating(a))
          : []
        setPlayers(sortedPlayers)
        setPage(1)
      } catch (err) {
        if (!controller.signal.aborted && err.name !== 'AbortError') {
          setError(err.message || 'Player rankings unavailable')
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
  }, [titledOnly])

  const totalPages = Math.max(1, Math.ceil(players.length / PLAYER_PAGE_SIZE))
  const safePage = Math.min(page, totalPages)
  const pageStart = (safePage - 1) * PLAYER_PAGE_SIZE
  const pagePlayers = players.slice(pageStart, pageStart + PLAYER_PAGE_SIZE)
  const hasPagination = players.length > PLAYER_PAGE_SIZE

  return (
    <section {...pageStyleTarget('characters.panel')} className="main-characters-carousel" aria-labelledby="main-characters-title">
      <div className="carousel-head">
        <h2 {...pageStyleTarget('title')} className="character-list-title" id="main-characters-title">
          {titledOnly ? 'TITLED' : 'UNTITLED'}
        </h2>
        <button {...pageStyleTarget('button')} type="button" className="character-medal-toggle"
          aria-label="Titled players" aria-pressed={titledOnly}
          title={titledOnly ? 'Show untitled players · Blitz rating' : 'Show titled players · Blitz rating'}
          onClick={() => { onTitleFilterChange?.(!titledOnly); setPlayers([]); setPage(1); setLoading(true) }}>
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden="true">
            <path d="M7 3H3l6 8m8-8h4l-6 8M7 3l5 7 5-7" />
            <circle cx="12" cy="16" r="6" />
            <path d="m12 12 1.1 2.3 2.5.4-1.8 1.7.4 2.5-2.2-1.2-2.2 1.2.4-2.5-1.8-1.7 2.5-.4Z" fill="currentColor" stroke="none" />
          </svg>
        </button>
      </div>

      {error ? <div className="status-banner warn">{error}</div> : null}

      <div className="character-rating-list" aria-label={`${titledOnly ? 'Titled' : 'Untitled'} players by blitz rating`} aria-busy={loading}>
        {!loading && !error && !pagePlayers.length && <p className="character-rating-empty">No {titledOnly ? 'titled' : 'untitled'} players with blitz games yet.</p>}
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

      {hasPagination && <div className="carousel-footer">
        <div className="character-footer-actions">
          <div className="character-pagination" aria-label="Player ranking pages">
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
        </div>
      </div>}
    </section>
  )
}

export default MainCharactersCarousel
