import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useCallback, useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { formatNumber } from '../../utils/formatters'
import { explorePlayerGames } from './playerAnalysisApi.js?game-explorer=b10cbe3'

const emptyRequest = { data: null, error: '', loading: false }
const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

function resultLabel(value) {
  if (value === 'win') return 'W'
  if (value === 'draw') return 'D'
  if (value === 'loss') return 'L'
  return '—'
}

function selectionTitle(selection) {
  if (!selection) return 'Analyzed games'
  if (selection.scope === 'rating') return `Games rated ${selection.rating_from}–${selection.rating_from + 9}`
  if (selection.scope === 'result') return { win: 'Won games', draw: 'Drawn games', loss: 'Lost games' }[selection.result]
  if (selection.scope === 'opponent') return `Games against ${selection.opponent}`
  if (selection.scope === 'opening') return `${selection.label || 'Unknown opening'} · ${selection.color || 'both colors'}`
  if (selection.scope === 'date') return `Games on ${selection.date}`
  if (selection.scope === 'hour') return `Games started at ${String(selection.hour).padStart(2, '0')}:00`
  if (selection.scope === 'weekday_hour') {
    return `${selection.label} at ${String(selection.hour).padStart(2, '0')}:00`
  }
  return `${selection.label} games`
}

function GameListDate({ value }) {
  // Keep the API's local clock; parsing as a Date would apply another timezone.
  const parts = String(value || '').match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}:\d{2})/)
  const month = parts && monthNames[Number(parts[2]) - 1]
  if (!month) return <span className="game-list-date"><span {...pageStyleTarget('gameExplorer.dateBadge')} className="game-list-date-badge">Unknown date</span></span>
  return <time className="game-list-date" dateTime={value}>
    <span {...pageStyleTarget('gameExplorer.dateBadge')} className="game-list-date-badge game-list-date-year">{parts[1]}</span>{' '}
    <span {...pageStyleTarget('gameExplorer.dateBadge')} className="game-list-date-badge game-list-date-month">{month} {parts[3]}</span>{' '}
    <span {...pageStyleTarget('gameExplorer.dateBadge')} className="game-list-date-badge game-list-date-time">{parts[4]}</span>
  </time>
}

export default function PlayerGameExplorerModal({ playerName, selection, onClose, onOpenGame }) {
  const [request, setRequest] = useState(emptyRequest)
  const paginationController = useRef(null)
  const dataSource = useRef(null)
  const listRef = useRef(null)
  const paginationSentinel = useRef(null)
  const pagination = request.data?.pagination

  useEffect(() => {
    dataSource.current = null
    if (!selection || !playerName) return undefined
    const controller = new AbortController()
    setRequest({ data: null, error: '', loading: true })
    explorePlayerGames({
      playerName,
      selection: { ...selection, limit: 30 },
      signal: controller.signal,
    })
      .then((data) => {
        if (!controller.signal.aborted) {
          dataSource.current = { playerName, selection }
          setRequest({ data, error: '', loading: false })
        }
      })
      .catch((error) => {
        if (!controller.signal.aborted && error.name !== 'AbortError') {
          setRequest({ data: null, error: error.message || 'Unable to load these games.', loading: false })
        }
      })
    return () => {
      controller.abort()
      paginationController.current?.abort()
      paginationController.current = null
      dataSource.current = null
    }
  }, [playerName, selection])

  useEffect(() => {
    if (!selection) return undefined
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    const closeWithEscape = (event) => {
      if (event.target?.closest?.('[data-appearance-tools]')) return
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', closeWithEscape)
    return () => {
      document.body.style.overflow = previousOverflow
      window.removeEventListener('keydown', closeWithEscape)
    }
  }, [onClose, selection])

  const loadMore = useCallback(async () => {
    const cursor = pagination?.next_cursor
    // The ref locks immediately, before React renders the loading state.
    // Never reuse the previous player's/filter's cursor during a fresh request.
    if (!cursor || !pagination.has_more || request.loading || paginationController.current
      || dataSource.current?.playerName !== playerName || dataSource.current?.selection !== selection) return
    const controller = new AbortController()
    paginationController.current = controller
    setRequest((current) => ({ ...current, error: '', loading: true }))
    try {
      const next = await explorePlayerGames({
        playerName,
        selection: { ...selection, limit: 30, cursor },
        signal: controller.signal,
      })
      if (controller.signal.aborted) return
      setRequest((current) => ({
        data: {
          ...next,
          games: [...(current.data?.games || []), ...next.games],
        },
        error: '',
        loading: false,
      }))
    } catch (error) {
      if (!controller.signal.aborted && error.name !== 'AbortError') {
        setRequest((current) => ({ ...current, error: error.message || 'Unable to load more games.', loading: false }))
      }
    } finally {
      if (paginationController.current === controller) paginationController.current = null
    }
  }, [pagination, playerName, request.loading, selection])

  useEffect(() => {
    const list = listRef.current
    const sentinel = paginationSentinel.current
    if (!list || !sentinel || !pagination?.has_more || request.loading || request.error) return undefined
    let active = true
    const observer = new IntersectionObserver((entries) => {
      if (active && entries.some((entry) => entry.isIntersecting)) loadMore()
    }, { root: list, rootMargin: '0px 0px 120px 0px' })
    observer.observe(sentinel)
    return () => {
      active = false
      observer.disconnect()
    }
  }, [loadMore, pagination?.has_more, request.error, request.loading])

  if (!selection) return null

  const summary = request.data?.summary
  const games = request.data?.games || []
  const analyzedOnly = selection.analyzed_only !== false
  const chartSelection = ['rating', 'result', 'opponent', 'opening'].includes(selection.scope)
  return createPortal(
    <div className="game-explorer-backdrop" role="presentation" onMouseDown={onClose}>
      <section {...pageStyleTarget('gameExplorer.panel')}
        className="game-explorer-modal"
        role="dialog"
        aria-modal="true"
        aria-label={selectionTitle(selection)}
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header {...pageStyleTarget('gameExplorer.header')} className="game-modal-header">
          <div>
            <p {...pageStyleTarget('gameExplorer.text')} className="eyebrow">GAME EXPLORER</p>
            <h2 {...pageStyleTarget('gameExplorer.title')}>{selectionTitle(selection)}</h2>
            <small {...pageStyleTarget('gameExplorer.text')}>{selection.modes.join(' · ')} · {analyzedOnly ? 'analyzed games only' : 'all matching games'}</small>
          </div>
          <button {...pageStyleTarget('gameExplorer.close')} className="game-modal-icon" type="button" aria-label="Close game explorer" onClick={onClose}>×</button>
        </header>

        {summary ? (
          <div className="game-explorer-summary">
            <div><span {...pageStyleTarget('gameExplorer.text')}>Games</span><strong {...pageStyleTarget('gameExplorer.value')}>{formatNumber(summary.games)}</strong></div>
            <div><span {...pageStyleTarget('gameExplorer.text')}>Analyzed</span><strong {...pageStyleTarget('gameExplorer.value')}>{formatNumber(summary.analyzed_games)}</strong></div>
            <div><span {...pageStyleTarget('gameExplorer.text')}>Accuracy</span><strong {...pageStyleTarget('gameExplorer.value')}>{summary.accuracy?.toFixed(2) ?? 'N/A'}</strong></div>
            <div><span {...pageStyleTarget('gameExplorer.text')}>Results</span><strong {...pageStyleTarget('gameExplorer.value')}>{summary.wins}W · {summary.draws}D · {summary.losses}L</strong></div>
          </div>
        ) : null}

        {request.error && !games.length ? <p {...pageStyleTarget('gameExplorer.text')} className="game-modal-message error" role="alert">{request.error}</p> : null}
        {!games.length && request.loading ? <p {...pageStyleTarget('gameExplorer.text')} className="game-modal-message">Loading games…</p> : null}
        {!games.length && !request.loading && !request.error ? <p {...pageStyleTarget('gameExplorer.text')} className="game-modal-message">{analyzedOnly ? 'No analyzed games are in this bin.' : 'No games are in this bin.'}</p> : null}

        {games.length ? (
          <div ref={listRef} className={`game-explorer-list${chartSelection ? ' game-explorer-list-compact' : ''}`}>
            {games.map((game) => (
              <button {...pageStyleTarget('gameExplorer.row')} type="button" onClick={() => onOpenGame(game.game_id)} key={`${game.game_id}-${game.player_color}`}>
                <span className={`game-result-mark ${game.result}`}>{resultLabel(game.result)}</span>
                <GameListDate value={game.played_at_local || game.played_at_utc} />
                <span className="game-list-opponent">
                  <strong>{game.opponent_name}</strong>
                </span>
                {!chartSelection ? <>
                  <span className="game-list-stat"><small>Accuracy</small><strong>{game.accuracy?.toFixed(2) ?? 'N/A'}</strong></span>
                  <span className="game-list-stat"><small>Blunders</small><strong>{game.blunder_count ?? 'N/A'}</strong></span>
                </> : null}
                <span className="game-list-rating" aria-label={`Rating: ${game.player_rating ?? 'unknown'}`}><strong>{game.player_rating ?? 'N/A'}</strong></span>
                <span className={`game-list-color ${game.player_color}`} role="img"
                  aria-label={`Player color: ${game.player_color || 'unknown'}`}
                  title={`Player color: ${game.player_color || 'unknown'}`} />
              </button>
            ))}
            <div ref={paginationSentinel} className="game-explorer-pagination">
              {request.error ? <>
                <p {...pageStyleTarget('gameExplorer.text')} role="alert">{request.error}</p>
                <button {...pageStyleTarget('gameExplorer.more')} className="game-explorer-retry" type="button" onClick={loadMore}>Retry</button>
              </> : <span {...pageStyleTarget('gameExplorer.text')} role="status">
                {request.loading ? 'Loading more games…' : !pagination?.has_more ? 'All matching games loaded.' : ''}
              </span>}
            </div>
          </div>
        ) : null}
      </section>
    </div>,
    document.body
  )
}
