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
  if (selection.scope === 'accuracy') return `Accuracy ${selection.accuracy_bin * 5}–${(selection.accuracy_bin + 1) * 5}`
  if (selection.scope === 'blunders') return `Games with ${selection.blunder_bin === 6 ? '6+' : selection.blunder_bin} blunder${selection.blunder_bin === 1 ? '' : 's'}`
  if (selection.scope === 'unexpected_endings') return selection.label || 'Unexpected endings'
  if (selection.scope === 'result') return { win: 'Won games', draw: 'Drawn games', loss: 'Lost games' }[selection.result]
  if (selection.scope === 'opponent') return `Games against ${selection.opponent}`
  if (selection.scope === 'opening') return `${selection.label || 'Unknown opening'} · ${selection.color || 'both colors'}`
  if (selection.scope === 'date') return `Games on ${selection.date}`
  if (selection.scope === 'hour') {
    const kind = { win: 'Won games', draw: 'Drawn games', loss: 'Lost games' }[selection.result] || 'Games'
    return `${kind} started at ${String(selection.hour).padStart(2, '0')}:00`
  }
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

export default function PlayerGameExplorerModal({ playerName, selection, onSelectionChange, onClose, onOpenGame }) {
  const [request, setRequest] = useState(emptyRequest)
  const paginationController = useRef(null)
  const dataSource = useRef(null)
  const listRef = useRef(null)
  const modalRef = useRef(null)
  const lastPointer = useRef(null)
  const [highlightedKey, setHighlightedKey] = useState(null)
  const paginationSentinel = useRef(null)
  const pagination = request.data?.pagination
  const isOpen = Boolean(selection)

  useEffect(() => {
    if (!isOpen) return undefined
    const opener = document.activeElement
    setHighlightedKey(null)
    lastPointer.current = null
    modalRef.current?.focus({ preventScroll: true })
    return () => {
      if (opener?.isConnected) opener.focus({ preventScroll: true })
    }
  }, [playerName, isOpen])

  useEffect(() => {
    // Transfer focus only on initial arrival, not on pagination or while the
    // user is editing styles / using another control in the modal.
    if (request.data?.games.length && document.activeElement === modalRef.current) {
      listRef.current?.focus({ preventScroll: true })
    }
  }, [request.data?.games.length])

  const highlightPointerRow = (event, key) => {
    const coordinates = `${event.clientX}:${event.clientY}`
    if (lastPointer.current === coordinates) return
    lastPointer.current = coordinates
    setHighlightedKey(key)
    const focused = document.activeElement
    if (!focused?.closest('[data-appearance-tools]')
      && !focused?.matches('input,textarea,select,[contenteditable="true"]')) {
      event.currentTarget.focus({ preventScroll: true })
    }
  }

  const handleListKey = event => {
    if (event.target !== listRef.current && !event.target.closest('[data-game-key]')) return
    if (!['ArrowUp', 'ArrowDown', 'Home', 'End', 'Enter', ' '].includes(event.key)) return
    event.preventDefault()
    event.stopPropagation()
    const rows = [...listRef.current.querySelectorAll('[data-game-key]')]
    if (!rows.length) return
    const current = rows.findIndex(row => row.dataset.gameKey === highlightedKey)
    if (event.key === 'Enter' || event.key === ' ') {
      if (current >= 0) rows[current].click()
      return
    }
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? rows.length - 1
      : current < 0 ? (event.key === 'ArrowUp' ? rows.length - 1 : 0)
      : Math.min(rows.length - 1, Math.max(0, current + (event.key === 'ArrowDown' ? 1 : -1)))
    setHighlightedKey(rows[next].dataset.gameKey)
    rows[next].focus({ preventScroll: true })
    const viewport = listRef.current.getBoundingClientRect()
    const bounds = rows[next].getBoundingClientRect()
    if (bounds.top < viewport.top) listRef.current.scrollTop -= viewport.top - bounds.top
    else if (bounds.bottom > viewport.bottom) listRef.current.scrollTop += bounds.bottom - viewport.bottom
    if (event.key === 'ArrowDown' && next === rows.length - 1) loadMore()
  }

  const handleModalKey = event => {
    if (event.target.closest('[data-appearance-tools]')) return
    if (event.target === modalRef.current && ['ArrowUp', 'ArrowDown', 'Home', 'End', ' '].includes(event.key)) {
      event.preventDefault()
    }
    if (event.key !== 'Tab') return
    const controls = [...modalRef.current.querySelectorAll('button:not(:disabled),[tabindex="0"]')]
      .filter(element => element.tabIndex >= 0 && element.getClientRects().length)
    const first = controls[0], last = controls.at(-1)
    if (!first) { event.preventDefault(); return }
    if (event.shiftKey && (event.target === first || event.target === modalRef.current)) {
      event.preventDefault(); last.focus({ preventScroll: true })
    } else if (!event.shiftKey && (event.target === last || event.target === modalRef.current)) {
      event.preventDefault(); first.focus({ preventScroll: true })
    }
  }

  useEffect(() => {
    dataSource.current = null
    if (!selection || !playerName) return undefined
    const controller = new AbortController()
    setHighlightedKey(null)
    lastPointer.current = null
    if (listRef.current) listRef.current.scrollTop = 0
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
  const chartSelection = ['rating', 'result', 'opponent', 'opening', 'accuracy', 'blunders', 'unexpected_endings'].includes(selection.scope)
  return createPortal(
    <div className="game-explorer-backdrop" role="presentation" onMouseDown={onClose}>
      <section {...pageStyleTarget('gameExplorer.panel')}
        ref={modalRef} tabIndex={-1} onKeyDown={handleModalKey}
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
            <small {...pageStyleTarget('gameExplorer.text')}>{selection.modes.join(' · ')} · {selection.scope === 'unexpected_endings'
              ? `final position · ${selection.score_band || 'advantage'} · 200 cp boundary`
              : analyzedOnly ? 'analyzed games only' : 'all matching games'}</small>
          </div>
          <button {...pageStyleTarget('gameExplorer.close')} className="game-modal-icon" type="button" aria-label="Close game explorer" onClick={onClose}>×</button>
        </header>

        {selection.scope === 'weekday_hour' && onSelectionChange ? <div {...pageStyleTarget('gameExplorer.results')}
          className="game-explorer-result-filters" role="group" aria-label="Filter games by result">
          {['win', 'draw', 'loss'].map(result => {
            const label = { win: 'Wins', draw: 'Draws', loss: 'Losses' }[result]
            const active = selection.result === result
            return <button {...pageStyleTarget(`gameExplorer.results.${result}`)} key={result} type="button"
              aria-pressed={active} title={active ? 'Show all results' : `Show only ${label.toLowerCase()}`}
              onClick={() => onSelectionChange({ ...selection, result: active ? null : result })}>{label}</button>
          })}
        </div> : null}

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
          <div ref={listRef} className={`game-explorer-list${chartSelection ? ' game-explorer-list-compact' : ''}`}
            tabIndex={0} role="region" aria-label="Games — use arrow keys to navigate and Enter to open" onKeyDown={handleListKey}>
            {games.map((game) => {
              const key = `${game.game_id}-${game.player_color}`
              const highlighted = highlightedKey === key
              return (
              <button {...pageStyleTarget('gameExplorer.row')} type="button" onClick={() => onOpenGame(game.game_id)} key={`${game.game_id}-${game.player_color}`}
                data-game-key={key} data-active={highlighted ? 'true' : undefined}
                className={highlighted ? 'is-highlighted' : undefined} tabIndex={highlighted ? 0 : -1}
                onPointerMove={event => highlightPointerRow(event, key)} onFocus={() => setHighlightedKey(key)}
                title={selection.scope === 'blunders' && game.blunder_count != null ? `Blunders: ${game.blunder_count}`
                  : selection.scope === 'unexpected_endings' && game.final_player_cp != null
                    ? `Final position: ${game.final_player_cp > 0 ? '+' : ''}${game.final_player_cp} cp (${playerName}'s perspective)`
                  : selection.scope === 'accuracy' && game.accuracy != null ? `Accuracy: ${game.accuracy.toFixed(2)}` : undefined}>
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
            )})}
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
