import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import { createPortal } from 'react-dom'
import { formatNumber } from '../../utils/formatters'
import { explorePlayerGames } from './playerAnalysisApi.js?game-explorer=b10cbe3'

const emptyRequest = { data: null, error: '', loading: false }

function resultLabel(value) {
  if (value === 'win') return 'W'
  if (value === 'draw') return 'D'
  return 'L'
}

function selectionTitle(selection) {
  if (!selection) return 'Analyzed games'
  if (selection.scope === 'date') return `Games on ${selection.date}`
  if (selection.scope === 'hour') return `Games started at ${String(selection.hour).padStart(2, '0')}:00`
  if (selection.scope === 'weekday_hour') {
    return `${selection.label} at ${String(selection.hour).padStart(2, '0')}:00`
  }
  return `${selection.label} games`
}

function localDateTime(value) {
  if (!value) return 'Unknown date'
  return String(value).replace('T', ' ').slice(0, 16)
}

export default function PlayerGameExplorerModal({ playerName, selection, onClose, onOpenGame }) {
  const [request, setRequest] = useState(emptyRequest)

  useEffect(() => {
    if (!selection || !playerName) return undefined
    const controller = new AbortController()
    setRequest({ data: null, error: '', loading: true })
    explorePlayerGames({
      playerName,
      selection: { ...selection, limit: 30 },
      signal: controller.signal,
    })
      .then((data) => {
        if (!controller.signal.aborted) setRequest({ data, error: '', loading: false })
      })
      .catch((error) => {
        if (error.name !== 'AbortError') {
          setRequest({ data: null, error: error.message || 'Unable to load these games.', loading: false })
        }
      })
    return () => controller.abort()
  }, [playerName, selection])

  useEffect(() => {
    if (!selection) return undefined
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    const closeWithEscape = (event) => {
      if (event.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', closeWithEscape)
    return () => {
      document.body.style.overflow = previousOverflow
      window.removeEventListener('keydown', closeWithEscape)
    }
  }, [onClose, selection])

  if (!selection) return null

  const loadMore = async () => {
    const cursor = request.data?.pagination?.next_cursor
    if (!cursor || request.loading) return
    const controller = new AbortController()
    setRequest((current) => ({ ...current, error: '', loading: true }))
    try {
      const next = await explorePlayerGames({
        playerName,
        selection: { ...selection, limit: 30, cursor },
        signal: controller.signal,
      })
      setRequest((current) => ({
        data: {
          ...next,
          games: [...(current.data?.games || []), ...next.games],
        },
        error: '',
        loading: false,
      }))
    } catch (error) {
      if (error.name !== 'AbortError') {
        setRequest((current) => ({ ...current, error: error.message, loading: false }))
      }
    }
  }

  const summary = request.data?.summary
  const games = request.data?.games || []
  const analyzedOnly = selection.analyzed_only !== false
  return createPortal(
    <div className="game-explorer-backdrop" role="presentation" onMouseDown={onClose}>
      <section {...pageStyleTarget('panel')}
        className="game-explorer-modal"
        role="dialog"
        aria-modal="true"
        aria-label={selectionTitle(selection)}
        onMouseDown={(event) => event.stopPropagation()}
      >
        <header className="game-modal-header">
          <div>
            <p {...pageStyleTarget('text')} className="eyebrow">GAME EXPLORER</p>
            <h2 {...pageStyleTarget('title')}>{selectionTitle(selection)}</h2>
            <small {...pageStyleTarget('text')}>{selection.modes.join(' · ')} · {analyzedOnly ? 'analyzed games only' : 'all games in this bin'}</small>
          </div>
          <button {...pageStyleTarget('button')} className="game-modal-icon" type="button" aria-label="Close game explorer" onClick={onClose}>×</button>
        </header>

        {summary ? (
          <div className="game-explorer-summary">
            <div><span>Games</span><strong {...pageStyleTarget('value')}>{formatNumber(summary.games)}</strong></div>
            <div><span>Analyzed</span><strong {...pageStyleTarget('value')}>{formatNumber(summary.analyzed_games)}</strong></div>
            <div><span>Accuracy</span><strong {...pageStyleTarget('value')}>{summary.accuracy?.toFixed(2) ?? 'N/A'}</strong></div>
            <div><span>Results</span><strong {...pageStyleTarget('value')}>{summary.wins}W · {summary.draws}D · {summary.losses}L</strong></div>
          </div>
        ) : null}

        {request.error ? <p {...pageStyleTarget('text')} className="game-modal-message error" role="alert">{request.error}</p> : null}
        {!games.length && request.loading ? <p {...pageStyleTarget('text')} className="game-modal-message">Loading games…</p> : null}
        {!games.length && !request.loading && !request.error ? <p {...pageStyleTarget('text')} className="game-modal-message">No analyzed games are in this bin.</p> : null}

        {games.length ? (
          <div className="game-explorer-list">
            {games.map((game) => (
              <button {...pageStyleTarget('button')} type="button" onClick={() => onOpenGame(game.game_id)} key={game.game_id}>
                <span className={`game-result-mark ${game.result}`}>{resultLabel(game.result)}</span>
                <span className="game-list-opponent">
                  <strong {...pageStyleTarget('value')}>vs {game.opponent_name}</strong>
                  <small {...pageStyleTarget('text')}>{localDateTime(game.played_at_local || game.played_at_utc)} · {game.mode} · {game.player_color}</small>
                </span>
                <span><small {...pageStyleTarget('text')}>Accuracy</small><strong {...pageStyleTarget('value')}>{game.accuracy?.toFixed(2) ?? 'N/A'}</strong></span>
                <span><small {...pageStyleTarget('text')}>Blunders</small><strong {...pageStyleTarget('value')}>{game.blunder_count ?? 'N/A'}</strong></span>
                <span><small {...pageStyleTarget('text')}>Rating</small><strong {...pageStyleTarget('value')}>{game.player_rating}</strong></span>
                <span className="game-list-open">open →</span>
              </button>
            ))}
          </div>
        ) : null}

        {request.data?.pagination?.has_more ? (
          <button {...pageStyleTarget('button')} className="game-explorer-more" type="button" disabled={request.loading} onClick={loadMore}>
            {request.loading ? 'Loading…' : 'Load more'}
          </button>
        ) : null}
      </section>
    </div>,
    document.body
  )
}
