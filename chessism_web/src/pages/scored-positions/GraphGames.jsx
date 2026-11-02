import { useEffect, useId, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import PlayerGameReviewModal from '../players/PlayerGameReviewModal'
import '../players/player-game-explorer.css'
import { graphStyle as s } from './GraphBoard'
import { useGraphRead } from './useGraphRead'

export default function GraphGames({ selection, filter, onClose }) {
  const [offset, setOffset] = useState(0), [highlight, setHighlight] = useState(null), [review, setReview] = useState(null)
  const request = useGraphRead('games', { ...filter, key: selection.key, move_key: selection.moveKey || '', offset }, !review)
  const rows = request.data?.rows || [], active = rows.findIndex(row => row.game_id === highlight)
  const dialog = useRef(null), navigation = useRef(null), title = useId(), lastPointer = useRef('')
  const startingPosition = useMemo(() => review ? { fen: review.fen,
    n_move: Math.ceil(review.ply / 2), move_color: review.ply % 2 ? 'white' : 'black' } : null, [review])
  navigation.current = event => {
    if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) {
      event.preventDefault()
      const index = event.key === 'Home' ? 0 : event.key === 'End' ? rows.length - 1
        : Math.max(0, Math.min(rows.length - 1, (active < 0 ? 0 : active) + (event.key === 'ArrowDown' ? 1 : -1)))
      if (rows[index]) { setHighlight(rows[index].game_id); dialog.current.querySelector(`[data-graph-game="${rows[index].game_id}"]`)?.focus({ preventScroll: true }) }
    } else if (event.key === 'Enter' && event.target === dialog.current && rows.length) {
      event.preventDefault(); setReview(rows[Math.max(0, active)])
    }
  }
  useEffect(() => {
    if (review) return undefined
    const opener = document.activeElement, overflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    dialog.current?.focus({ preventScroll: true })
    const key = event => {
      if (event.target.closest?.('[data-appearance-tools],input,textarea,select')) return
      if (event.key === 'Escape') { event.preventDefault(); onClose(); return }
      navigation.current?.(event)
      if (event.key === 'Tab') {
        const controls = [...dialog.current.querySelectorAll('button:not(:disabled)')]
        const first = controls[0], last = controls.at(-1)
        if (event.shiftKey && [first, dialog.current].includes(document.activeElement)) { event.preventDefault(); last?.focus() }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus() }
      }
    }
    document.addEventListener('keydown', key)
    return () => { document.body.style.overflow = overflow; document.removeEventListener('keydown', key); if (opener?.isConnected) opener.focus({ preventScroll: true }) }
  }, [Boolean(review), onClose])
  if (review) return <PlayerGameReviewModal gameId={review.game_id} playerName={filter.player || review.white}
    startingPosition={startingPosition} onBack={() => setReview(null)} onClose={onClose} />
  return createPortal(<div {...s('gamesOverlay')} className="common-graph-overlay" onClick={event => { if (event.target === event.currentTarget) onClose() }}>
    <section {...s('gamesPanel')} className="common-graph-games" role="dialog" aria-modal="true" aria-labelledby={title} ref={dialog} tabIndex={-1}>
      <header><h3 {...s('gamesTitle')} id={title}>{selection.move ? `Games with ${selection.move}` : 'Games with this position'}</h3>
        <button {...s('gamesClose')} type="button" aria-label="Close graph games" onClick={onClose}>×</button></header>
      <p {...s('gamesScope')}>Imported games only · newest first</p>
      {request.loading && <p {...s('status')} role="status">Loading games…</p>}
      {request.error && <p {...s('error')} role="alert">{request.error} <button {...s('retry')} onClick={request.retry}>Retry</button></p>}
      {!request.loading && !request.error && !rows.length && <p {...s('empty')}>No matching imported games.</p>}
      <div {...s('gamesList')} className="common-graph-games-list">
        {rows.map((row, index) => <button {...s('gameRow')} key={row.game_id} type="button" data-graph-game={row.game_id}
          data-active={highlight == null ? index === 0 : row.game_id === highlight} onFocus={() => setHighlight(row.game_id)}
          onPointerMove={event => {
            const point = `${event.clientX}:${event.clientY}`
            if (point !== lastPointer.current) { lastPointer.current = point; setHighlight(row.game_id); event.currentTarget.focus({ preventScroll: true }) }
          }} onClick={() => setReview(row)}>
          <time {...s('gameDate')}>{row.played_at ? new Date(row.played_at.replace(' ', 'T')).toLocaleString() : 'Date unavailable'}</time>
          <span {...s('gamePlayers')}>{row.white} — {row.black}</span>
          <span {...s('gameMode')}>{row.mode}</span>
          <span {...s('gameResult')}>{row.white_result === 1 ? '1–0' : row.black_result === 1 ? '0–1' : row.white_result === 0.5 ? '½–½' : '—'}</span>
        </button>)}
      </div>
      <footer><button {...s('gamesPrevious')} type="button" disabled={!offset || request.loading} onClick={() => { setOffset(n => n - 15); setHighlight(null) }}>Previous</button>
        <button {...s('gamesNext')} type="button" disabled={!request.data?.has_more || request.loading || offset >= 9990} onClick={() => { setOffset(n => n + 15); setHighlight(null) }}>Next</button></footer>
    </section>
  </div>, document.body)
}
