import { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { Chessboard } from 'react-chessboard'
import { fetchGameScore } from './playerAnalysisApi'

function formatClock(seconds) {
  if (seconds === null || seconds === undefined || !Number.isFinite(Number(seconds))) return '—'
  const total = Math.max(0, Math.round(Number(seconds)))
  return `${Math.floor(total / 60)}:${String(total % 60).padStart(2, '0')}`
}

function evaluationLabel(evaluation, orientation = 'white') {
  if (!evaluation) return 'N/A'
  const score = evaluation.white_cp * (orientation === 'black' ? -1 : 1)
  if (evaluation.kind === 'mate') return score > 0 ? 'MATE +' : 'MATE −'
  if (evaluation.kind === 'tablebase') return score > 0 ? 'TB WIN' : score < 0 ? 'TB LOSS' : 'TB DRAW'
  return `${score >= 0 ? '+' : ''}${(score / 100).toFixed(2)}`
}

function useBoardWidth(containerRef, maximized) {
  const [width, setWidth] = useState(420)
  useEffect(() => {
    const element = containerRef.current
    if (!element) return undefined
    const update = () => {
      const modalHeight = maximized ? window.innerHeight : window.innerHeight * 0.94
      const availableHeight = modalHeight - 190
      setWidth(Math.max(280, Math.min(680, Math.floor(element.clientWidth), availableHeight)))
    }
    update()
    const observer = new ResizeObserver(update)
    observer.observe(element)
    window.addEventListener('resize', update)
    return () => {
      observer.disconnect()
      window.removeEventListener('resize', update)
    }
  }, [containerRef, maximized])
  return width
}

export default function PlayerGameReviewModal({ gameId, playerName, onBack, onClose }) {
  const [request, setRequest] = useState({ data: null, error: '', loading: false })
  const [positionIndex, setPositionIndex] = useState(0)
  const [maximized, setMaximized] = useState(false)
  const boardRef = useRef(null)
  const boardWidth = useBoardWidth(boardRef, maximized)

  useEffect(() => {
    if (!gameId) return undefined
    const controller = new AbortController()
    setPositionIndex(0)
    setRequest({ data: null, error: '', loading: true })
    fetchGameScore({ gameId, signal: controller.signal })
      .then((data) => {
        if (!controller.signal.aborted) setRequest({ data, error: '', loading: false })
      })
      .catch((error) => {
        if (error.name !== 'AbortError') {
          setRequest({ data: null, error: error.message || 'Unable to load this game.', loading: false })
        }
      })
    return () => controller.abort()
  }, [gameId])

  const moves = request.data?.moves || []
  const game = request.data?.game
  const playerSummary = useMemo(() => (
    request.data?.player_summaries?.find(
      (summary) => summary.player_name.toLowerCase() === String(playerName).toLowerCase()
    )
  ), [playerName, request.data])
  const orientation = playerSummary?.player_color
    || (game?.black?.name?.toLowerCase() === String(playerName).toLowerCase() ? 'black' : 'white')
  const currentMove = positionIndex > 0 ? moves[positionIndex - 1] : null
  const currentFen = currentMove?.fen_after || game?.initial_fen
  const evaluation = currentMove?.evaluation_after || {
    kind: 'cp', white_cp: 15, white_win_percent: 51.38, source: 'initial',
  }
  const whitePercent = Math.max(2, Math.min(98, Number(evaluation?.white_win_percent ?? 50)))

  useEffect(() => {
    if (!gameId) return undefined
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    const handleKey = (event) => {
      if (event.key === 'Escape') onClose()
      if (event.key === 'ArrowLeft') setPositionIndex((value) => Math.max(0, value - 1))
      if (event.key === 'ArrowRight') setPositionIndex((value) => Math.min(moves.length, value + 1))
    }
    window.addEventListener('keydown', handleKey)
    return () => {
      document.body.style.overflow = previousOverflow
      window.removeEventListener('keydown', handleKey)
    }
  }, [gameId, moves.length, onClose])

  if (!gameId) return null

  return createPortal(
    <div className="game-review-backdrop" role="presentation">
      <section className={`game-review-modal ${maximized ? 'maximized' : ''}`} role="dialog" aria-modal="true" aria-label={`Game ${gameId}`}>
        <header className="game-modal-header game-review-header">
          <button className="game-review-back" type="button" onClick={onBack}>← games</button>
          <div>
            <p className="eyebrow">GAME REVIEW</p>
            <h2>{game ? `${game.white.name} · ${game.black.name}` : `Game ${gameId}`}</h2>
          </div>
          <div className="game-review-window-actions">
            <button className="game-modal-icon" type="button" aria-label={maximized ? 'Restore viewer' : 'Maximize viewer'} onClick={() => setMaximized((value) => !value)}>{maximized ? '↙' : '↗'}</button>
            <button className="game-modal-icon" type="button" aria-label="Close game viewer" onClick={onClose}>×</button>
          </div>
        </header>

        {request.loading ? <p className="game-modal-message">Loading move-by-move analysis…</p> : null}
        {request.error ? <p className="game-modal-message error" role="alert">{request.error}</p> : null}
        {game && !request.loading ? (
          <div className="game-review-body">
            <div className="game-review-board-column">
              <div className="game-review-names">
                <span>{orientation === 'white' ? game.black.name : game.white.name}</span>
                <strong>{orientation === 'white' ? game.black.rating : game.white.rating}</strong>
              </div>
              <div className="game-review-stage">
                <div className={`game-review-eval ${orientation === 'black' ? 'flipped' : ''}`} style={{ '--white-eval': `${whitePercent}%` }}>
                  <div className="game-review-eval-black" />
                  <div className="game-review-eval-white" />
                  <strong>{evaluationLabel(evaluation, orientation)}</strong>
                </div>
                <div className="game-review-board" ref={boardRef}>
                  <Chessboard
                    id={`review-board-${gameId}`}
                    position={currentFen}
                    boardOrientation={orientation}
                    boardWidth={boardWidth}
                    animationDuration={180}
                    arePiecesDraggable={false}
                  />
                </div>
              </div>
              <div className="game-review-names">
                <span>{orientation === 'white' ? game.white.name : game.black.name}</span>
                <strong>{orientation === 'white' ? game.white.rating : game.black.rating}</strong>
              </div>
              <div className="game-review-navigation">
                <button type="button" aria-label="Previous move" disabled={positionIndex === 0} onClick={() => setPositionIndex((value) => Math.max(0, value - 1))}>←</button>
                <span>{positionIndex === 0 ? 'Starting position' : `${currentMove.move_number}${currentMove.move_color === 'white' ? '.' : '…'} ${currentMove.san}`}</span>
                <button type="button" aria-label="Next move" disabled={positionIndex >= moves.length} onClick={() => setPositionIndex((value) => Math.min(moves.length, value + 1))}>→</button>
              </div>
            </div>

            <aside className="game-review-analysis">
              <div className="game-review-summary">
                <div><span>Accuracy</span><strong>{playerSummary?.accuracy?.toFixed(2) ?? 'N/A'}</strong></div>
                <div><span>Result</span><strong>{playerSummary?.result || '—'}</strong></div>
                <div><span>Blunders</span><strong>{playerSummary?.blunder_count ?? '—'}</strong></div>
                <div><span>Ended by</span><strong>{playerSummary?.end_by?.replaceAll('_', ' ') || '—'}</strong></div>
              </div>
              <div className="game-review-current">
                <strong>{currentMove?.classification || 'position'}</strong>
                <span>Evaluation {evaluationLabel(evaluation, 'white')} for White</span>
                <span>White clock {formatClock(currentMove?.clock?.white_time_left)}</span>
                <span>Black clock {formatClock(currentMove?.clock?.black_time_left)}</span>
                {currentMove?.move_accuracy !== null && currentMove?.move_accuracy !== undefined ? <span>Move accuracy {currentMove.move_accuracy.toFixed(2)}</span> : null}
                {currentMove?.engine_line_before?.length ? <small>Engine line: {currentMove.engine_line_before.slice(0, 8).join(' ')}</small> : null}
              </div>
              <div className="game-review-moves" aria-label="Game moves">
                {moves.map((move, index) => (
                  <button
                    type="button"
                    className={`${positionIndex === index + 1 ? 'active' : ''} ${move.classification || ''}`}
                    onClick={() => setPositionIndex(index + 1)}
                    key={move.ply}
                  >
                    <span>{move.move_color === 'white' ? `${move.move_number}.` : `${move.move_number}…`}</span>
                    <strong>{move.san || '—'}</strong>
                    <small>{evaluationLabel(move.evaluation_after, 'white')}</small>
                  </button>
                ))}
              </div>
            </aside>
          </div>
        ) : null}
      </section>
    </div>,
    document.body
  )
}
