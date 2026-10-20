import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { Chessboard } from 'react-chessboard'
import { fetchGameScore } from './playerAnalysisApi.js?game-explorer=b10cbe3'
import { findPositionOccurrence } from '../../components/chess/positionOccurrence'

const endingLabels = {
  checkmated: 'checkmate',
  resigned: 'resignation',
  abandoned: 'abandonment',
  agreed: 'agreement',
  insufficient: 'insufficient material',
  timevsinsufficient: 'timeout vs insufficient material',
  '50move': 'fifty move rule',
}

function recordedOutcome(game, playerColor) {
  const recorded = game?.result
  const value = recorded?.[playerColor]
  const result = value === 1 ? 'win' : value === 0.5 ? 'draw' : value === 0 ? 'loss' : null
  // A winner's own reason is only "win"; the losing side records how it ended.
  const reasonColor = value === 1 ? (playerColor === 'white' ? 'black' : 'white') : playerColor
  const reason = String(recorded?.[`${reasonColor}_reason`] ?? '').trim().toLowerCase()
  const endBy = ['', 'none', 'null', 'unknown', 'win'].includes(reason)
    ? null : endingLabels[reason] || reason.replaceAll('_', ' ')
  return { result, endBy }
}

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

function useBoardWidth(containerRef, active, maximized) {
  const [width, setWidth] = useState(0)
  useLayoutEffect(() => {
    const element = containerRef.current
    if (!active || !element) return undefined
    const update = () => {
      const style = getComputedStyle(element)
      const evaluationWidth = parseFloat(style.getPropertyValue('--game-review-eval-width')) || 24
      const gap = parseFloat(style.columnGap) || 0
      // Measure the reserved board area, not the window or the board itself.
      // The player labels are outside this area and must never be covered.
      const availableWidth = element.clientWidth - evaluationWidth - gap
      setWidth(Math.max(0, Math.floor(Math.min(680, availableWidth, element.clientHeight))))
    }
    update()
    const observer = new ResizeObserver(update)
    observer.observe(element)
    return () => observer.disconnect()
  }, [containerRef, active, maximized])
  return width
}

export default function PlayerGameReviewModal({ gameId, playerName, startingPosition, onBack, onClose }) {
  const [request, setRequest] = useState({ data: null, error: '', loading: false })
  const [positionIndex, setPositionIndex] = useState(0)
  const [maximized, setMaximized] = useState(false)
  const boardStageRef = useRef(null)
  const boardVisible = Boolean(gameId && request.data?.game && !request.loading)
  const boardWidth = useBoardWidth(boardStageRef, boardVisible, maximized)
  const closeViewer = onBack || onClose

  useLayoutEffect(() => {
    setMaximized(false)
  }, [gameId])

  useEffect(() => {
    if (!gameId) return undefined
    const controller = new AbortController()
    setPositionIndex(0)
    setRequest({ data: null, error: '', loading: true })
    fetchGameScore({ gameId, signal: controller.signal })
      .then((data) => {
        if (!controller.signal.aborted) {
          const match=findPositionOccurrence(data,startingPosition)
          setPositionIndex(match.index)
          setRequest({ data, error: '', loading: false, missingPosition:!match.found })
        }
      })
      .catch((error) => {
        if (error.name !== 'AbortError') {
          setRequest({ data: null, error: error.message || 'Unable to load this game.', loading: false })
        }
      })
    return () => controller.abort()
  }, [gameId,startingPosition])

  const moves = request.data?.moves || []
  const game = request.data?.game
  const playerSummary = useMemo(() => (
    request.data?.player_summaries?.find(
      (summary) => summary.player_name.toLowerCase() === String(playerName).toLowerCase()
    )
  ), [playerName, request.data])
  const orientation = game?.black?.name?.toLowerCase() === String(playerName).toLowerCase() ? 'black' : 'white'
  const outcome = recordedOutcome(game, orientation)
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
      if (event.target?.closest?.('[data-appearance-tools]')) return
      if (event.key === 'Escape') closeViewer()
      if (event.key === 'ArrowLeft') setPositionIndex((value) => Math.max(0, value - 1))
      if (event.key === 'ArrowRight') setPositionIndex((value) => Math.min(moves.length, value + 1))
    }
    window.addEventListener('keydown', handleKey)
    return () => {
      document.body.style.overflow = previousOverflow
      window.removeEventListener('keydown', handleKey)
    }
  }, [gameId, moves.length, closeViewer])

  if (!gameId) return null

  return createPortal(
    <div className={`game-review-backdrop${maximized ? ' maximized' : ''}`} role="presentation">
      <section {...pageStyleTarget('panel')} className={`game-review-modal ${maximized ? 'maximized' : ''}`} role="dialog" aria-modal="true" aria-label={`Game ${gameId}`}>
        <header className="game-modal-header game-review-header">
          <div className="game-review-heading">
            <p {...pageStyleTarget('text')} className="eyebrow">GAME REVIEW</p>
            <h2 {...pageStyleTarget('title')}>{game ? `${game.white.name} · ${game.black.name}` : `Game ${gameId}`}</h2>
          </div>
          <div className="game-review-window-actions">
            <button {...pageStyleTarget('gameReview.fullscreen')} className="game-modal-icon" type="button"
              aria-label={maximized ? 'Exit fullscreen' : 'Fullscreen'} title={maximized ? 'Exit fullscreen' : 'Fullscreen'}
              aria-pressed={maximized} onClick={() => setMaximized((value) => !value)}>
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                <path d={maximized ? 'M3 8h5V3m8 0v5h5M8 21v-5H3m13 5v-5h5' : 'M8 3H3v5m13-5h5v5M3 16v5h5m13-5v5h-5'} />
              </svg>
            </button>
            <button {...pageStyleTarget('gameReview.close')} className="game-modal-icon" type="button" aria-label="Close game viewer" title="Close game viewer" onClick={closeViewer}>×</button>
          </div>
        </header>

        {request.loading ? <p {...pageStyleTarget('text')} className="game-modal-message">Loading move-by-move analysis…</p> : null}
        {request.error ? <p {...pageStyleTarget('text')} className="game-modal-message error" role="alert">{request.error}</p> : null}
        {request.missingPosition ? <p {...pageStyleTarget('text')} className="game-modal-message error" role="alert">The requested occurrence is unavailable in this game's move record. Showing the starting position.</p> : null}
        {game && !request.loading ? (
          <div className="game-review-body">
            <div className="game-review-board-column">
              <div className="game-review-names game-review-opponent-rating">
                <strong {...pageStyleTarget('value')}>{orientation === 'white' ? game.black.rating : game.white.rating}</strong>
              </div>
              <div className="game-review-stage" ref={boardStageRef} style={{ '--game-review-board-size': `${boardWidth}px` }}>
                <div className={`game-review-eval ${orientation === 'black' ? 'flipped' : ''}`} style={{ '--white-eval': `${whitePercent}%` }}>
                  <div className="game-review-eval-black" />
                  <div className="game-review-eval-white" />
                  <strong {...pageStyleTarget('value')}>{evaluationLabel(evaluation, orientation)}</strong>
                </div>
                <div className="game-review-board">
                  {boardWidth > 0 ? <Chessboard
                    id={`review-board-${gameId}`}
                    position={currentFen}
                    boardOrientation={orientation}
                    boardWidth={boardWidth}
                    animationDuration={180}
                    arePiecesDraggable={false}
                  /> : null}
                </div>
              </div>
              <div className="game-review-names">
                <span>{orientation === 'white' ? game.white.name : game.black.name}</span>
                <strong {...pageStyleTarget('value')}>{orientation === 'white' ? game.white.rating : game.black.rating}</strong>
              </div>
            </div>

            <aside className="game-review-analysis">
              <div className="game-review-summary">
                <div><span>Accuracy</span><strong {...pageStyleTarget('value')}>{playerSummary?.accuracy?.toFixed(2) ?? 'N/A'}</strong></div>
                <div><span>Result</span><strong {...pageStyleTarget('value')}>{outcome.result ?? '—'}</strong></div>
                <div><span>Blunders</span><strong {...pageStyleTarget('value')}>{playerSummary?.blunder_count ?? '—'}</strong></div>
                <div><span>Ended by</span><strong {...pageStyleTarget('value')}>{outcome.endBy ?? '—'}</strong></div>
              </div>
              <div className="game-review-current">
                <strong {...pageStyleTarget('value')}>{currentMove?.classification || 'position'}</strong>
                <span>Evaluation {evaluationLabel(evaluation, 'white')} for White</span>
                <span>White clock {formatClock(currentMove?.clock?.white_time_left)}</span>
                <span>Black clock {formatClock(currentMove?.clock?.black_time_left)}</span>
                {currentMove?.move_accuracy !== null && currentMove?.move_accuracy !== undefined ? <span>Move accuracy {currentMove.move_accuracy.toFixed(2)}</span> : null}
                {currentMove?.engine_line_before?.length ? <small {...pageStyleTarget('text')}>Engine line: {currentMove.engine_line_before.slice(0, 8).join(' ')}</small> : null}
              </div>
              <div className="game-review-moves" aria-label="Game moves">
                {moves.map((move, index) => (
                  <button {...pageStyleTarget('button')}
                    type="button"
                    className={`${positionIndex === index + 1 ? 'active' : ''} ${move.classification || ''}`}
                    onClick={() => setPositionIndex(index + 1)}
                    key={move.ply}
                  >
                    <span>{move.move_color === 'white' ? `${move.move_number}.` : `${move.move_number}…`}</span>
                    <strong {...pageStyleTarget('value')}>{move.san || '—'}</strong>
                    <small {...pageStyleTarget('text')}>{evaluationLabel(move.evaluation_after, 'white')}</small>
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
