import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { Chessboard } from 'react-chessboard'
import { fetchGameScore } from './playerAnalysisApi.js?game-explorer=b10cbe3'
import { findPositionOccurrence } from '../../components/chess/positionOccurrence'
import useBoardSounds from '../../components/chess/useBoardSounds'
import { useBoardSoundPreferences } from '../../components/chess/BoardSoundPreferences'
import { BOARD_SOUND_SETS } from '../../components/chess/boardSoundSets'

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

function GameReviewDate({ value }) {
  const date = value ? new Date(value) : null
  if (!date || !Number.isFinite(date.getTime())) {
    return <span {...pageStyleTarget('text')} className="game-review-date-badge">Date unavailable</span>
  }
  const month = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][date.getUTCMonth()]
  const day = String(date.getUTCDate()).padStart(2, '0')
  const time = `${String(date.getUTCHours()).padStart(2, '0')}:${String(date.getUTCMinutes()).padStart(2, '0')}`
  const label = `Game started: ${date.getUTCFullYear()} ${month} ${day}, ${time} UTC`
  return <time {...pageStyleTarget('text')} className="game-review-start-date" dateTime={date.toISOString()}
    title={label} aria-label={label}>
    <span className="game-review-date-badge">{date.getUTCFullYear()}</span>
    <span className="game-review-date-badge">{month}</span>
    <span className="game-review-date-badge">{day}</span>
    <span className="game-review-date-badge">{time}</span>
  </time>
}

function clockHistory(moves, timeControl) {
  const control = /^(\d+)(?:\+(\d+))?$/.exec(String(timeControl ?? ''))
  const initial = control ? Number(control[1]) : null
  const increment = control ? Number(control[2] || 0) : null
  const frames = [{
    white: { remaining: initial, spent: null },
    black: { remaining: initial, spent: null },
  }]
  for (const move of moves) {
    const previous = frames[frames.length - 1]
    const color = move.move_color
    if (color !== 'white' && color !== 'black') {
      frames.push(previous)
      continue
    }
    const raw = move.clock?.[`${color}_time_left`]
    const remaining = raw != null && Number.isFinite(Number(raw)) ? Number(raw) : null
    const spent = previous[color].remaining != null && remaining != null && increment != null
      ? previous[color].remaining + increment - remaining : null
    // Each API row contains a full move's clocks. Only advance the mover's
    // clock, so White's move never reveals Black's next clock prematurely.
    frames.push({ ...previous, [color]: { remaining, spent: spent >= 0 ? spent : null } })
  }
  return frames
}

function BoardClock({ color, clock }) {
  const remaining = formatClock(clock?.remaining)
  return (
    <span className={`game-review-clock ${color}`} title={`${color} clock`}>
      {clock?.spent != null ? (
        <small className="game-review-move-time" title="Time spent on the last move by this player">
          {clock.spent.toFixed(1)}s
        </small>
      ) : null}
      <strong {...pageStyleTarget('value')} aria-label={`${color} time remaining: ${remaining}`}>{remaining}</strong>
    </span>
  )
}

function evaluationLabel(evaluation, orientation = 'white') {
  if (!Number.isFinite(evaluation?.white_cp)) return 'N/A'
  const score = evaluation.white_cp * (orientation === 'black' ? -1 : 1)
  if (evaluation.kind === 'mate') return score > 0 ? 'MATE +' : 'MATE −'
  if (evaluation.kind === 'tablebase') return score > 0 ? 'TB WIN' : score < 0 ? 'TB LOSS' : 'TB DRAW'
  return `${score >= 0 ? '+' : ''}${(score / 100).toFixed(2)}`
}

function usePieceAnimationDuration() {
  const [reducedMotion, setReducedMotion] = useState(() => (
    typeof window !== 'undefined' && Boolean(window.matchMedia?.('(prefers-reduced-motion: reduce)').matches)
  ))
  useEffect(() => {
    const preference = window.matchMedia?.('(prefers-reduced-motion: reduce)')
    if (!preference) return undefined
    const update = () => setReducedMotion(preference.matches)
    update()
    preference.addEventListener('change', update)
    return () => preference.removeEventListener('change', update)
  }, [])
  return reducedMotion ? 0 : 320
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
  const pieceAnimationDuration = usePieceAnimationDuration()
  const { muted, toggleMuted, playMove } = useBoardSounds(gameId)
  const { soundSet, updatePreferences, saveError } = useBoardSoundPreferences()
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
  const opponentColor = orientation === 'white' ? 'black' : 'white'
  const clocks = useMemo(() => clockHistory(moves, game?.time_control), [moves, game?.time_control])
  const currentClocks = clocks[positionIndex] || clocks[0]
  const outcome = recordedOutcome(game, orientation)
  const currentMove = positionIndex > 0 ? moves[positionIndex - 1] : null
  const currentFen = currentMove?.fen_after || game?.initial_fen
  const evaluation = currentMove?.evaluation_after ?? null
  const hasEvaluation = Number.isFinite(evaluation?.white_cp)
  const noScoreMessage = 'No Stockfish score is available for this FEN.'
  const whitePercent = Math.max(2, Math.min(98, Number(evaluation?.white_win_percent ?? 50)))

  const navigateTo = useCallback((index) => {
    if (!boardVisible) return
    const next = Math.max(0, Math.min(moves.length, index))
    if (next === positionIndex) return
    setPositionIndex(next)
    playMove(moves[next - 1], next < positionIndex)
  }, [boardVisible, moves, positionIndex, playMove])

  useEffect(() => {
    if (!gameId) return undefined
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    const handleKey = (event) => {
      if (event.target?.closest?.('[data-appearance-tools]')) return
      if (event.key === 'Escape') closeViewer()
      if (event.defaultPrevented || event.altKey || event.ctrlKey || event.metaKey) return
      if (event.target?.closest?.('input, textarea, select, [contenteditable="true"]')) return
      if (event.key === 'ArrowLeft' || event.key === 'ArrowRight') {
        event.preventDefault()
        navigateTo(positionIndex + (event.key === 'ArrowLeft' ? -1 : 1))
      }
    }
    window.addEventListener('keydown', handleKey)
    return () => {
      document.body.style.overflow = previousOverflow
      window.removeEventListener('keydown', handleKey)
    }
  }, [gameId, positionIndex, navigateTo, closeViewer])

  if (!gameId) return null

  return createPortal(
    <div className={`game-review-backdrop${maximized ? ' maximized' : ''}`} role="presentation">
      <section {...pageStyleTarget('panel')} className={`game-review-modal ${maximized ? 'maximized' : ''}`} role="dialog" aria-modal="true" aria-label={`Game ${gameId}`}>
        <header className="game-modal-header game-review-header">
          <div className="game-review-heading">
            <div className="game-review-heading-meta">
              <p {...pageStyleTarget('text')} className="eyebrow">GAME REVIEW</p>
              {game && !request.loading ? <GameReviewDate value={game.played_at_utc} /> : null}
            </div>
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
        {saveError ? <p {...pageStyleTarget('text')} className="game-modal-message error" role="alert">{saveError}</p> : null}
        {request.missingPosition ? <p {...pageStyleTarget('text')} className="game-modal-message error" role="alert">The requested occurrence is unavailable in this game's move record. Showing the starting position.</p> : null}
        {game && !request.loading ? (
          <div className="game-review-body">
            <div className="game-review-board-column" style={{ '--game-review-board-size': `${boardWidth}px` }}>
              <div className="game-review-names game-review-board-header">
                <div className="game-review-board-tools">
                  <select {...pageStyleTarget('gameReview.soundSet')} className="game-review-sound-set"
                    aria-label="Board sound set" title="Board sound set" value={soundSet}
                    onChange={event => updatePreferences({ soundSet: event.target.value })}
                    onKeyDown={event => event.stopPropagation()}>
                    {BOARD_SOUND_SETS.map(set => <option key={set.id} value={set.id}>{set.name}</option>)}
                  </select>
                  <button type="button" className="game-review-sound-toggle"
                    aria-label="Board sounds" aria-pressed={!muted}
                    title={muted ? 'Unmute board sounds' : 'Mute board sounds'} onClick={toggleMuted}>
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                      <path d="M11 5 6 9H3v6h3l5 4Z" />
                      {muted ? <path d="m16 9 6 6m0-6-6 6" /> : <>
                        <path d="M15 8a6 6 0 0 1 0 8" />
                        <path d="M18 5a10 10 0 0 1 0 14" />
                      </>}
                    </svg>
                  </button>
                  <span {...pageStyleTarget('text')} className="game-review-mode" title={`Time control: ${game.time_control}`}>{game.mode}</span>
                </div>
                <div className="game-review-player-clock">
                  <strong {...pageStyleTarget('value')}>{game[opponentColor].rating}</strong>
                  <BoardClock color={opponentColor} clock={currentClocks[opponentColor]} />
                </div>
              </div>
              <div className="game-review-stage" ref={boardStageRef}>
                <div className={`game-review-eval${orientation === 'black' ? ' flipped' : ''}${hasEvaluation ? '' : ' is-unscored'}`}
                  style={{ '--white-eval': `${whitePercent}%` }} role="img" aria-disabled={!hasEvaluation}
                  aria-label={hasEvaluation ? `Position evaluation: ${evaluationLabel(evaluation, orientation)}` : noScoreMessage}
                  title={hasEvaluation ? undefined : noScoreMessage}>
                  <div className="game-review-eval-black" aria-hidden="true" />
                  <div className="game-review-eval-white" aria-hidden="true" />
                  {hasEvaluation ? <strong {...pageStyleTarget('value')} aria-hidden="true">{evaluationLabel(evaluation, orientation)}</strong> : null}
                </div>
                <div className="game-review-board">
                  {boardWidth > 0 ? <Chessboard
                    id={`review-board-${gameId}`}
                    position={currentFen}
                    boardOrientation={orientation}
                    boardWidth={boardWidth}
                    animationDuration={pieceAnimationDuration}
                    arePiecesDraggable={false}
                  /> : null}
                </div>
              </div>
              <div className="game-review-names">
                <span>{game[orientation].name}</span>
                <div className="game-review-player-clock">
                  <strong {...pageStyleTarget('value')}>{game[orientation].rating}</strong>
                  <BoardClock color={orientation} clock={currentClocks[orientation]} />
                </div>
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
                <span>{hasEvaluation ? `Evaluation ${evaluationLabel(evaluation, 'white')} for White` : noScoreMessage}</span>
                {currentMove?.move_accuracy !== null && currentMove?.move_accuracy !== undefined ? <span>Move accuracy {currentMove.move_accuracy.toFixed(2)}</span> : null}
                {currentMove?.engine_line_before?.length ? <small {...pageStyleTarget('text')}>Engine line: {currentMove.engine_line_before.slice(0, 8).join(' ')}</small> : null}
              </div>
              <div className="game-review-moves" aria-label="Game moves">
                {moves.map((move, index) => (
                  <button {...pageStyleTarget('button')}
                    type="button"
                    className={`${positionIndex === index + 1 ? 'active' : ''} ${move.classification || ''}`}
                    onClick={() => navigateTo(index + 1)}
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
