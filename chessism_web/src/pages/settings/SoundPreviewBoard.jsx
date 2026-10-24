import { useEffect, useRef, useState } from 'react'
import { Chess } from 'chess.js'
import { Chessboard } from 'react-chessboard'
import { pageStyleTarget } from '../../appearance/styleCore'
import { boardMoveSound, createBoardAudio } from '../../components/chess/boardAudio'
import { getBoardSoundSet } from '../../components/chess/boardSoundSets'

export default function SoundPreviewBoard({ soundSet }) {
  const [game] = useState(() => new Chess())
  const [fen, setFen] = useState(() => game.fen())
  const [selectedSquare, setSelectedSquare] = useState('')
  const [boardWidth, setBoardWidth] = useState(0)
  const [error, setError] = useState('')
  const boardWrap = useRef(null)
  const audio = useRef(null)
  const playRequest = useRef(0)

  useEffect(() => {
    const element = boardWrap.current
    const update = () => setBoardWidth(Math.max(0, Math.floor(element.getBoundingClientRect().width)))
    update()
    const observer = new ResizeObserver(update)
    observer.observe(element)
    return () => observer.disconnect()
  }, [])

  useEffect(() => {
    const player = createBoardAudio(soundSet)
    audio.current = player
    setError('')
    const onVisibility = () => {
      if (document.hidden) { playRequest.current += 1; player.stop() }
    }
    document.addEventListener('visibilitychange', onVisibility)
    return () => {
      playRequest.current += 1
      document.removeEventListener('visibilitychange', onVisibility)
      audio.current = null
      player.dispose()
    }
  }, [soundSet])

  const play = move => {
    const request = ++playRequest.current
    setError('')
    // Trying sounds is explicit, so the board previews even if the viewer is muted.
    audio.current?.play(boardMoveSound(move)).then(played => {
      if (!played && request === playRequest.current) {
        setError('Unable to play audio. Check your browser’s sound permissions and try another move.')
      }
    })
  }

  const makeMove = (from, to) => {
    if (!from || !to || game.isGameOver()) return false
    let move
    try { move = game.move({ from, to, promotion: 'q' }) } catch { return false }
    if (!move) return false
    setFen(game.fen())
    setSelectedSquare('')
    play(move)
    return true
  }

  const onSquareClick = square => {
    if (selectedSquare && makeMove(selectedSquare, square)) return
    setSelectedSquare(selectedSquare !== square && game.get(square)?.color === game.turn() ? square : '')
  }

  const reset = () => {
    playRequest.current += 1
    audio.current?.stop()
    game.reset()
    setFen(game.fen())
    setSelectedSquare('')
    setError('')
  }

  const status = game.isCheckmate() ? 'Checkmate' : game.isDraw() ? 'Draw'
    : `${game.turn() === 'w' ? 'White' : 'Black'} to move${game.isCheck() ? ' · Check' : ''}`
  return (
    <div className="settings-preview-board">
      <p {...pageStyleTarget('text')} className="settings-trial-title">Trying: <strong>{getBoardSoundSet(soundSet).name}</strong></p>
      <div className="settings-board-surface" ref={boardWrap} aria-label="Sound trial chessboard. Play both sides by dragging pieces or clicking squares.">
        {boardWidth > 0 ? <Chessboard id="settings-sound-board" position={fen} boardWidth={boardWidth}
          animationDuration={0} arePiecesDraggable={!game.isGameOver()} areArrowsAllowed={false}
          autoPromoteToQueen onPieceDrop={makeMove} onSquareClick={onSquareClick}
          customSquareStyles={selectedSquare ? { [selectedSquare]: { backgroundColor: 'rgba(21, 199, 128, .5)' } } : {}} /> : null}
      </div>
      <p {...pageStyleTarget('text')} className="settings-board-status" role="status">{status}</p>
      <div className="settings-board-controls">
        <button {...pageStyleTarget('sounds.preview')} className="settings-control settings-board-reset" type="button" onClick={reset}
          aria-label="Reset board to the starting position" title="Reset board">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
            <path d="M3 10a9 9 0 1 1 2.1 8M3 4v6h6" />
          </svg>
        </button>
      </div>
      <p {...pageStyleTarget('text')} className="settings-trial-note">Play both sides to hear this set.</p>
      {error ? <p className="settings-error" role="alert">{error}</p> : null}
    </div>
  )
}
