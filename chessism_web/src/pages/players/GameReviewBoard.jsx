import { Children, createContext, forwardRef, useContext, useLayoutEffect, useRef, useState } from 'react'
import { Chessboard } from 'react-chessboard'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { createReviewBoardMotion } from './reviewBoardMotion.js'
import './game-review-board.css'

const BoardView = createContext({ orientation: 'white', width: 0 })
const transparentSquare = { backgroundColor: 'transparent' }

// Keep the library's pieces and hit targets. Our skins own move animations and
// stable editor IDs, without changing the library or other boards in the app.
const ReviewSquare = forwardRef(function ReviewSquare({ square, squareColor, style, children }, ref) {
  const { orientation, width } = useContext(BoardView)
  const showRank = square[0] === (orientation === 'white' ? 'a' : 'h')
  const showFile = square[1] === (orientation === 'white' ? '1' : '8')
  return <div {...pageStyleTarget(`gameReview.board.square.${squareColor === 'white' ? 'light' : 'dark'}`)}
    ref={ref} className={`game-review-square ${squareColor}`} style={style}>
    {Children.map(children, child => child?.props?.piece
      ? <span {...pageStyleTarget(`gameReview.board.piece.${child.props.piece[0] === 'w' ? 'white' : 'black'}`)} className="game-review-piece-skin">{child}</span>
      : child)}
    {showRank ? <span {...pageStyleTarget('gameReview.board.rank')} className="game-review-coordinate rank"
      style={{ '--review-coordinate-size': `${width / 48}px` }}>{square[1]}</span> : null}
    {showFile ? <span {...pageStyleTarget('gameReview.board.file')} className="game-review-coordinate file"
      style={{ '--review-coordinate-size': `${width / 48}px` }}>{square[0]}</span> : null}
  </div>
})

export default function GameReviewBoard({ id, position, orientation, animationDuration, ply }) {
  const frame = useRef(null)
  const pieces = useRef(null), overlay = useRef(null), motion = useRef(null)
  const [width, setWidth] = useState(0)
  useLayoutEffect(() => {
    motion.current = createReviewBoardMotion(pieces.current, overlay.current)
    return () => motion.current.destroy()
  }, [])
  useLayoutEffect(() => {
    motion.current.update({ id, position, orientation, width, duration: animationDuration, ply })
  }, [id, position, orientation, width, animationDuration, ply])
  useLayoutEffect(() => {
    const element = frame.current
    const observer = new ResizeObserver(([entry]) => {
      // Border/padding edits must shrink the board, not push it into the names.
      setWidth(Math.max(0, Math.floor(Math.min(entry.contentRect.width, entry.contentRect.height))))
    })
    observer.observe(element)
    return () => observer.disconnect()
  }, [])
  return <div {...pageStyleTarget('gameReview.board')} ref={frame} className="game-review-board">
    <div className="game-review-board-renderer" style={{ width, height: width }}>
      <div ref={pieces}>
        {width > 0 ? <BoardView.Provider value={{ orientation, width }}><Chessboard
          id={id} position={position} boardOrientation={orientation} boardWidth={width}
          animationDuration={0} arePiecesDraggable={false}
          customSquare={ReviewSquare} showBoardNotation={false}
          customLightSquareStyle={transparentSquare} customDarkSquareStyle={transparentSquare}
        /></BoardView.Provider> : null}
      </div>
      <div ref={overlay} className="game-review-capture-layer" aria-hidden="true" />
    </div>
  </div>
}
