import { Children, createContext, forwardRef, useContext, useId, useLayoutEffect, useRef, useState } from 'react'
import { Chessboard } from 'react-chessboard'
import { pageStyleTarget } from '../../appearance/styleCore'

const View = createContext({ orientation: 'white', width: 280 })
const transparent = { backgroundColor: 'transparent' }
const Square = forwardRef(function Square({ square, squareColor, style, children }, ref) {
  const { orientation, width } = useContext(View)
  return <div {...pageStyleTarget(`scorePositions.board.square.${squareColor === 'white' ? 'light' : 'dark'}`)}
    ref={ref} className={`score-position-square ${squareColor}`} style={style}>
    {Children.map(children, child => child?.props?.piece ? <span
      {...pageStyleTarget(`scorePositions.board.piece.${child.props.piece[0] === 'w' ? 'white' : 'black'}`)}
      className="score-position-piece">{child}</span> : child)}
    {square[0] === (orientation === 'white' ? 'a' : 'h') ? <span
      {...pageStyleTarget('scorePositions.board.rank')} className="score-position-coordinate rank"
      style={{ '--coordinate-size': `${width / 48}px` }}>{square[1]}</span> : null}
    {square[1] === (orientation === 'white' ? '1' : '8') ? <span
      {...pageStyleTarget('scorePositions.board.file')} className="score-position-coordinate file"
      style={{ '--coordinate-size': `${width / 48}px` }}>{square[0]}</span> : null}
  </div>
})

export default function ScorePositionBoard({ fen, orientation }) {
  const id = useId(), frame = useRef(null)
  const [width, setWidth] = useState(0)
  useLayoutEffect(() => {
    const observer = new ResizeObserver(([entry]) => setWidth(Math.max(0, Math.floor(entry.contentRect.width))))
    observer.observe(frame.current)
    return () => observer.disconnect()
  }, [])
  return <div {...pageStyleTarget('scorePositions.board')} ref={frame} className="score-position-board">
    {width > 0 ? <View.Provider value={{ orientation, width }}><Chessboard id={`score-position-${id}`}
      position={fen} boardWidth={width} boardOrientation={orientation} animationDuration={0}
      arePiecesDraggable={false} areArrowsAllowed={false} customSquare={Square} showBoardNotation={false}
      customLightSquareStyle={transparent} customDarkSquareStyle={transparent} /></View.Provider> : null}
  </div>
}
