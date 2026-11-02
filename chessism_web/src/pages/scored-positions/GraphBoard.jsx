import { Children, createContext, forwardRef, useContext, useId, useLayoutEffect, useRef, useState } from 'react'
import { Chessboard } from 'react-chessboard'
import { styleTarget } from '../../appearance/styleCore'

export const graphStyle = part => styleTarget(`commonGraph.${part}`)
const View = createContext('white')
const transparent = { backgroundColor: 'transparent' }
const Square = forwardRef(function Square({ square, squareColor, children, style }, ref) {
  const orientation = useContext(View)
  return <div {...graphStyle(`board.${squareColor === 'white' ? 'light' : 'dark'}`)} ref={ref}
    className={`common-graph-square ${squareColor}`} style={style}>
    {Children.map(children, child => child?.props?.piece ? <span {...graphStyle(`board.${child.props.piece[0] === 'w' ? 'whitePiece' : 'blackPiece'}`)}>{child}</span> : child)}
    {square[0] === (orientation === 'white' ? 'a' : 'h') && <span {...graphStyle('board.rank')} className="common-graph-coordinate rank">{square[1]}</span>}
    {square[1] === (orientation === 'white' ? '1' : '8') && <span {...graphStyle('board.file')} className="common-graph-coordinate file">{square[0]}</span>}
  </div>
})

export default function GraphBoard({ fen, orientation = 'white', preview = false }) {
  const id = useId(), frame = useRef(null)
  const [width, setWidth] = useState(0)
  useLayoutEffect(() => {
    const observer = new ResizeObserver(([entry]) => setWidth(Math.floor(entry.contentRect.width)))
    observer.observe(frame.current)
    return () => observer.disconnect()
  }, [])
  return <div {...graphStyle(preview ? 'previewBoard' : 'board')} ref={frame} className="common-graph-board">
    {width > 0 && fen && <View.Provider value={orientation}><Chessboard id={`graph-board-${id}`}
      position={fen.split(' ').length === 4 ? `${fen} 0 1` : fen} boardWidth={width}
      boardOrientation={orientation} arePiecesDraggable={false} areArrowsAllowed={false}
      animationDuration={0} showBoardNotation={false} customSquare={Square}
      customLightSquareStyle={transparent} customDarkSquareStyle={transparent} /></View.Provider>}
  </div>
}

export function graphEvaluation(position) {
  if (!position) return ''
  if (position.score_kind === 'cp') return `${position.score_cp > 0 ? '+' : ''}${position.score_cp} cp · White's perspective`
  if (position.score_kind === 'mate' && position.mate_in != null) return `${position.mate_for_white ? 'White' : 'Black'} mates in ${position.mate_in}`
  if (position.score_kind === 'tablebase') return `Tablebase · ${position.tablebase_wdl > 0 ? 'White winning' : position.tablebase_wdl < 0 ? 'Black winning' : position.tablebase_wdl === 0 ? 'Draw' : 'Result unavailable'}`
  return 'Evaluation unavailable'
}
