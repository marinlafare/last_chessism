import { useId } from 'react'
import { Chessboard } from 'react-chessboard'
export default function PositionBoard({ fen, width=160 }) {
  const id=useId()
  return <Chessboard id={`position-preview-${id}`} position={fen} boardWidth={width}
    arePiecesDraggable={false} areArrowsAllowed={false} animationDuration={0}/>
}
