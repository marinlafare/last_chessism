import { createPortal } from 'react-dom'
import PositionBoard from '../../components/chess/PositionBoard'
import { formatNumber } from '../../utils/formatters'

export default function CommonPositionPreview({ selection }) {
  if (!selection) return null
  const { bar, rect } = selection
  const width = Math.min(286, window.innerWidth - 16)
  const left = Math.max(8, Math.min(rect.right + 10, window.innerWidth - width - 8))
  const top = Math.max(8, Math.min(rect.top, window.innerHeight - 340))
  return createPortal(<div className="common-position-preview" role="tooltip" id="common-position-preview"
    style={{ left, top, width }}>
    <strong>{bar.label}: {formatNumber(bar.appearances)} times</strong>
    <span className="common-position-caption">Representative arrival</span>
    <div className="common-position-board" aria-label={`Position usually reached after ${bar.label}`}>
      <PositionBoard fen={bar.fen}/>
    </div>
    <p>{bar.example_line ? `Example: ${bar.example_line}` : 'Example sequence unavailable.'}</p>
  </div>, document.body)
}
