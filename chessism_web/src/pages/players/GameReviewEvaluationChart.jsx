import { useEffect, useId, useMemo, useRef, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'
import { evaluationDescription } from './gameEvaluationLabel'
import { reviewEvaluationSeries } from './reviewEvaluationSeries'
import './game-review-evaluation-chart.css'

const style = part => pageStyleTarget(`gameReview.chart.${part}`)
const moveLabel = move => `${move.move_number}${move.move_color === 'black' ? '…' : '.'} ${move.san || ''}`

export default function GameReviewEvaluationChart({ moves, positionIndex, onNavigate }) {
  const stage = useRef(null)
  const [width, setWidth] = useState(480)
  const [hovered, setHovered] = useState(null)
  const descriptionId = useId()
  useEffect(() => {
    const observer = new ResizeObserver(([entry]) => setWidth(Math.max(160, entry.contentRect.width)))
    observer.observe(stage.current)
    return () => observer.disconnect()
  }, [])
  const series = useMemo(() => reviewEvaluationSeries(moves, width), [moves, width])
  const { points, segments, left, right, top, bottom, zero } = series
  const active = points[positionIndex - 1]
  const inspected = points[(hovered ?? 0) - 1]
  const nearest = event => {
    const matrix = event.currentTarget.getScreenCTM()
    if (!matrix || !points.length) return null
    const { x } = new DOMPoint(event.clientX, event.clientY).matrixTransform(matrix.inverse())
    return Math.max(1, Math.min(points.length, Math.round((x - left) / (right - left) * (points.length - 1)) + 1))
  }
  return <section {...style('container')} className="game-review-timeline" aria-label="Move evaluations">
    <div ref={stage} className="game-review-timeline-stage">
      <svg {...style('plot')} viewBox={`0 0 ${width} 100`} height="100" role="slider" tabIndex={0}
        aria-label="Game move" aria-orientation="horizontal" aria-valuemin={0} aria-valuemax={moves.length}
        aria-valuenow={positionIndex} aria-valuetext={active ? `${moveLabel(active.move)}, ${evaluationDescription(active.move.evaluation_after)}` : 'Starting position'}
        aria-describedby={descriptionId}
        onPointerMove={event => setHovered(nearest(event))} onPointerLeave={() => setHovered(null)}
        onClick={event => { const index = nearest(event); if (index !== null) onNavigate(index) }}
        onKeyDown={event => {
          if (event.key === 'Home' || event.key === 'End') {
            event.preventDefault(); onNavigate(event.key === 'Home' ? 0 : moves.length)
          }
        }}>
        <desc id={descriptionId}>Move number horizontally; evaluation in pawns from White’s perspective vertically. Scores beyond ±5 pawns, forced mates and decisive tablebase results are drawn at the edges. Gaps mean no evaluation. Click to review a move, or use Left and Right arrow keys.</desc>
        <rect {...style('blackArea')} className="review-chart-black" x={left} y={top} width={right - left} height={bottom - top} />
        {segments.map((segment, index) => {
          const line = segment.map(point => `${point.x},${point.y}`).join(' L')
          return <g key={index}>
            <path {...style('whiteArea')} className="review-chart-white" d={`M${segment[0].x},${bottom} L${line} L${segment.at(-1).x},${bottom} Z`} />
            <path {...style('line')} className="review-chart-line" d={`M${line}`} />
          </g>
        })}
        <line {...style('zero')} className="review-chart-zero" x1={left} x2={right} y1={zero} y2={zero} />
        {active ? <line {...style('selected')} className="review-chart-selected" x1={active.x} x2={active.x} y1={top} y2={bottom} /> : null}
        {active && active.y !== null ? <circle {...style('currentPoint')} className="review-chart-current-point"
          data-ply={active.index} cx={active.x} cy={active.y} r={3.5} aria-hidden="true" /> : null}
        {!segments.length ? <text {...style('empty')} className="review-chart-label" x={(left + right) / 2} y={zero - 8} textAnchor="middle">No evaluations available</text> : null}
      </svg>
      {inspected ? <output {...style('tooltip')} className="game-review-timeline-tooltip"
        style={{ left: `clamp(1rem, ${inspected.x / width * 100}%, calc(100% - 1rem))` }}>
        {inspected.move.move_number}
      </output> : null}
    </div>
  </section>
}
