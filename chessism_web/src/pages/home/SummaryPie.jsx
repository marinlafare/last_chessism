import { useLayoutEffect, useState } from 'react'
import useCircleRotation from '../../hooks/useCircleRotation'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { pieSegments } from '../../utils/pieSegments.js'
import { fenTotalLabel, sliceDescription } from './summaryData.js'

export default function SummaryPie({ items, ready, totalFens, titledOnly = true, onTitleFilterChange }) {
  const [tip, setTip] = useState(null)
  const rotation = useCircleRotation()
  const slices = pieSegments(items)
  useLayoutEffect(() => {
    // A stationary pointer can land on a different slice after rotation.
    setTip(current => {
      if (!current?.pointer) return current
      const slice = document.elementFromPoint(current.x, current.y)?.closest('.home-summary-slice')
      const text = slice?.getAttribute('aria-label')
      return text ? (text === current.text ? current : { ...current, text }) : null
    })
  }, [rotation.angle])
  const show = (event, slice) => {
    const rect = event.currentTarget.getBoundingClientRect()
    const pointer = event.type.startsWith('pointer')
    setTip({ text: sliceDescription(slice), pointer, x: pointer ? event.clientX : rect.x + rect.width / 2,
      y: pointer ? event.clientY : rect.y + rect.height / 2 })
  }
  return (
    <figure {...pageStyleTarget('summary.positions')} className="home-summary-pie home-summary-positions">
      <div {...pageStyleTarget('summary.total')} className="home-summary-total"
        aria-label={ready && Number.isFinite(totalFens) ? `${totalFens.toLocaleString('en-US')} total FENs` : 'Total FENs loading'}>
        {fenTotalLabel(ready ? totalFens : null)}
      </div>
      <svg {...rotation.svg} role="group" aria-label="Positions distribution. Select a slice to show titled or untitled players. Scroll to rotate; double-click to reset.">
        {!slices.length && <circle className="home-summary-empty" cx="100" cy="100" r="94" />}
        <g transform={`rotate(${rotation.angle} 100 100)`}>
        {slices.map(slice => {
          const Shape = slice.full ? 'circle' : 'path'
          const selectGroup = () => onTitleFilterChange?.(slice.key === 'titled')
          return <Shape key={slice.key} {...(slice.target ? pageStyleTarget(slice.target) : {})}
            className={`home-summary-slice ${slice.key === 'titled' ? 'titled' : slice.key === 'other' ? 'other' : ''}`}
            {...(slice.full ? { cx: 100, cy: 100, r: 94 } : { d: slice.path })}
            tabIndex="0" role="button" aria-label={sliceDescription(slice)}
            aria-pressed={titledOnly === (slice.key === 'titled')}
            onClick={selectGroup}
            onPointerMove={event => show(event, slice)} onPointerLeave={() => setTip(null)}
            onFocus={event => show(event, slice)} onBlur={() => setTip(null)}
            onKeyDown={event => {
              if (event.key === 'Escape') setTip(null)
              if (event.key === 'Enter' || event.key === ' ') {
                event.preventDefault()
                selectGroup()
              }
            }} />
        })}
        </g>
      </svg>
      <figcaption {...pageStyleTarget('label')}>Positions</figcaption>
      {!slices.length && <span className="home-summary-empty-label">{ready ? 'No data yet' : 'Preparing…'}</span>}
      {tip && <div className="home-summary-tooltip home-summary-pie-tooltip" role="tooltip" style={{
        left: Math.max(8, Math.min(tip.x + 12, window.innerWidth - Math.min(280, window.innerWidth - 16) - 8)),
        top: Math.max(8, Math.min(tip.y + 16, window.innerHeight - 90)),
      }}>{tip.text}</div>}
    </figure>
  )
}
