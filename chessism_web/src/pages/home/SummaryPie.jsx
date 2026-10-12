import { useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { pieSegments } from '../../utils/pieSegments.js'
import { sliceDescription } from './summaryData.js'

export default function SummaryPie({ items, ready }) {
  const [tip, setTip] = useState(null)
  const slices = pieSegments(items)
  const show = (event, slice) => {
    const rect = event.currentTarget.getBoundingClientRect()
    setTip({ text: sliceDescription(slice), x: event.clientX || rect.x + rect.width / 2,
      y: event.clientY || rect.y + rect.height / 2 })
  }
  return (
    <figure {...pageStyleTarget('summary.positions')} className="home-summary-pie home-summary-positions">
      <svg viewBox="0 0 200 200" role="group" aria-label="Positions distribution">
        {!slices.length && <circle className="home-summary-empty" cx="100" cy="100" r="94" />}
        {slices.map(slice => {
          const Shape = slice.full ? 'circle' : 'path'
          return <Shape key={slice.key} {...(slice.target ? pageStyleTarget(slice.target) : {})}
            className={`home-summary-slice ${slice.key === 'titled' ? 'titled' : slice.key === 'other' ? 'other' : ''}`}
            {...(slice.full ? { cx: 100, cy: 100, r: 94 } : { d: slice.path })}
            tabIndex="0" role="img" aria-label={sliceDescription(slice)}
            onPointerMove={event => show(event, slice)} onPointerLeave={() => setTip(null)}
            onFocus={event => show(event, slice)} onBlur={() => setTip(null)}
            onKeyDown={event => {
              if (event.key === 'Escape') setTip(null)
            }} />
        })}
      </svg>
      <figcaption {...pageStyleTarget('label')}>Positions</figcaption>
      {!slices.length && <span className="home-summary-empty-label">{ready ? 'No data yet' : 'Preparing…'}</span>}
      {tip && <div className="home-summary-tooltip" role="tooltip" style={{
        left: Math.max(8, Math.min(tip.x + 12, window.innerWidth - Math.min(280, window.innerWidth - 16) - 8)),
        top: Math.max(8, Math.min(tip.y + 16, window.innerHeight - 90)),
      }}>{tip.text}</div>}
    </figure>
  )
}
