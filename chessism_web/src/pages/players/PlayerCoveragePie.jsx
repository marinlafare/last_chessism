import { useEffect, useId, useState } from 'react'
import { createPortal } from 'react-dom'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { pieSegments } from '../../utils/pieSegments.js'

const integer = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 })
const count = value => Number.isFinite(Number(value)) ? Math.max(0, Math.trunc(Number(value))) : 0

export default function PlayerCoveragePie({ kind, total, scored, ready }) {
  const [tip, setTip] = useState(null)
  const tooltipId = useId()
  const all = count(total), completed = Math.min(all, count(scored))
  const label = kind === 'games' ? 'Games' : 'Positions'
  const slices = ready ? pieSegments([
    { key: 'scored', value: completed },
    { key: 'unscored', value: all - completed },
  ]) : []
  const percentage = all > 0 ? Math.round(completed / all * 100) : 0
  const description = `${label}: ${integer.format(all)}\nScored ${kind}: ${integer.format(completed)}`

  useEffect(() => {
    const clear = () => setTip(null)
    window.addEventListener('scroll', clear, true)
    window.addEventListener('resize', clear)
    return () => { window.removeEventListener('scroll', clear, true); window.removeEventListener('resize', clear) }
  }, [])
  useEffect(() => setTip(null), [total, scored, ready])

  const show = event => {
    const bounds = event.currentTarget.getBoundingClientRect()
    const pointer = event.type.startsWith('pointer')
    setTip({ x: pointer ? event.clientX : bounds.left + bounds.width / 2,
      y: pointer ? event.clientY : bounds.top + bounds.height / 2 })
  }

  return (
    <figure className={`player-coverage-pie player-coverage-${kind}`} aria-busy={!ready}>
      <svg viewBox="0 0 200 200" role="group" aria-label={`${label} analysis coverage`}>
        {!slices.length && <circle className="player-coverage-empty" cx="100" cy="100" r="94" />}
        {slices.map(slice => {
          const Shape = slice.full ? 'circle' : 'path'
          return <Shape key={slice.key} className={`player-coverage-slice ${slice.key}`}
            {...(slice.full ? { cx: 100, cy: 100, r: 94 } : { d: slice.path })}
            tabIndex="0" role="img"
            aria-label={`${slice.key === 'scored' ? 'Scored' : 'Unscored'} ${kind}: ${integer.format(slice.value)}. ${description}`}
            aria-describedby={tip ? tooltipId : undefined}
            onPointerEnter={show} onPointerMove={show} onPointerLeave={() => setTip(null)}
            onFocus={show} onBlur={() => setTip(null)}
            onKeyDown={event => { if (event.key === 'Escape') setTip(null) }} />
        })}
        <text className="player-coverage-percent" x="100" y="108" textAnchor="middle">
          {ready ? `${percentage}%` : '…'}
        </text>
      </svg>
      <figcaption {...pageStyleTarget('label')}>{label}</figcaption>
      {ready && !all && <small>No {kind} yet</small>}
      {tip && createPortal(<div className="player-coverage-tooltip" role="tooltip" id={tooltipId} style={{
        left: Math.max(8, Math.min(tip.x + 14, window.innerWidth - 252)),
        top: Math.max(8, Math.min(tip.y + 14, window.innerHeight - 76)),
      }}>{description}</div>, document.body)}
    </figure>
  )
}
