import { useEffect, useId, useLayoutEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { pieSegments } from '../../utils/pieSegments.js'
import useCircleRotation from '../../hooks/useCircleRotation'

const integer = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 })
const count = value => Number.isFinite(Number(value)) ? Math.max(0, Math.trunc(Number(value))) : 0

export default function PlayerCoveragePie({ kind, total, scored, ready }) {
  const [tip, setTip] = useState(null)
  const tooltipId = useId()
  const figureRef = useRef(null)
  const rotation = useCircleRotation()
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
  useLayoutEffect(() => {
    // Rotation can bring a different slice under a stationary pointer.
    setTip(current => {
      if (!current?.pointer) return current
      const slice = document.elementFromPoint(current.x, current.y)?.closest('.player-coverage-slice')
      if (!slice || !figureRef.current?.contains(slice)) return null
      const sliceKey = slice.dataset.coverageSlice
      const value = sliceKey === 'scored' ? completed : all - completed
      return sliceKey === current.sliceKey && value === current.value ? current : { ...current, sliceKey, value }
    })
  }, [rotation.angle, all, completed])

  const show = (event, slice) => {
    const bounds = event.currentTarget.getBoundingClientRect()
    const pointer = event.type.startsWith('pointer')
    setTip({ x: pointer ? event.clientX : bounds.left + bounds.width / 2,
      y: pointer ? event.clientY : bounds.top + bounds.height / 2,
      pointer,
      sliceKey: slice.key,
      value: slice.value })
  }
  const tipText = tip ? `${tip.sliceKey === 'scored' ? 'scored: ' : ''}${integer.format(tip.value)}` : ''

  return (
    <figure ref={figureRef} className={`player-coverage-pie player-coverage-${kind}`} aria-busy={!ready}>
      <svg {...rotation.svg} role="group" aria-label={`${label} analysis coverage. Scroll to rotate; double-click to reset.`}>
        {!slices.length && <circle className="player-coverage-empty" cx="100" cy="100" r="94" />}
        <g transform={`rotate(${rotation.angle} 100 100)`}>
        {slices.map(slice => {
          const Shape = slice.full ? 'circle' : 'path'
          return <Shape key={slice.key} className={`player-coverage-slice ${slice.key}`}
            data-coverage-slice={slice.key}
            {...(slice.full ? { cx: 100, cy: 100, r: 94 } : { d: slice.path })}
            tabIndex="0" role="img"
            aria-label={`${slice.key === 'scored' ? 'Scored' : 'Unscored'} ${kind}: ${integer.format(slice.value)}. ${description}`}
            aria-describedby={tip?.sliceKey === slice.key ? tooltipId : undefined}
            onPointerEnter={event => show(event, slice)} onPointerMove={event => show(event, slice)} onPointerLeave={() => setTip(null)}
            onFocus={event => show(event, slice)} onBlur={() => setTip(null)}
            onKeyDown={event => { if (event.key === 'Escape') setTip(null) }} />
        })}
        </g>
        <text className="player-coverage-percent" x="100" y="108" textAnchor="middle">
          {ready ? `${percentage}%` : '…'}
        </text>
      </svg>
      <figcaption {...pageStyleTarget('label')}>{label}</figcaption>
      {ready && !all && <small>No {kind} yet</small>}
      {tip && createPortal(<div className="player-coverage-tooltip" role="tooltip" id={tooltipId} style={{
        left: Math.max(8, Math.min(tip.x + 14, window.innerWidth - (28 + tipText.length * 8))),
        top: Math.max(8, Math.min(tip.y + 14, window.innerHeight - 44)),
      }}>{tipText}</div>, document.body)}
    </figure>
  )
}
