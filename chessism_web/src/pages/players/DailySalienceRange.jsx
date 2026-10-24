import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useId, useRef } from 'react'
import { moveSalienceBound } from './dailySalience'

export default function DailySalienceRange({ range, scale, onChange }) {
  const id = useId()
  const track = useRef(null)
  const drag = useRef(null)
  const disabled = !scale.enabled
  const bounds = disabled ? [0, 100] : range
  const actual = (value) => scale.minimum + (scale.maximum - scale.minimum) * value / 100
  const pointerValue = (event) => {
    const rect = track.current.getBoundingClientRect()
    // Absolute-positioned handles use the padding box, excluding the border.
    // Keep pointer mapping accurate when Edit styles changes either of them.
    const height = track.current.clientHeight
    const bottom = rect.top + track.current.clientTop + height
    return height ? Math.max(0, Math.min(100, 100 * (bottom - event.clientY) / height)) : 0
  }
  const pointerMove = (event) => {
    if (disabled || drag.current?.pointerId !== event.pointerId) return
    onChange(moveSalienceBound(bounds, drag.current.index, pointerValue(event)))
  }
  const pointerDown = (event) => {
    if (disabled || event.button !== 0 || drag.current !== null) return
    event.preventDefault()
    const value = pointerValue(event)
    const handle = event.target.closest('[data-salience-handle]')
    const index = handle ? Number(handle.dataset.salienceHandle)
      : Math.abs(value - bounds[0]) <= Math.abs(value - bounds[1]) ? 0 : 1
    drag.current = { index, pointerId: event.pointerId }
    track.current.setPointerCapture(event.pointerId)
    track.current.querySelector(`[data-salience-handle="${index}"]`).focus()
    pointerMove(event)
  }
  const keyDown = (event, index) => {
    const values = {
      ArrowUp: bounds[index] + 1, ArrowRight: bounds[index] + 1,
      ArrowDown: bounds[index] - 1, ArrowLeft: bounds[index] - 1,
      PageUp: bounds[index] + 10, PageDown: bounds[index] - 10,
      Home: index === 0 ? 0 : bounds[0], End: index === 0 ? bounds[1] : 100,
    }
    if (disabled || !Object.hasOwn(values, event.key)) return
    event.preventDefault()
    onChange(moveSalienceBound(bounds, index, values[event.key]))
  }

  return (
    <div {...pageStyleTarget('player.accuracy.salience.panel')} className="daily-salience-control" role="group" aria-labelledby={`${id}-label`}>
      <strong {...pageStyleTarget('player.accuracy.salience.title')} id={`${id}-label`}>salience</strong>
      <div {...pageStyleTarget('player.accuracy.salience.track')} className="daily-salience-track" ref={track} data-disabled={disabled}
        onPointerDown={pointerDown} onPointerMove={pointerMove}
        onPointerUp={(event) => {
          if (drag.current?.pointerId !== event.pointerId) return
          pointerMove(event)
          drag.current = null
          if (track.current.hasPointerCapture(event.pointerId)) track.current.releasePointerCapture(event.pointerId)
        }}
        onPointerCancel={() => { drag.current = null }} onLostPointerCapture={() => { drag.current = null }}>
        <span {...pageStyleTarget('player.accuracy.salience.rail')} className="daily-salience-rail" aria-hidden="true" />
        <span {...pageStyleTarget('player.accuracy.salience.fill')} className="daily-salience-fill" aria-hidden="true"
          style={{ bottom: `${bounds[0]}%`, height: `${bounds[1] - bounds[0]}%` }} />
        {[0, 25, 50, 75, 100].map((value) => (
          <span {...pageStyleTarget('player.accuracy.salience.ticks')} key={value} className="daily-salience-tick" style={{ bottom: `${value}%` }} aria-hidden="true">{value}</span>
        ))}
        {bounds.map((value, index) => (
          <button {...pageStyleTarget(`player.accuracy.salience.${index === 0 ? 'minimum' : 'maximum'}.handle`)} key={index} type="button" role="slider" className="daily-salience-handle"
            data-salience-handle={index} disabled={disabled} style={{ bottom: `${value}%` }}
            aria-label={`${index === 0 ? 'Minimum' : 'Maximum'} relative average game salience`}
            aria-orientation="vertical" aria-valuemin={index === 0 ? 0 : bounds[0]}
            aria-valuemax={index === 0 ? bounds[1] : 100} aria-valuenow={value}
            aria-valuetext={disabled ? 'Salience range unavailable'
              : `${value} of 100; ${actual(value).toPrecision(4)} average game salience`}
            onKeyDown={(event) => keyDown(event, index)}>
            <span {...pageStyleTarget(`player.accuracy.salience.${index === 0 ? 'minimum' : 'maximum'}.knob`)} aria-hidden="true" />
          </button>
        ))}
      </div>
    </div>
  )
}
