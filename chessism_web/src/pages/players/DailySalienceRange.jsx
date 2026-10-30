import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useId, useRef } from 'react'
import { moveSalienceBound } from './dailySalience'
import { SALIENCE_BAND_BOUNDARIES, moveSalienceBandBound, salienceBoundStops, salienceBandOffset, salienceBandValue } from './dailySalienceBands'

export default function DailySalienceRange({ range, scale, onChange, banded = false, unavailableMessage = '' }) {
  const id = useId()
  const track = useRef(null)
  const drag = useRef(null)
  const disabled = !scale.enabled
  const bounds = disabled ? [0, 100] : range
  const moveBound = banded ? moveSalienceBandBound : moveSalienceBound
  const ticks = banded ? SALIENCE_BAND_BOUNDARIES : [0, 25, 50, 75, 100]
  const offset = banded ? salienceBandOffset : value => value
  const actual = (value) => scale.minimum + (scale.maximum - scale.minimum) * value / 100
  const pointerValue = (event) => {
    const rect = track.current.getBoundingClientRect()
    // Absolute-positioned handles use the padding box, excluding the border.
    // Keep pointer mapping accurate when Edit styles changes either of them.
    const height = track.current.clientHeight
    const bottom = rect.top + track.current.clientTop + height
    const position = height ? Math.max(0, Math.min(100, 100 * (bottom - event.clientY) / height)) : 0
    return banded ? salienceBandValue(position) : position
  }
  const pointerMove = (event) => {
    if (disabled || drag.current?.pointerId !== event.pointerId) return
    onChange(moveBound(bounds, drag.current.index, pointerValue(event)))
  }
  const pointerDown = (event) => {
    if (disabled || event.button !== 0 || drag.current !== null) return
    event.preventDefault()
    const value = pointerValue(event)
    const handle = event.target.closest('[data-salience-handle]')
    const index = handle ? Number(handle.dataset.salienceHandle)
      : Math.abs(offset(value) - offset(bounds[0])) <= Math.abs(offset(value) - offset(bounds[1])) ? 0 : 1
    drag.current = { index, pointerId: event.pointerId }
    track.current.setPointerCapture(event.pointerId)
    track.current.querySelector(`[data-salience-handle="${index}"]`).focus()
    pointerMove(event)
  }
  const keyDown = (event, index) => {
    const stops = banded ? salienceBoundStops(bounds, index) : null
    const position = stops?.indexOf(bounds[index])
    const higher = banded ? stops[Math.min(position + 1, stops.length - 1)] : bounds[index] + 1
    const lower = banded ? stops[Math.max(position - 1, 0)] : bounds[index] - 1
    const values = {
      ArrowUp: higher, ArrowRight: higher, ArrowDown: lower, ArrowLeft: lower,
      PageUp: banded ? higher : bounds[index] + 10, PageDown: banded ? lower : bounds[index] - 10,
      Home: banded ? stops[0] : index === 0 ? 0 : bounds[0],
      End: banded ? stops[stops.length - 1] : index === 0 ? bounds[1] : 100,
    }
    if (disabled || !Object.hasOwn(values, event.key)) return
    event.preventDefault()
    onChange(moveBound(bounds, index, values[event.key]))
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
          style={{ bottom: `${offset(bounds[0])}%`, height: `${offset(bounds[1]) - offset(bounds[0])}%` }} />
        {ticks.map((value) => (
          <span {...pageStyleTarget('player.accuracy.salience.ticks')} key={value} className="daily-salience-tick" style={{ bottom: `${offset(value)}%` }} aria-hidden="true">{value}</span>
        ))}
        {bounds.map((value, index) => (
          <button {...pageStyleTarget(`player.accuracy.salience.${index === 0 ? 'minimum' : 'maximum'}.handle`)} key={index} type="button" role="slider" className="daily-salience-handle"
            data-salience-handle={index} disabled={disabled} style={{ bottom: `${offset(value)}%` }}
            aria-label={`${index === 0 ? 'Minimum' : 'Maximum'} ${banded ? 'daily salience bin boundary' : 'relative average game salience'}`}
            aria-orientation="vertical" aria-valuemin={banded ? salienceBoundStops(bounds, index)[0] : index === 0 ? 0 : bounds[0]}
            aria-valuemax={banded ? salienceBoundStops(bounds, index).at(-1) : index === 0 ? bounds[1] : 100} aria-valuenow={value}
            title={banded ? `${bounds[0]}–${bounds[1]}: most common daily salience bins (including ties)` : undefined}
            aria-valuetext={disabled ? 'Salience range unavailable'
              : banded ? `${value} of 100; selected most common daily salience bins: ${bounds[0]}–${bounds[1]}`
                : `${value} of 100; ${actual(value).toPrecision(4)} average game salience`}
            onKeyDown={(event) => keyDown(event, index)}>
            <span {...pageStyleTarget(`player.accuracy.salience.${index === 0 ? 'minimum' : 'maximum'}.knob`)} aria-hidden="true" />
          </button>
        ))}
      </div>
      {unavailableMessage && <span {...pageStyleTarget('player.accuracy.salience.status')} role="status">{unavailableMessage}</span>}
    </div>
  )
}
