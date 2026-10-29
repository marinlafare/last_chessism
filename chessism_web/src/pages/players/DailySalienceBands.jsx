import { useId } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { SALIENCE_BAND_LABELS } from './dailySalienceBands'

export default function DailySalienceBands({ selected, snapshot, onChange, disabled = false }) {
  const id = useId()
  const ready = snapshot?.status === 'ready'
  const keyDown = event => {
    const buttons = [...event.currentTarget.querySelectorAll('button:not(:disabled)')]
    const index = buttons.indexOf(event.target)
    if (index < 0 || !['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return
    event.preventDefault()
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? buttons.length - 1
      : (index + (['ArrowUp', 'ArrowLeft'].includes(event.key) ? -1 : 1) + buttons.length) % buttons.length
    buttons[next]?.focus()
  }
  return <div {...pageStyleTarget('player.accuracy.salience.panel')} className="daily-salience-control daily-salience-bands"
    role="group" aria-labelledby={id} onKeyDown={keyDown}>
    <strong {...pageStyleTarget('player.accuracy.salience.title')} id={id}>Salience</strong>
    <button {...pageStyleTarget('player.accuracy.salience.all')} type="button" aria-pressed={selected === null}
      disabled={disabled || !ready} onClick={() => onChange(null)}>All</button>
    {SALIENCE_BAND_LABELS.map((label, band) => <button {...pageStyleTarget(`player.accuracy.salience.band.${band}`)}
      key={band} type="button" data-salience-band={band} aria-label={`Daily salience ${label}`}
      title={`Show days whose most common salience band is ${label}. Tied bands are included.`}
      aria-pressed={selected === band} disabled={disabled || !ready}
      onClick={() => onChange(selected === band ? null : band)}>{label}</button>)}
    {!ready && <span {...pageStyleTarget('player.accuracy.salience.status')} role="status">
      {snapshot?.status === 'unavailable_timezone' ? 'Unavailable for this timezone' : 'Awaiting CUDA run'}
    </span>}
  </div>
}
