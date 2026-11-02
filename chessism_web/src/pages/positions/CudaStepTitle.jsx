import { useEffect, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'

// Keyed by run + step, not poll time or counters. A new step cancels the old
// animation immediately; repeated status polls never restart the typing.
export default function CudaStepTitle({ title, active }) {
  const [reduced, setReduced] = useState(() => window.matchMedia('(prefers-reduced-motion: reduce)').matches)
  const [length, setLength] = useState(0)
  const characters = Array.from(title)
  useEffect(() => {
    const preference = window.matchMedia('(prefers-reduced-motion: reduce)')
    const changed = () => setReduced(preference.matches)
    preference.addEventListener('change', changed)
    return () => preference.removeEventListener('change', changed)
  }, [])
  useEffect(() => {
    if (reduced) return undefined
    let frame, started
    const duration = Math.min(1000, characters.length * 28)
    const tick = now => {
      started ??= now
      const next = Math.min(characters.length, Math.floor((now - started) / Math.max(1, duration) * characters.length))
      setLength(next)
      if (next < characters.length) frame = requestAnimationFrame(tick)
    }
    setLength(0)
    frame = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(frame)
  }, [title, reduced])
  const visible = reduced ? title : characters.slice(0, length).join('')
  const typing = !reduced && length < characters.length
  return <strong {...pageStyleTarget('cuda.progress.phase')} className="cuda-terminal-title"
    data-title={title} data-typing={typing}>
    <span className="cuda-terminal-accessible" role="status" aria-live="polite" aria-atomic="true">{title}</span>
    <span className="cuda-terminal-measure" aria-hidden="true">{title}<span className="cuda-terminal-cursor" /></span>
    <span className="cuda-terminal-line" aria-hidden="true"><span className="cuda-terminal-text">{visible}</span>
      <span className="cuda-terminal-cursor" data-blinking={active && !typing && !reduced} /></span>
  </strong>
}
