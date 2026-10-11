import { useEffect, useRef, useState } from 'react'
import { INITIAL_CIRCLE_VIEW, zoomCircle } from './circleZoom'

export default function useCircleZoom() {
  const [element, ref] = useState(null)
  const [view, setView] = useState(INITIAL_CIRCLE_VIEW)
  const current = useRef(view)
  const update = next => { current.current = next; setView(next) }
  const reset = () => update(INITIAL_CIRCLE_VIEW)
  useEffect(() => {
    if (!element) return undefined
    const wheel = event => {
      // Keep browser zoom and horizontal scrolling available. A native,
      // non-passive listener prevents page scrolling only while zoom changes.
      if (event.ctrlKey || !event.deltaY || Math.abs(event.deltaX) > Math.abs(event.deltaY)) return
      const rect = element.getBoundingClientRect(), side = Math.min(rect.width, rect.height)
      if (!side) return
      const x = (event.clientX - rect.left - (rect.width - side) / 2) / side
      const y = (event.clientY - rect.top - (rect.height - side) / 2) / side
      const delta = event.deltaY * (event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? side : 1)
      const next = zoomCircle(current.current, Math.exp(-Math.max(-600, Math.min(600, delta)) * .002), x, y)
      if (next === current.current) return
      event.preventDefault()
      update(next)
    }
    element.addEventListener('wheel', wheel, { passive: false })
    return () => element.removeEventListener('wheel', wheel)
  }, [element])
  return { ref, viewBox: `${view.x} ${view.y} ${view.size} ${view.size}`, 'data-zoom': 200 / view.size,
    tabIndex: 0, onDoubleClick: reset, onKeyDown: event => {
      if (['+', '=', '-', '0', 'Home'].includes(event.key)) {
        event.preventDefault()
        if (event.key === '0' || event.key === 'Home') reset()
        else update(zoomCircle(current.current, event.key === '-' ? 1 / 1.25 : 1.25))
      }
    } }
}
