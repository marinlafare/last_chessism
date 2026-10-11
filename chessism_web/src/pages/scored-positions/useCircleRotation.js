import { useEffect, useState } from 'react'
import { rotateCircle, wheelRotation } from './circleRotation'

export default function useCircleRotation() {
  const [element, ref] = useState(null)
  const [angle, setAngle] = useState(0)
  const reset = () => setAngle(0)
  useEffect(() => {
    if (!element) return undefined
    const wheel = event => {
      if (event.ctrlKey || !event.deltaY || Math.abs(event.deltaX) > Math.abs(event.deltaY)) return
      const rect = element.getBoundingClientRect(), side = Math.min(rect.width, rect.height)
      // Scroll the page normally outside the actual disk, including SVG corners.
      if (!side || Math.hypot(event.clientX - rect.left - rect.width / 2,
        event.clientY - rect.top - rect.height / 2) > side * 94 / 200) return
      const delta = wheelRotation(event.deltaY, event.deltaMode, side)
      if (!delta) return
      event.preventDefault()
      setAngle(current => rotateCircle(current, delta))
    }
    element.addEventListener('wheel', wheel, { passive: false })
    return () => element.removeEventListener('wheel', wheel)
  }, [element])
  return { angle, svg: { ref, viewBox: '0 0 200 200', 'data-rotation': angle,
    tabIndex: 0, onDoubleClick: reset, onKeyDown: event => {
      if (!['ArrowLeft', 'ArrowRight', 'ArrowUp', 'ArrowDown', '0', 'Home'].includes(event.key)) return
      event.preventDefault()
      event.stopPropagation()
      if (event.key === '0' || event.key === 'Home') reset()
      else setAngle(current => rotateCircle(current, ['ArrowLeft', 'ArrowUp'].includes(event.key) ? -15 : 15))
    } } }
}
