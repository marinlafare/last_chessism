import { useLayoutEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'

export default function AppearanceCircleTooltip({ selection }) {
  const node = useRef(null)
  const [position, setPosition] = useState({ left: 0, top: 0 })
  useLayoutEffect(() => {
    if (!selection || !node.current) return
    const { width, height } = node.current.getBoundingClientRect()
    const { x, y } = selection
    const left = x + 14 + width > window.innerWidth - 8 ? x - width - 14 : x + 14
    const top = y + 14 + height > window.innerHeight - 8 ? y - height - 14 : y + 14
    setPosition({ left: Math.max(8, Math.min(left, window.innerWidth - width - 8)),
      top: Math.max(8, Math.min(top, window.innerHeight - height - 8)) })
  }, [selection])
  if (!selection) return null
  return createPortal(<div ref={node} className="appearance-circle-tooltip" id="appearance-circle-tooltip"
    role="tooltip" style={position}>
    <strong>{selection.percentage}</strong><span>{selection.label}</span>
  </div>, document.body)
}
