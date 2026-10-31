import { useLayoutEffect, useState } from 'react'

// Match SVG coordinates to its actual width so compact charts can fill their
// cards without stretching text or leaving letterboxed horizontal margins.
export default function useSummaryChartWidth() {
  const [element, ref] = useState(null)
  const [width, setWidth] = useState(500)
  useLayoutEffect(() => {
    if (!element) return undefined
    const update = () => {
      const next = Math.round(element.getBoundingClientRect().width)
      // A collapsed disclosure has no width; retain the last useful layout.
      if (next > 0) setWidth(current => current === next ? current : next)
    }
    update()
    const observer = new ResizeObserver(update)
    observer.observe(element)
    return () => observer.disconnect()
  }, [element])
  return { ref, width }
}
