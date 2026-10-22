import { useLayoutEffect, useState } from 'react'

// The list determines the panel height. Fit SVG coordinates to that existing
// space so swapping views cannot resize the panel or squash text/photos.
export default function useRatingChartSize() {
  const [element, ref] = useState(null)
  const [size, setSize] = useState({ width: 500, height: 150 })
  useLayoutEffect(() => {
    if (!element) return undefined
    const update = () => {
      const box = element.getBoundingClientRect()
      const width = Math.round(box.width), height = Math.round(box.height)
      if (width > 0 && height > 0) setSize(current => current.width === width && current.height === height
        ? current : { width, height })
    }
    update()
    const observer = new ResizeObserver(update)
    observer.observe(element)
    return () => observer.disconnect()
  }, [element])
  return { ref, ...size }
}
