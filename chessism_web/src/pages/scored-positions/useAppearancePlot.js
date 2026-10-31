import { useLayoutEffect, useState } from 'react'

// Match SVG coordinates to its actual CSS box: a wide, short panel must not
// letterbox a fixed-aspect-ratio chart into a tiny plot in the middle.
export default function useAppearancePlot() {
  const [element, ref] = useState(null)
  const [size, setSize] = useState({ width: 760, height: 180 })
  useLayoutEffect(() => {
    if (!element) return undefined
    const measure = () => {
      const { width, height } = element.getBoundingClientRect()
      // Hidden tab panels report zero; retain their last useful geometry.
      if (width <= 0 || height <= 0) return
      setSize(previous => Math.abs(previous.width - width) < .1 && Math.abs(previous.height - height) < .1
        ? previous : { width, height })
    }
    const observer = new ResizeObserver(measure)
    observer.observe(element)
    measure()
    return () => observer.disconnect()
  }, [element])
  return { ref, ...size }
}
