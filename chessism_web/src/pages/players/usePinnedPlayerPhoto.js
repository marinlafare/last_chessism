import { useEffect, useState } from 'react'

// A sticky child of the hero would stop at the hero's bottom. Track the photo's
// original slot instead, and keep only its portal pinned across the workspace.
export default function usePinnedPlayerPhoto(anchorRef, boundaryRef, enabled) {
  const [position, setPosition] = useState(null)
  useEffect(() => {
    const anchor = anchorRef.current, boundary = boundaryRef?.current
    if (!enabled || !anchor || !boundary) { setPosition(null); return undefined }
    let frame = 0
    const measure = () => {
      frame = 0
      const photo = anchor.getBoundingClientRect(), workspace = boundary.getBoundingClientRect()
      // The card lifts on hover. Ignore that visual translation so handing the
      // pointer to the portal cannot toggle pinning repeatedly at the threshold.
      const transform = getComputedStyle(anchor.parentElement).transform
      const lift = transform === 'none' ? 0 : new DOMMatrixReadOnly(transform).m42
      const gap = 12, viewportWidth = document.documentElement.clientWidth
      let next = null
      if (photo.top - lift < gap && photo.width > 0 && workspace.bottom > 0) {
        const width = Math.min(photo.width, Math.max(1, viewportWidth - gap * 2))
        const height = photo.height * width / photo.width
        next = { width, height, top: Math.min(gap, workspace.bottom - height),
          left: Math.max(gap, Math.min(photo.left, viewportWidth - width - gap)) }
      }
      setPosition(previous => {
        if (previous === next || (previous && next && Object.keys(next).every(key => previous[key] === next[key]))) return previous
        return next
      })
    }
    const schedule = () => { if (!frame) frame = requestAnimationFrame(measure) }
    const observer = new ResizeObserver(schedule)
    observer.observe(anchor)
    observer.observe(boundary)
    observer.observe(document.documentElement)
    window.addEventListener('scroll', schedule, { passive: true, capture: true })
    window.addEventListener('resize', schedule)
    measure()
    return () => {
      cancelAnimationFrame(frame)
      observer.disconnect()
      window.removeEventListener('scroll', schedule, true)
      window.removeEventListener('resize', schedule)
    }
  }, [anchorRef, boundaryRef, enabled])
  return enabled ? position : null
}
