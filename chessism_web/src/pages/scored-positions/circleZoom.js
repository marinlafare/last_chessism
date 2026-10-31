export const INITIAL_CIRCLE_VIEW = { x: 0, y: 0, size: 200 }

export function zoomCircle(view, factor, anchorX = .5, anchorY = .5) {
  if (!Number.isFinite(factor) || factor <= 0) return view
  const size = Math.max(50, Math.min(200, view.size / factor))
  if (size === view.size) return view
  const anchor = value => Number.isFinite(value) ? Math.max(0, Math.min(1, value)) : .5
  return {
    x: Math.max(0, Math.min(200 - size, view.x + anchor(anchorX) * (view.size - size))),
    y: Math.max(0, Math.min(200 - size, view.y + anchor(anchorY) * (view.size - size))),
    size,
  }
}
