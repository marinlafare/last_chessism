// Shared by pointer dragging and viewport changes; never let the header escape.
export const EDITOR_MARGIN = 8
export const RESIZE_EDGES = ['n', 's', 'e', 'w', 'ne', 'nw', 'se', 'sw']
export function fitEditor(box, viewport) {
  const maxWidth = Math.max(1, viewport.width - EDITOR_MARGIN * 2)
  const maxHeight = Math.max(1, viewport.height - EDITOR_MARGIN * 2)
  const width = Math.min(maxWidth, Math.max(Math.min(280, maxWidth), box.width))
  const height = Math.min(maxHeight, Math.max(Math.min(160, maxHeight), box.height))
  return { width, height,
    left: Math.max(EDITOR_MARGIN, Math.min(box.left, viewport.width - width - EDITOR_MARGIN)),
    top: Math.max(EDITOR_MARGIN, Math.min(box.top, viewport.height - height - EDITOR_MARGIN)) }
}
export function resizeEditor(start, edge, dx, dy, viewport) {
  const minWidth = Math.min(280, viewport.width - EDITOR_MARGIN * 2)
  const minHeight = Math.min(160, viewport.height - EDITOR_MARGIN * 2)
  const right = start.left + start.width, bottom = start.top + start.height
  let { left, top, width, height } = start
  if (edge.includes('w')) { left = Math.max(EDITOR_MARGIN, Math.min(right - minWidth, left + dx)); width = right - left }
  if (edge.includes('e')) width = Math.max(minWidth, Math.min(viewport.width - left - EDITOR_MARGIN, width + dx))
  if (edge.includes('n')) { top = Math.max(EDITOR_MARGIN, Math.min(bottom - minHeight, top + dy)); height = bottom - top }
  if (edge.includes('s')) height = Math.max(minHeight, Math.min(viewport.height - top - EDITOR_MARGIN, height + dy))
  return fitEditor({ left, top, width, height }, viewport)
}
