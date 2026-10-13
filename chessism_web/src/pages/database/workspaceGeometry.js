import { CARD_WIDTH, CARD_HEIGHT, HEADER_HEIGHT, HIERARCHY_PRESET, buildDatabaseHierarchy, regionsForPositions } from './workspaceHierarchy.js'
export { CARD_WIDTH, CARD_HEIGHT, HEADER_HEIGHT } from './workspaceHierarchy.js'
export const MIN_ZOOM = 0.002
export const MAX_ZOOM = 2
export const DEFAULT_VIEW = { x: 24, y: 24, zoom: 0.85 }
export const WORKSPACE_MARGIN = 3
const bounded = (value, fallback, min, max) => Number.isFinite(value) ? Math.max(min, Math.min(max, value)) : fallback

export function defaultPositions(tables) {
  return buildDatabaseHierarchy(tables).positions
}

function appendedPositions(tables) {
  const columns = Math.max(1, Math.ceil(Math.sqrt(tables.length * 1.4)))
  return Object.fromEntries(tables.map((table, index) => [table.id, {
    x: (index % columns) * (CARD_WIDTH + 52), y: Math.floor(index / columns) * (CARD_HEIGHT + 52),
  }]))
}

export function normalizeLayout(raw, tables) {
  const hierarchy = buildDatabaseHierarchy(tables), defaults = hierarchy.positions
  const saved = raw?.version === 1 ? raw : null
  const retained = tables.filter(table => Number.isFinite(saved?.positions?.[table.id]?.x) && Number.isFinite(saved?.positions?.[table.id]?.y))
  const retainedIds = new Set(retained.map(table => table.id))
  const newcomers = retained.length ? tables.filter(table => !retainedIds.has(table.id)) : []
  const newPositions = appendedPositions(newcomers)
  const appendY = retained.reduce((max, table) => Math.max(max, saved.positions[table.id].y + CARD_HEIGHT + 52), 0)
  const positions = Object.fromEntries(tables.map(table => {
    const point = saved?.positions?.[table.id], fallback = newPositions[table.id]
      ? { x: newPositions[table.id].x, y: newPositions[table.id].y + appendY } : defaults[table.id]
    return [table.id, { x: bounded(point?.x, fallback.x, -100000, 100000), y: bounded(point?.y, fallback.y, -100000, 100000) }]
  }))
  const ids = new Set(tables.map(table => table.id))
  const expanded = Array.isArray(saved?.expanded) ? [...new Set(saved.expanded.filter(id => ids.has(id)))] : []
  const preset = !saved || saved.preset === HIERARCHY_PRESET ? HIERARCHY_PRESET : null
  return { version: 1, positions, expanded, preset,
    regions: regionsForPositions(tables, positions),
    autoFrame: !saved || saved.autoFrame === true, view: {
    x: bounded(saved?.view?.x, DEFAULT_VIEW.x, -200000, 200000),
    y: bounded(saved?.view?.y, DEFAULT_VIEW.y, -200000, 200000),
    zoom: bounded(saved?.view?.zoom, DEFAULT_VIEW.zoom, MIN_ZOOM, MAX_ZOOM),
  } }
}

export function zoomAt(view, zoom, point) {
  const next = bounded(zoom, view.zoom, MIN_ZOOM, MAX_ZOOM)
  return { x: point.x - (point.x - view.x) * next / view.zoom,
    y: point.y - (point.y - view.y) * next / view.zoom, zoom: next }
}

export function centerTable(point, size, zoom = 1) {
  return { x: size.width / 2 - (point.x + CARD_WIDTH / 2) * zoom,
    y: size.height / 2 - (point.y + CARD_HEIGHT / 2) * zoom, zoom }
}

export function tableBounds(positions, expanded = []) {
  const open = new Set(expanded)
  if (!Object.keys(positions).length) return { left: 0, top: 0, right: CARD_WIDTH, bottom: HEADER_HEIGHT }
  return Object.entries(positions).reduce((b, [id, p]) => ({ left: Math.min(b.left, p.x), top: Math.min(b.top, p.y),
    right: Math.max(b.right, p.x + CARD_WIDTH), bottom: Math.max(b.bottom, p.y + (open.has(id) ? CARD_HEIGHT : HEADER_HEIGHT)) }),
  { left: Infinity, top: Infinity, right: -Infinity, bottom: -Infinity })
}

// Keep the table coordinates (and saved arrangements) unchanged. The desktop adds
// three original-layout spans of empty space on EACH side, including negative x/y.
export function workspaceBounds(originalPositions, positions, expanded = []) {
  const original = tableBounds(originalPositions), current = tableBounds(positions, expanded)
  const padX = Math.max(1000, original.right - original.left) * WORKSPACE_MARGIN
  const padY = Math.max(600, original.bottom - original.top) * WORKSPACE_MARGIN
  const left = Math.min(original.left - padX, current.left - CARD_WIDTH)
  const top = Math.min(original.top - padY, current.top - CARD_HEIGHT)
  const right = Math.max(original.right + padX, current.right + CARD_WIDTH)
  const bottom = Math.max(original.bottom + padY, current.bottom + CARD_HEIGHT)
  return { left, top, right, bottom, width: right - left, height: bottom - top }
}

export function fitBounds(bounds, size) {
  const zoom = bounded(Math.min((size.width - 48) / (bounds.right - bounds.left),
    (size.height - 48) / (bounds.bottom - bounds.top)), 1, MIN_ZOOM, 1)
  return { x: (size.width - (bounds.right + bounds.left) * zoom) / 2,
    y: (size.height - (bounds.bottom + bounds.top) * zoom) / 2, zoom }
}

export function fitTables(positions, size, expanded = []) {
  return Object.keys(positions).length ? fitBounds(tableBounds(positions, expanded), size) : DEFAULT_VIEW
}

export function inViewport(point, view, size, expanded = true) {
  const x = point.x * view.zoom + view.x, y = point.y * view.zoom + view.y
  return x < size.width + 120 && y < size.height + 120
    && x + CARD_WIDTH * view.zoom > -120 && y + (expanded ? CARD_HEIGHT : HEADER_HEIGHT) * view.zoom > -120
}

export function relatedTables(selected, relationships) {
  return new Set(relationships.filter(r => r.source_table === selected || r.target_table === selected)
    .flatMap(r => [r.source_table, r.target_table]))
}

export function relationshipPath(source, target, self = false) {
  if (self) return `M ${source.x + CARD_WIDTH} ${source.y + 30} c 80 -70 80 150 0 90`
  const forward = target.x >= source.x + CARD_WIDTH / 2
  const x1 = source.x + (forward ? CARD_WIDTH : 0), x2 = target.x + (forward ? 0 : CARD_WIDTH)
  const y1 = source.y + 30, y2 = target.y + 30
  const bend = Math.max(48, Math.abs(x2 - x1) / 2) * (forward ? 1 : -1)
  return `M ${x1} ${y1} C ${x1 + bend} ${y1}, ${x2 - bend} ${y2}, ${x2} ${y2}`
}

export function searchTables(tables, query) {
  const term = query.trim().toLowerCase()
  if (!term) return []
  return tables.filter(t => t.id.toLowerCase().includes(term) || t.columns.some(c => c.name.toLowerCase().includes(term)))
}
