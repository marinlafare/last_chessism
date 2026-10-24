// Old player bookmarks now open the same homepage workspace.
export const normalizePath = (pathname) => {
  const path = pathname.replace(/\/+$/, '') || '/'
  return path === '/players' ? '/' : path
}

export const normalizePlayer = (name) => String(name || '').trim().toLowerCase()
export const playerHomeHref = (name) => {
  const player = normalizePlayer(name)
  return player ? `/?player=${encodeURIComponent(player)}` : '/'
}
export const playerFromLocation = (locationKey) => (
  normalizePlayer(new URLSearchParams(locationKey.split('?')[1] || '').get('player'))
)

export const isPlainClick = (event) => !event.defaultPrevented && event.button === 0
  && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey
