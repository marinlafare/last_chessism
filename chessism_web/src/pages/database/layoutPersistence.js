// Personal layouts are opt-in. Neither reading a saved layout nor opening the
// workspace may replace the system default or write browser storage.
export const legacyLayoutStorageKey = accountId => `chessism:database-workspace:v1:${encodeURIComponent(accountId)}`
export const savedLayoutStorageKey = accountId => `chessism:database-saved-layout:v1:${encodeURIComponent(accountId)}`

function validLayout(layout) {
  return layout?.version === 1 && layout.positions && typeof layout.positions === 'object' && !Array.isArray(layout.positions)
}

export function readSavedLayout(storage, accountId) {
  const saved = storage.getItem(savedLayoutStorageKey(accountId))
  if (saved !== null) {
    const document = JSON.parse(saved)
    if (document?.version !== 1 || !validLayout(document.layout)) throw Error('Invalid saved layout')
    return document.layout
  }
  // Preserve pre-feature auto-saved arrangements as a recoverable personal
  // layout. Do not apply, migrate, overwrite or delete that older entry.
  const previous = storage.getItem(legacyLayoutStorageKey(accountId))
  if (previous === null) return null
  const layout = JSON.parse(previous)
  if (!validLayout(layout)) throw Error('Invalid previous layout')
  return layout
}

export function writeSavedLayout(storage, accountId, layout) {
  if (!validLayout(layout)) throw Error('Invalid layout')
  const document = { version: 1, layout: { ...layout, autoFrame: false } }
  storage.setItem(savedLayoutStorageKey(accountId), JSON.stringify(document))
  return document.layout
}
