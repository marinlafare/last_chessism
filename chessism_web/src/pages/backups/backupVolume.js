export const formatBytes = value => {
  if (value === null || value === undefined || value === '') return 'Unknown'
  const bytes = Number(value)
  if (!Number.isFinite(bytes) || bytes < 0) return 'Unknown'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let amount = bytes, unit = 0
  while (amount >= 1000 && unit < units.length - 1) { amount /= 1000; unit += 1 }
  return `${amount.toFixed(unit < 2 ? 0 : 2)} ${units[unit]}`
}

const bytes = value => value !== null && value !== undefined && value !== ''
  && Number.isFinite(Number(value)) && Number(value) >= 0 ? Number(value) : null

export function backupVolumeLayout(snapshot) {
  const total = bytes(snapshot?.total_bytes), used = bytes(snapshot?.used_bytes)
  const free = bytes(snapshot?.free_bytes)
  if (!total || used === null || free === null || !Array.isArray(snapshot?.allocations)) return null
  const rows = snapshot.allocations.map(item => {
    const current = bytes(item.used_bytes), reserved = bytes(item.reserved_bytes)
    return { ...item, used: current, reserved, size: Math.max(current || 0, reserved || 0),
      kind: item.is_application ? 'application' : 'backup' }
  })
  const knownUsed = rows.reduce((sum, row) => sum + (row.used || 0), 0)
  // Unassigned physical blocks include filesystem metadata and unmeasured files.
  // Filesystem-reserved blocks are not free to the application either.
  const otherUsed = Math.max(0, used - knownUsed) + Math.max(0, total - used - free)
  if (otherUsed) rows.push({ id: '__system', label: 'Other / filesystem', kind: 'other', size: otherUsed, used: otherUsed })
  const requestedReserve = bytes(snapshot.free_floor_bytes) || 0
  const reserve = Math.min(requestedReserve, free)
  if (reserve) rows.push({ id: '__reserve', label: 'Protected free reserve', kind: 'reserve', size: reserve })
  const assigned = rows.reduce((sum, row) => sum + row.size, 0)
  const overcommitted = Math.max(0, assigned - total)
  if (assigned < total) rows.push({ id: '__free', label: 'Unallocated', kind: 'free', size: total - assigned })
  return { total, free, rows: rows.map(row => ({ ...row, percent: row.size / total * 100,
    fillPercent: row.size > 0 ? Math.min(100, (row.used || 0) / row.size * 100) : 0 })),
  overcommitted, reserveShortfall: Math.max(0, requestedReserve - free) }
}

export function allocationDescription(row) {
  if (row.reserved) return `${row.label}: ${formatBytes(row.used)} used of ${formatBytes(row.reserved)} allocated${row.used > row.reserved ? ' — over allocation' : ''}`
  if (row.kind === 'backup' || row.kind === 'application') return `${row.label}: ${formatBytes(row.used)} used; no published reservation`
  return `${row.label}: ${formatBytes(row.size)}`
}
