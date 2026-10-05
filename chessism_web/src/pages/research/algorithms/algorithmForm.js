export function numericColumns(matrix, catalog) {
  const type = catalog?.row_types?.find((item) => item.key === matrix?.config.row_type)
  const selected = new Set([...(matrix?.config.feature_columns || []), ...(matrix?.config.label_columns || [])])
  return (type?.columns || []).filter((column) => column.data_type === 'number' && selected.has(column.key))
}

export function newAlgorithmForm(matrix, catalog) {
  const columns = numericColumns(matrix, catalog).slice(0, 12).map((column) => column.key)
  return {
    name: `${matrix?.name || 'Matrix'} relationships`, matrix_definition_id: matrix?.id || '',
    operation: 'feature_relationships', columns, missing: 'drop_rows', scaling: 'none',
    rating_difference: false, x: columns[0] || '', y: columns[1] || '',
    max_rows: Math.min(matrix?.config.filters.max_rows || 100000, 100000), seed: 42,
  }
}

export function updateAlgorithmForm(form, changes) {
  const next = { ...form, ...changes }
  next.rating_difference = next.rating_difference && next.columns.includes('rating') && next.columns.includes('opponent_rating')
  const choices = [...next.columns, ...(next.rating_difference ? ['rating_difference'] : [])]
  if (!choices.includes(next.x)) next.x = choices[0] || ''
  if (!choices.includes(next.y) || next.y === next.x) next.y = choices.find((key) => key !== next.x) || ''
  return next
}

export const activeRun = (run) => ['queued', 'running'].includes(run.status)
export const number = (value) => value == null || !Number.isFinite(value) ? 'N/A' : value.toLocaleString(undefined, { maximumFractionDigits: 3 })
export const bytes = (value) => `${number(value / (1024 * 1024))} MiB`
export function duration(seconds) {
  if (seconds == null || !Number.isFinite(seconds)) return '—'
  const total = Math.max(0, Math.floor(seconds))
  return `${Math.floor(total / 3600)}:${String(Math.floor(total / 60) % 60).padStart(2, '0')}:${String(total % 60).padStart(2, '0')}`
}
