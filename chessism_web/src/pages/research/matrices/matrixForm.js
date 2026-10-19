// Pure form transformations; saved definitions are never mutated by the editor.
export const defaultFeatures = (rowType) => (rowType?.columns || [])
  .filter((column) => column.default).map((column) => column.key)

export const createMatrixForm = () => ({
  name: 'Custom game matrix', rowTypeKey: 'game', features: [], labels: [],
})

export const matrixFormPayload = ({ name, rowTypeKey, features, labels }) => ({
  name, definition_kind: 'template', row_type: rowTypeKey, feature_columns: [...features], label_columns: [...labels],
  ...(rowTypeKey === 'move_scores' ? { move_scores: {
    all_moves: true, first_move: 1, last_move: null, score_perspective: 'player',
  } } : {}),
})

export const formFromDefinition = ({ name, config }) => ({
  name: `${name} copy`, rowTypeKey: config.row_type,
  features: config.row_type === 'move_scores' ? ['player', 'game', 'result'] : [...config.feature_columns],
  labels: config.row_type === 'move_scores' ? [] : [...config.label_columns],
})

export const moveScoreColumns = (options, maximum = 500) => {
  if (options.all_moves) return ['player', 'game', 'result']
  const first = Number(options.first_move), last = Number(options.last_move)
  if (!Number.isInteger(first) || !Number.isInteger(last) || first < 1 || last < first || last > maximum) return []
  return ['player', 'game', 'result', ...Array.from({ length: last - first + 1 }, (_, i) => `move_score_${first + i}`)]
}

const toggle = (items, value) => items.includes(value)
  ? items.filter((item) => item !== value) : [...items, value]

export const toggleFormMode = (form, mode) => ({
  ...form, filters: { ...form.filters, modes: toggle(form.filters.modes, mode) },
})

export const toggleFormColumn = (form, key, role) => {
  const selected = role === 'feature' ? 'features' : 'labels'
  const other = role === 'feature' ? 'labels' : 'features'
  return { ...form, [selected]: toggle(form[selected], key), [other]: form[other].filter((item) => item !== key) }
}

export const mergeDefinitions = (current, incoming) => {
  const seen = new Set()
  return [...current, ...incoming].filter((item) => {
    if (seen.has(item.id)) return false
    seen.add(item.id)
    return true
  })
}
