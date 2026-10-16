// Pure form transformations; saved definitions are never mutated by the editor.
export const defaultFeatures = (rowType) => (rowType?.columns || [])
  .filter((column) => column.default).map((column) => column.key)

export const createMatrixForm = () => ({
  name: 'Player-game research matrix', rowTypeKey: 'game_player', features: [], labels: [],
  filters: {
    players: '', modes: ['bullet', 'blitz', 'rapid'], date_from: '', date_to: '',
    min_moves: 0, analyzed_only: false, max_rows: 100000,
  },
})

export const matrixFormPayload = ({ name, rowTypeKey, features, labels, filters }) => ({
  name, row_type: rowTypeKey, feature_columns: [...features], label_columns: [...labels],
  filters: {
    ...filters, modes: [...filters.modes],
    players: filters.players.split(',').map((player) => player.trim()).filter(Boolean),
    min_moves: Number(filters.min_moves === '' ? 0 : filters.min_moves),
    max_rows: Number(filters.max_rows === '' ? 100000 : filters.max_rows),
    date_from: filters.date_from || null, date_to: filters.date_to || null,
  },
})

export const formFromDefinition = ({ name, config }) => ({
  name: `${name} copy`, rowTypeKey: config.row_type,
  features: [...config.feature_columns], labels: [...config.label_columns],
  filters: {
    ...createMatrixForm().filters, ...config.filters, modes: [...config.filters.modes],
    players: config.filters.players.join(', '),
    date_from: config.filters.date_from || '', date_to: config.filters.date_to || '',
  },
})

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
