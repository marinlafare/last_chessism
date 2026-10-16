// Draft schemas assist the editor. The backend is authoritative for formula types.
export function inputColumns(matrix, catalog) {
  const rowType = catalog?.row_types?.find((item) => item.key === matrix?.config.row_type)
  const selected = new Set([...(matrix?.config.feature_columns || []), ...(matrix?.config.label_columns || [])])
  return (rowType?.columns || []).filter((column) => selected.has(column.key))
}

export function blankBuilder(matrix, catalog) {
  return {
    name: `${matrix?.name || 'New'} algorithm`, operation: 'pipeline',
    matrix_definition_id: matrix?.id || null, parent_definition_id: null,
    columns: inputColumns(matrix, catalog).slice(0, 12).map((column) => column.key),
    steps: [], outputs: [], missing: 'keep', invalid_values: 'null',
    max_rows: Math.min(matrix?.config.filters.max_rows || 100000, 100000), seed: 42,
  }
}

export function draftSchemas(form, matrix, catalog) {
  const available = Object.fromEntries(inputColumns(matrix, catalog).map((column) => [column.key, column.data_type]))
  const schemas = { input: Object.fromEntries(form.columns.map((key) => [key, available[key] || 'number'])) }
  for (const step of form.steps) {
    let schema = { ...(schemas[step.input] || {}) }
    if (step.op === 'calculate' && step.column) {
      schema[step.column] = /\butc_(day|month|year)\(/.test(step.expression) ? 'category' : 'number'
    } else if (step.op === 'aggregate') {
      schema = {
        ...Object.fromEntries((step.group_by || []).map((key) => [key, schema[key]])),
        ...Object.fromEntries((step.metrics || []).filter((item) => item.name).map((item) => [item.name, 'number'])),
      }
    } else if (step.op === 'correlate') {
      schema = { variable: 'category', ...Object.fromEntries((step.columns || []).map((key) => [key, 'number'])) }
    } else if (step.op === 'select') {
      schema = Object.fromEntries((step.columns || []).map((key) => [key, schema[key]]))
    }
    schemas[step.id] = schema
  }
  return schemas
}

export function nextStepId(steps) {
  let n = 1
  while (steps.some((step) => step.id === `step_${n}`)) n++
  return `step_${n}`
}

export function stepDefaults(op, id, input, schema = {}) {
  const numeric = Object.keys(schema).filter((key) => schema[key] === 'number')
  return {
    id, name: `${op} ${id.replace('step_', '')}`, input, op,
    ...(op === 'filter' ? { expression: numeric.length ? `${numeric[0]} > 10` : 'True' } : {}),
    ...(op === 'calculate' ? { column: `${id}_value`, expression: numeric[0] || '1' } : {}),
    ...(['select', 'transform', 'correlate'].includes(op) ? { columns: op === 'select' ? Object.keys(schema) : numeric } : {}),
    ...(op === 'transform' ? { method: 'standardize' } : {}),
    ...(op === 'sort' ? { column: Object.keys(schema)[0] || '', descending: false } : {}),
    ...(op === 'aggregate' ? { group_by: [], metrics: [{ name: 'rows', op: 'count' }] } : {}),
  }
}

export function relationshipsTemplate(form, schema, legacy = null) {
  const numeric = form.columns.filter((key) => schema[key] === 'number')
  if (numeric.length < 2) throw new Error('The relationships template needs two numeric columns.')
  const steps = []
  let source = 'input'
  if (legacy?.rating_difference) {
    steps.push({ id: 'rating_gap', name: 'Rating difference', input: source, op: 'calculate', column: 'rating_difference', expression: 'opponent_rating - rating' })
    source = 'rating_gap'; numeric.push('rating_difference')
  }
  steps.push({ id: 'summaries', name: 'Column summaries', input: source, op: 'aggregate', group_by: [],
    metrics: numeric.slice(0, 12).flatMap((column, index) => ['mean', 'std', 'min', 'max'].map((op) => ({ name: `c${index + 1}_${op}`, op, column }))) })
  steps.push({ id: 'correlations', name: 'Pearson correlations', input: source, op: 'correlate', columns: numeric.slice(0, 12) })
  let scatterSource = source
  if (legacy?.scaling === 'standardize') {
    steps.push({ id: 'scaled', name: 'Standardize scatter', input: source, op: 'transform', method: 'standardize', columns: numeric })
    scatterSource = 'scaled'
  }
  return { ...form, steps, outputs: [
    { name: 'Column summaries', type: 'table', input: 'summaries', columns: [] },
    { name: 'Pearson correlations', type: 'heatmap', input: 'correlations' },
    { name: 'Feature scatter', type: 'scatter', input: scatterSource, x: legacy?.x || numeric[0], y: legacy?.y || numeric[1] },
  ] }
}

export function durationTemplate(form) {
  if (!['moves', 'elapsed_seconds', 'result'].every((key) => form.columns.includes(key))) {
    throw new Error('Select moves, elapsed_seconds and result for the duration example.')
  }
  return { ...form, name: 'Duration per full move by result', steps: [
    { id: 'long_games', name: 'Keep games over 10 moves', input: 'input', op: 'filter', expression: 'moves > 10' },
    { id: 'pace', name: 'Duration per full move', input: 'long_games', op: 'calculate', column: 'seconds_per_full_move', expression: 'safe_divide(elapsed_seconds, moves)' },
    { id: 'by_result', name: 'Group by result', input: 'pace', op: 'aggregate', group_by: ['result'], metrics: [
      { name: 'games', op: 'count' }, { name: 'mean_seconds', op: 'mean', column: 'seconds_per_full_move' },
    ] },
  ], outputs: [
    { name: 'Duration by result', type: 'table', input: 'by_result', columns: [] },
    { name: 'Mean duration per full move', type: 'bar', input: 'by_result', x: 'result', y: 'mean_seconds' },
    { name: 'Individual games', type: 'scatter', input: 'pace', x: 'moves', y: 'seconds_per_full_move' },
  ] }
}

export function openBuilder(definition, catalog) {
  const matrix = { id: definition.matrix_definition_id, name: 'Frozen input', config: definition.config.matrix, storage_kind: 'definition' }
  let form
  if (definition.config.operation === 'pipeline') {
    form = { ...structuredClone(definition.config), name: definition.name, matrix_definition_id: definition.matrix_definition_id }
    delete form.schemas; delete form.matrix
  } else {
    form = { ...blankBuilder(matrix, catalog), columns: definition.config.columns, missing: definition.config.missing,
      max_rows: Math.min(definition.config.max_rows, 100000), seed: definition.config.seed }
    const schema = Object.fromEntries(inputColumns(matrix, catalog).map((column) => [column.key, column.data_type]))
    form = relationshipsTemplate(form, schema, definition.config)
    form.name = definition.name
  }
  return { form: { ...form, parent_definition_id: definition.id }, matrix }
}
