import { deleteJson, getJson, postJson } from '../../../services/apiClient'

export const fetchMatrixCatalog = (options = {}) => getJson('/research/matrices/catalog', options)

export const fetchDefaultMatrices = (options = {}) => getJson('/research/matrices/defaults', options)

export const estimateMatrix = (config, options = {}) => postJson('/research/matrices/estimate', config, options)

export const saveMatrixDefinition = (config) => postJson('/research/matrices', config)

export const fetchMatrixDefinitions = ({ offset = 0, collection = 'all', signal } = {}) => getJson(`/research/matrices?limit=100&offset=${offset}&collection=${encodeURIComponent(collection)}`, { signal })

export const fetchMatrixDefinition = (id, options = {}) => getJson(`/research/matrices/definitions/${encodeURIComponent(id)}`, options)

export const deleteMatrixDefinition = (id) => deleteJson(`/research/matrices/definitions/${encodeURIComponent(id)}`)

// Formula-browser previews illustrate the formula; they never configure a future run
// or mutate its saved instructions. The server bounds the sample to <=100 rows.
const formulaSampleParameters = (definition) => ({
  filters: { players: [], modes: [], max_rows: null, analyzed_only: definition.row_type === 'move_scores' },
  ...(definition.row_type === 'move_scores' ? {
    move_scores: { all_moves: true, score_perspective: definition.config.move_scores?.score_perspective || 'player' },
  } : {}),
})

export const fetchLiveMatrixPreview = (definition, { limit = 50, role = 'all', signal } = {}) => {
  const params = new URLSearchParams({ limit, role })
  if (definition.config?.definition_kind === 'template' || definition.runtime_parameters) {
    if (!definition.id) {
      return Promise.reject(new Error('Save this formula before opening its preview.'))
    }
    const path = definition.builtin_key
      ? `/research/matrices/defaults/${encodeURIComponent(definition.builtin_key)}/preview`
      : `/research/matrices/definitions/${encodeURIComponent(definition.id)}/preview`
    return postJson(`${path}?${params}`, definition.runtime_parameters || formulaSampleParameters(definition), { signal })
  }
  return definition.id
    ? getJson(`/research/matrices/definitions/${encodeURIComponent(definition.id)}/preview?${params}`, { signal })
    : postJson(`/research/matrices/preview?${params}`, definition.config, { signal })
}

export const fetchMatrixPreview = (artifactId, { offset = 0, limit = 50, role = 'all', sliceIndex = 0, signal } = {}) => {
  const params = new URLSearchParams({ offset, limit, role, slice_index: sliceIndex })
  return getJson(`/research/matrices/snapshots/${encodeURIComponent(artifactId)}/preview?${params}`, { signal })
}
