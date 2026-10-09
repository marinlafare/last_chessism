import { deleteJson, getJson, postJson } from '../../../services/apiClient'

export const fetchMatrixCatalog = (options = {}) => getJson('/research/matrices/catalog', options)

export const estimateMatrix = (config, options = {}) => postJson('/research/matrices/estimate', config, options)

export const saveMatrixDefinition = (config) => postJson('/research/matrices', config)

export const fetchMatrixDefinitions = ({ offset = 0, signal } = {}) => getJson(`/research/matrices?limit=100&offset=${offset}`, { signal })

export const fetchMatrixArtifacts = ({ signal } = {}) => getJson('/research/matrices/snapshots?limit=100', { signal })

export const deleteMatrixDefinition = (id) => deleteJson(`/research/matrices/definitions/${encodeURIComponent(id)}`)

export const fetchLiveMatrixPreview = (definition, { limit = 50, role = 'all', signal } = {}) => {
  const params = new URLSearchParams({ limit, role })
  return definition.id
    ? getJson(`/research/matrices/definitions/${encodeURIComponent(definition.id)}/preview?${params}`, { signal })
    : postJson(`/research/matrices/preview?${params}`, definition.config, { signal })
}

export const fetchMatrixPreview = (artifactId, { offset = 0, limit = 50, role = 'all', sliceIndex = 0, signal } = {}) => {
  const params = new URLSearchParams({ offset, limit, role, slice_index: sliceIndex })
  return getJson(`/research/matrices/snapshots/${encodeURIComponent(artifactId)}/preview?${params}`, { signal })
}

export const deleteMatrixArtifact = (artifactId) => (
  deleteJson(`/research/matrices/snapshots/${encodeURIComponent(artifactId)}`)
)
