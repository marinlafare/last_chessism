import { deleteJson, getJson, postJson } from '../../services/apiClient'

export const fetchMatrixCatalog = () => getJson('/research/matrices/catalog')

export const estimateMatrix = (config) => postJson('/research/matrices/estimate', config)

export const constructMatrix = (config) => postJson('/research/matrices', config)

export const fetchMatrixArtifacts = () => getJson('/research/matrices?limit=30')

export const deleteMatrixArtifact = (artifactId) => (
  deleteJson(`/research/matrices/${encodeURIComponent(artifactId)}`)
)
