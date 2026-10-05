import { deleteJson, getJson, postJson } from '../../../services/apiClient'

const base = '/research/algorithms'
export const fetchAlgorithmCatalog = (options) => getJson(`${base}/catalog`, options)
export const fetchInputCatalog = (options) => getJson('/research/matrices/catalog', options)
export const fetchInputs = (offset = 0, options) => getJson(`/research/matrices?limit=100&offset=${offset}`, options)
export const fetchDefinitions = (offset = 0, options) => getJson(`${base}/definitions?limit=100&offset=${offset}`, options)
export const fetchRuns = (offset = 0, options) => getJson(`${base}/runs?limit=30&offset=${offset}`, options)
export const fetchRun = (id, options) => getJson(`${base}/runs/${encodeURIComponent(id)}`, options)
export const preflightAlgorithm = (form) => postJson(`${base}/preflight`, form)
export const saveAlgorithm = (form) => postJson(`${base}/definitions`, form)
export const deleteAlgorithm = (id) => deleteJson(`${base}/definitions/${encodeURIComponent(id)}`)
export const runAlgorithm = (id) => postJson(`${base}/definitions/${encodeURIComponent(id)}/runs`, {})
export const cancelAlgorithm = (id) => postJson(`${base}/runs/${encodeURIComponent(id)}/cancel`, {})
export const validateAlgorithm = (form) => postJson(`${base}/validate`, form)
export const sampleAlgorithm = (form) => postJson(`${base}/sample`, form)
