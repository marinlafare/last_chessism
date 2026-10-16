import { getJson, postJson } from '../../services/apiClient'

export const previewCoefficientDataset = (config) => (
  postJson('/research/coefficient/overview', config)
)

export const fetchCoefficientExperiments = () => (
  getJson('/research/coefficient/experiments?limit=20')
)

export const fetchCoefficientExperiment = (experimentId) => (
  getJson(`/research/coefficient/experiments/${encodeURIComponent(experimentId)}`)
)

export const startCoefficientExperiment = (config) => (
  postJson('/research/coefficient/experiments', config)
)

export const recordCoefficientDecision = (experimentId, decision) => (
  postJson(
    `/research/coefficient/experiments/${encodeURIComponent(experimentId)}/decision`,
    decision
  )
)
