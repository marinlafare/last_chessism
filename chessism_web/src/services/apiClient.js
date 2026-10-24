import { API_BASE_URL } from '../config'

const apiUrl = (pathOrUrl) => (
  /^https?:\/\//i.test(String(pathOrUrl || ''))
    ? pathOrUrl
    : `${API_BASE_URL}${pathOrUrl}`
)

export async function requestJson(pathOrUrl, options = {}) {
  const response = await fetch(apiUrl(pathOrUrl), {
    credentials: 'include',
    ...options,
    headers: {
      Accept: 'application/json',
      ...options.headers,
    },
  })
  const payload = response.status === 204
    ? {}
    : await response.json().catch(() => ({}))

  if (!response.ok) {
    const error = new Error(payload.detail || payload.message || `HTTP ${response.status}`)
    error.status = response.status
    error.payload = payload
    throw error
  }

  return payload
}

export const getJson = (pathOrUrl, options = {}) => requestJson(pathOrUrl, options)

export const postJson = (pathOrUrl, body, options = {}) => requestJson(pathOrUrl, {
  ...options,
  method: 'POST',
  headers: { 'Content-Type': 'application/json', ...options.headers },
  body: JSON.stringify(body),
})

export const deleteJson = (pathOrUrl, options = {}) => requestJson(pathOrUrl, {
  ...options,
  method: 'DELETE',
})
