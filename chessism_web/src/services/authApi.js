import { API_BASE_URL } from '../config'

async function authRequest(path, options = {}) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    credentials: 'include',
    ...options,
  })
  const payload = await response.json().catch(() => ({}))
  return { response, payload }
}

export async function fetchCurrentAccount() {
  const { response, payload } = await authRequest('/auth/me')
  return response.ok ? payload : null
}

export async function unlockSuperadmin(code) {
  const { response, payload } = await authRequest('/auth/gate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ code }),
  })
  return response.ok ? payload : null
}

export async function fetchAdmins() {
  const { response, payload } = await authRequest('/auth/admins')
  if (!response.ok) throw new Error('Gate access required.')
  return payload
}

export async function submitAdminSignup(data) {
  const { response, payload } = await authRequest('/auth/admins/signup', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!response.ok) throw new Error(payload.detail || 'Could not create admin.')
  return payload
}

export async function submitAdminLogin(data) {
  const { response, payload } = await authRequest('/auth/admins/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  })
  if (!response.ok) throw new Error(payload.detail || 'Could not sign in.')
  return payload
}

export async function logoutAdmin() {
  await authRequest('/auth/logout', { method: 'POST' }).catch(() => {})
}
