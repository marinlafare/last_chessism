import React from 'react'
import { createRoot } from 'react-dom/client'
import App from '/src/App.jsx'
import '/src/index.css'

history.replaceState(null, '', '/research')
const key = 'research-hover-fixture'
const state = window.researchHoverTest = { requests: [], errors: [] }
window.addEventListener('error', event => state.errors.push(event.message))
window.addEventListener('unhandledrejection', event => state.errors.push(String(event.reason)))
window.prompt = () => 'Research hover test'
window.fetch = async (input, options = {}) => {
  const path = new URL(input, location.href).pathname, method = options.method || 'GET'
  state.requests.push({ path, method })
  const reply = value => new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } })
  if (method === 'GET' && path.endsWith('/auth/me')) return reply({ account: { id: 1, name: 'Test user', role: 'admin' } })
  if (method === 'POST' && path.endsWith('/auth/logout')) return reply({ ok: true })
  let saved = { revision: 0, presets: [], active_preset_id: null, overrides: [],
    ...JSON.parse(localStorage.getItem(key) || 'null'), registry: window.researchHoverRegistry }
  if (method === 'POST' && path.endsWith('/appearance/save')) {
    const payload = JSON.parse(options.body)
    saved = { ...saved, revision: saved.revision + 1, presets: [{ id: 'test', name: payload.name || 'Research hover test' }],
      active_preset_id: 'test', overrides: payload.overrides }
    localStorage.setItem(key, JSON.stringify(saved))
  } else if (method !== 'GET' || !path.endsWith('/appearance')) throw Error('No live API access: ' + path)
  return new Response(JSON.stringify(saved), { headers: { 'Content-Type': 'application/json' } })
}
createRoot(document.getElementById('root')).render(<App />)
