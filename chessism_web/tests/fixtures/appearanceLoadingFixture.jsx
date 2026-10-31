import React from 'react'
import { createRoot } from 'react-dom/client'
import AppearanceProvider, { AppearanceControls } from '/src/appearance/AppearanceProvider.jsx'
import { styleTarget } from '/src/appearance/styleCore.js'

const registry = { version: 1, properties: {
  background: { kind: 'color', css: 'background-color', label: 'Background' },
  radius: { kind: 'number', css: 'border-radius', label: 'Corner radius', min: 0, max: 40, unit: 'px' },
}, scopes: { home: { label: 'Home', targets: { page: { label: 'Page', properties: ['background', 'radius'] } } } } }
const key = 'appearance-loading-fixture'
const initial = {
  revision: 1, default_preset_id: 'official', active_preset_id: 'official',
  presets: [{ id: 'official', name: 'Default' }, { id: 'original', name: 'test_2' }],
  overrides: [{ scope: 'home', target: 'page', property: 'background', value: '#123456' }],
}
const state = window.loadingAppearanceTest = { requests: [], pending: [], fail: false, hold: true,
  data: JSON.parse(localStorage.getItem(key) || 'null') || initial,
  rows: { official: initial.overrides, original: [] }, frames: [] }
state.rows[state.data.active_preset_id] = state.data.overrides
window.fetch = async (input, options = {}) => {
  const path = new URL(input, location.href).pathname, method = options.method || 'GET'
  if (!path.includes('/appearance')) throw Error('No live API access: ' + path)
  state.requests.push({ path, method })
  const reply = (data, status = 200) => new Response(JSON.stringify(data), { status, headers: { 'Content-Type': 'application/json' } })
  if (method === 'GET') {
    const data = structuredClone({ ...state.data, registry })
    if (state.hold) return new Promise(resolve => state.pending.push({ data, finish: (failed = false) => resolve(reply(failed ? { detail: 'Temporary API outage' } : data, failed ? 503 : 200)) }))
    if (state.fail) return reply({ detail: 'Temporary API outage' }, 503)
    return reply(data)
  }
  const payload = JSON.parse(options.body)
  if (payload.revision !== state.data.revision) return reply({ detail: 'Stale revision' }, 409)
  if (path.endsWith('/activate')) {
    state.data.active_preset_id = payload.preset_id || state.data.default_preset_id
    state.data.overrides = state.rows[state.data.active_preset_id]
  } else if (path.endsWith('/save')) {
    if (payload.preset_id !== 'official') throw Error('Test saves should update Default, not create another preset')
    state.data.overrides = payload.overrides
    state.rows.official = payload.overrides
  } else throw Error('Unexpected action: ' + path)
  state.data.revision++
  localStorage.setItem(key, JSON.stringify(state.data))
  return reply({ ...state.data, registry })
}
const sample = () => {
  const page = document.getElementById('themed-page')
  if (page) state.frames.push(getComputedStyle(page).backgroundColor)
  requestAnimationFrame(sample)
}
requestAnimationFrame(sample)
createRoot(document.getElementById('root')).render(<AppearanceProvider account={{ id: 'fixture-user', role: 'admin' }} pageKey="/">
  <AppearanceControls />
  <main {...styleTarget('page', 'home')} id="themed-page" style={{ minHeight: 200, padding: 20 }}>Themed page</main>
</AppearanceProvider>)
