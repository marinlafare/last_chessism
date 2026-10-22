import React from 'react'
import { createRoot } from 'react-dom/client'
import CloudJobSteps from '/src/pages/positions/CloudJobSteps.jsx'
import AppearanceProvider, { AppearanceControls } from '/src/appearance/AppearanceProvider.jsx'
import '/src/pages/positions/cloudAnalysis.css'

// Isolated browser profile, supplied local registry, no real API requests.
history.replaceState(null, '', '/analize_positions')
const state = window.stepTest = {
  requests: [], errors: [], online: true,
  job: { status: 'running', target: 6000000, imported: 0, runs: [],
    sequence: { automatic: true, phase: 'selecting', requested: 6000000, reserved: 50000 } },
}
window.addEventListener('error', event => state.errors.push(event.message))
window.addEventListener('unhandledrejection', event => state.errors.push(String(event.reason)))
window.fetch = async (input, options = {}) => {
  const url = new URL(input, location.href)
  state.requests.push({ path: url.pathname, method: options.method || 'GET' })
  if (!url.pathname.endsWith('/appearance') || (options.method && options.method !== 'GET')) {
    throw new Error('No live API access in this fixture')
  }
  return new Response(JSON.stringify({ revision: 0, active_preset_id: null, presets: [], overrides: [], registry: window.stepRegistry }), {
    headers: { 'Content-Type': 'application/json' },
  })
}
const root = createRoot(document.getElementById('root'))
state.render = () => root.render(<AppearanceProvider account={{ role: 'admin' }} pageKey="/analize_positions">
  <AppearanceControls />
  <main style={{ padding: '1rem', background: '#10191f', color: '#dfedef' }}>
    <CloudJobSteps job={state.job} controllerOnline={state.online} />
  </main>
</AppearanceProvider>)
state.render()
