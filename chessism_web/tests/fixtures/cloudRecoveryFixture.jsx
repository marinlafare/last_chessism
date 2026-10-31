import React from 'react'
import { createRoot } from 'react-dom/client'
import SystemStatus from '/src/pages/SystemStatus.jsx'
import AppearanceProvider, { AppearanceControls } from '/src/appearance/AppearanceProvider.jsx'
import CloudRecoveryHistory from '/src/pages/positions/CloudRecoveryHistory.jsx'
import '/src/pages/positions/analysisHistory.css'
import '/src/index.css'

history.replaceState(null, '', '/system_status')
const state = window.cloudRecoveryTest = {
  requests: [], errors: [], online: true, failStatus: false, resumeStatus: 200,
  holdResume: false, holdRefresh: false, recoveryRequested: false,
  job: { id: 'synthetic-job', status: 'paused', selection: { backend: 'batch_spot', n_vms: 6 },
    target: 5819577, imported: 3727000, runs: [{ status: 'running' }], error: 'Synthetic Google Storage timeout' },
}
window.addEventListener('error', event => state.errors.push(event.message))
window.addEventListener('unhandledrejection', event => state.errors.push(String(event.reason)))
const reply = (value, status = 200) => new Response(JSON.stringify(value), { status, headers: { 'Content-Type': 'application/json' } })
const cloudStatus = () => ({ controller_online: state.online, cloud_busy: state.job.status !== 'complete',
  blocking_job_id: state.job.status !== 'complete' ? state.job.id : null, jobs: [state.job] })
window.fetch = async (input, options = {}) => {
  const path = new URL(input, location.href).pathname.replace(/^\/api/, '')
  const method = options.method || 'GET'
  state.requests.push({ path, method })
  if (method === 'POST' && path === '/analysis/cloud/synthetic-job/resume') {
    if (state.holdResume) await new Promise(resolve => { state.finishResume = resolve })
    if (state.resumeStatus !== 200) return reply({ detail: 'Synthetic recovery rejected' }, state.resumeStatus)
    state.job = { ...state.job, status: 'queued', error: null }
    state.recoveryRequested = true
    return reply({ id: state.job.id, status: 'queued' })
  }
  if (method !== 'GET') throw Error('Forbidden mutation in isolated test: ' + path)
  if (path === '/appearance') return reply({ revision: 0, presets: [], active_preset_id: null, overrides: [], registry: window.cloudRecoveryRegistry })
  if (path === '/status') return reply({ api: { ok: true }, workers: { total: 1, busy: 0, idle: 1 }, jobs: { queued: 0, running: 0, failed: 0, completed: 1 } })
  if (path === '/analysis/cloud') {
    if (state.recoveryRequested && state.holdRefresh) await new Promise(resolve => { state.finishRefresh = resolve })
    if (state.failStatus) return reply({ detail: 'Synthetic status unavailable' }, 503)
    return reply(cloudStatus())
  }
  if (path === '/analysis/cloud/synthetic-job/usage') return reply({ job_id: state.job.id, phases: [], usage: [], attempts: [], imports: {} })
  throw Error('No live API access: ' + path)
}
const root = createRoot(document.getElementById('root'))
let revision = 0
state.renderPage = () => root.render(<AppearanceProvider account={{ role: 'admin' }} pageKey="/system_status">
  <AppearanceControls /><SystemStatus key={++revision} />
  <CloudRecoveryHistory recovery={{ total: 3, counts: { retry: 1, paused: 1, resumed: 1 }, paused_seconds: 125,
    note: 'Older pauses cannot be reconstructed.', events: [
      { id: 1, kind: 'retry', created_at: '2026-10-23T01:00:00Z', operation: 'storage read', error_type: 'TimeoutError', attempt: 1, delay_seconds: 5 },
      { id: 2, kind: 'paused', created_at: '2026-10-23T01:01:00Z', operation: 'local workflow', message: 'Synthetic failure retained' },
      { id: 3, kind: 'resumed', created_at: '2026-10-23T01:03:05Z', operation: 'manual recovery', elapsed_seconds: 125 },
    ] }} />
</AppearanceProvider>)
state.renderPage()
