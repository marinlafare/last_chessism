import React from 'react'
import { createRoot } from 'react-dom/client'
import CudaSummaryPanel from '../../src/pages/positions/CudaSummaryPanel'
history.replaceState(null, '', '/analize_positions')
const state = window.cudaFixture = { posts: [], unexpected: [], failed: false, active: null, latest: null,
  scores: false, pending_score_players: 0, pending_games: { blunders: 20, accuracy: 25, score_distribution: 25 } }
window.fetch = async (input, options = {}) => {
  const path = new URL(input, location.href).pathname
  if (!path.includes('/analysis/cuda-summaries')) {
    state.unexpected.push(path); throw Error('Unexpected request: ' + path)
  }
  if (options.method === 'POST') {
    state.posts.push(path)
    state.active = { id: 'fixture', status: 'running', phase: 'Calculating blunders on RTX 4060',
      total_games: 20, processed_games: 5, superseded_games: 0, created_at: new Date().toISOString(), timings: {} }
  }
  if (state.failed && state.active) {
    state.latest = { ...state.active, status: 'failed', phase: 'Failed — retry required', error: 'Synthetic interruption' }
    state.active = null; state.failed = false
  }
  const payload = { manual: true, available_metrics: state.scores ? ['blunders','score_distribution'] : ['blunders'],
    deferred_metrics: state.scores ? ['accuracy'] : ['accuracy','score_distribution'], pending_score_players: state.pending_score_players,
    pending_players: 3, pending_games: state.pending_games, preparing_games: 0,
    active: state.active, latest: state.latest, history: state.latest ? [state.latest] : [] }
  if (state.metadata) Object.assign(payload,{available_metrics:['blunders','score_distribution','accuracy','behavioral','unexpected_endings'],
    deferred_metrics:[],pending_score_players:0,pending_games:{},pending_behavioral_players:2,pending_unexpected_endings_players:1})
  if (state.salience) Object.assign(payload,{available_metrics:['blunders','score_distribution','accuracy','salience'],
    deferred_metrics:[],pending_score_players:0,pending_games:{},pending_behavioral_players:0,pending_unexpected_endings_players:0,pending_salience_players:51})
  return new Response(JSON.stringify(payload), { headers: { 'Content-Type': 'application/json' } })
}
createRoot(document.getElementById('root')).render(<CudaSummaryPanel />)
