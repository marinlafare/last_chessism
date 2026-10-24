import React from 'react'
import { createRoot } from 'react-dom/client'
import PlayerBehaviouralPanel from '/src/pages/players/PlayerBehaviouralPanel.jsx'
import PlayerScoreDistribution from '/src/pages/players/PlayerScoreDistribution.jsx'
import PlayerBlunderDistribution from '/src/pages/players/PlayerBlunderDistribution.jsx'
import BoardSoundPreferences from '/src/components/chess/BoardSoundPreferences.jsx'
import '/src/index.css'
import '/src/pages/players/players.css'
import '/src/pages/players/player-analysis.css'
import '/src/pages/players/player-hero-analytics.css'
import '/src/pages/players/player-game-explorer.css'

history.replaceState(null, '', '/')
const state = window.chartLiftTest = { requests: [], unexpected: [] }
const hours = Array.from({ length: 24 }, (_, hour) => ({ hour, games: hour === 23 ? 0 : 180,
  wins: hour === 23 ? 0 : 100, losses: hour === 23 ? 0 : 60, draws: hour === 23 ? 0 : 20 }))
const activity = { hours, weekdays: [{ weekday: 1, label: 'Monday', games: 4140, wins: 2300, losses: 1380, draws: 460 }],
  weekday_hours: hours.map(row => ({ ...row, weekday: 1 })), time_context: { timezone: 'UTC' } }
const scores = [100, 40, 0, 2, 10, 30, 60, 80, 3, 25, 15, 1, 20], blunders = [100, 4, 0, 30, 7, 1, 2]
window.fetch = async (input, options = {}) => {
  const path = new URL(input, location.href).pathname.replace(/^\/api(?=\/)/, ''), method = options.method || 'GET'
  state.requests.push({ path, method, body: options.body ? JSON.parse(options.body) : null })
  const reply = data => new Response(JSON.stringify(data), { headers: { 'Content-Type': 'application/json' } })
  if (method === 'GET' && path.endsWith('/ppd')) return reply({ mode: 'all', x: [2000], y: [100], total_games: 4140 })
  if (method === 'GET' && path.endsWith('/score-distribution')) return reply({ snapshot: 'mock',
    bins: scores.map((_, id) => ({ id, label: String((id - 6) * 100), lower: (id - 6) * 100 - 50, upper: (id - 6) * 100 + 50 })),
    by_mode: { blitz: { counts: scores, endings: { games: 100 }, cp_positions: 487, unscored_positions: 0, mate_positions: 0, tablebase_positions: 0 } } })
  if (method === 'GET' && path.endsWith('/blunder-distribution')) return reply({
    bins: blunders.map((_, id) => ({ id, label: id === 6 ? '6+' : String(id) })),
    by_mode: { blitz: { counts: blunders, games_count: 144 } } })
  if (method === 'POST' && path.endsWith('/explore-games')) {
    if (!window.chartResultFiltersTest) return reply({ games: [], pagination: { has_more: false }, summary: null })
    const selection = JSON.parse(options.body), offset = selection.cursor ? 30 : 0
    // Deliberately ignore abort while held: the UI must reject a stale response.
    if ((state.holdResult && state.holdResult === selection.result) || (state.holdPage && offset)) {
      await new Promise(resolve => { state.release = resolve })
    }
    const games = Array.from({ length: offset ? 5 : 30 }, (_, index) => ({
      game_id: `${selection.result || 'all'}-${offset + index}`, player_color: 'white',
      opponent_name: `Opponent ${offset + index}`, played_at_utc: '2026-10-19T00:00:00Z',
      mode: selection.modes[0], player_rating: 2000,
      result: selection.result || ['win', 'draw', 'loss'][index % 3],
    }))
    return reply({ games, summary: { games: 35, analyzed_games: 0, accuracy: null,
      wins: selection.result === 'win' ? 35 : 0, draws: selection.result === 'draw' ? 35 : 0,
      losses: selection.result === 'loss' ? 35 : 0 },
      pagination: { has_more: !offset, next_cursor: offset ? null : `${selection.result || 'all'}-page-2` } })
  }
  if (method === 'POST' && path.endsWith('/score-distribution/positions')) return reply({ snapshot: 'mock', rows: [], next_cursor: null })
  state.unexpected.push(`${method} ${path}`)
  throw Error('No live access allowed: ' + path)
}
createRoot(document.getElementById('root')).render(<BoardSoundPreferences accountId="chart-lift-fixture"><main style={{ maxWidth: 1200, margin: 'auto', padding: 20 }}>
  <style>{window.chartLiftStyles || ''}</style>
  <PlayerBehaviouralPanel playerName="example" activity={activity} ratings={{ series: [] }} />
  <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, minmax(0, 1fr))', gap: 20, marginTop: 20 }}>
    <PlayerScoreDistribution playerName="example" /><PlayerBlunderDistribution playerName="example" />
  </div>
</main></BoardSoundPreferences>)
