import React, { useState } from 'react'
import { createRoot } from 'react-dom/client'
import AppearanceProvider, { AppearanceControls } from '/src/appearance/AppearanceProvider.jsx'
import BoardSoundPreferences from '/src/components/chess/BoardSoundPreferences.jsx'
import PlayerWorkspace from '/src/pages/players/PlayerWorkspace.jsx'
import '/src/index.css'

history.replaceState(null, '', '/')
const state = window.playerSummaryTest = { requests: [], errors: [], unexpected: [] }
window.addEventListener('error', event => state.errors.push(event.message))
window.addEventListener('unhandledrejection', event => state.errors.push(String(event.reason)))
const empty = () => ({ games: 0, wins: 0, draws: 0, losses: 0, proportion: 0 })
const cells = Array.from({ length: 168 }, (_, index) => ({ weekday: Math.floor(index / 24) + 1, hour: index % 24, ...empty() }))
;[[0, 10, 5, 5], [1, 5, 3, 2], [2, 20, 16, 4], [3, 10, 4, 5], [4, 10, 1, 9],
  [24, 2, 1, 0], [25, 4, 3, 1]].forEach(([index, games, wins, losses]) => {
  cells[index] = { ...cells[index], games, wins, losses, draws: games - wins - losses }
})
const totals = rows => Object.fromEntries(['games', 'wins', 'draws', 'losses'].map(key => [key, rows.reduce((sum, row) => sum + row[key], 0)]))
const weekdays = Array.from({ length: 7 }, (_, index) => ({ weekday: index + 1, label: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][index], ...totals(cells.slice(index * 24, (index + 1) * 24)) }))
const activity = { total_games: totals(cells).games, weekday_hours: cells, weekdays,
  hours: Array.from({ length: 24 }, (_, hour) => ({ hour, ...totals(cells.filter(cell => cell.hour === hour)) })), time_context: { timezone: 'UTC', label: 'UTC' } }
const emptyActivity = { total_games: 0, weekday_hours: cells.map(cell => ({ ...cell, ...empty() })),
  weekdays: weekdays.map(day => ({ ...day, ...empty() })), hours: activity.hours.map(hour => ({ ...hour, ...empty() })) }
activity.by_mode = { blitz: { ...activity }, bullet: emptyActivity, rapid: emptyActivity }
const appearance = { revision: 0, active_preset_id: null, presets: [], overrides: [], registry: window.playerSummaryRegistry }
window.fetch = async (input, options = {}) => {
  const url = new URL(input, location.href), path = url.pathname
  state.requests.push({ path, method: options.method || 'GET' })
  const reply = (body, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
  if (path.endsWith('/appearance')) return reply(appearance)
  if (path.endsWith('/appearance/save')) {
    appearance.overrides = JSON.parse(options.body).overrides; appearance.revision++
    return reply(appearance)
  }
  if (path.endsWith('/hero')) return reply({ player_name: path.includes('/shell/') ? 'shell' : 'fixture', joined: path.includes('/shell/') ? 0 : 1600000000, country: 'MX', name: 'Fixture player' })
  if (path.endsWith('/hours_played')) return reply({ blitz_games: activity.total_games, blitz_hours: 10 })
  if (path.endsWith('/analysis_counts')) return reply({ total_games: 50, analyzed_games: 20, total_fens: 2000, analyzed_fens: 1000 })
  if (path.endsWith('/neighbors')) return reply({ previous_player: null, next_player: null })
  if (path.endsWith('/latest')) return reply({ played_at: '2026-10-24T12:00:00+00:00' })
  if (path.endsWith('/analysis/behavioural/activity')) return reply(activity)
  if (path.endsWith('/analysis/behavioural/ratings')) {
    await new Promise(resolve => setTimeout(resolve, 100))
    return reply({ series: [{ mode: 'blitz', points: [{ date: '2026-10-24', last_rating: 2200, game_id: 1 }] }] })
  }
  if (path.endsWith('/ppd')) return reply({ x: [2200], y: [100], bin_width: 10, games_count: 50 })
  state.unexpected.push(path)
  throw Error(`Fixture rejects live API access: ${path}`)
}
function Fixture() {
  const [player, setPlayer] = useState('fixture')
  return <AppearanceProvider account={{ role: 'admin' }}><AppearanceControls />
    <button id="choose-shell" onClick={() => setPlayer('shell')}>Open ineligible fixture</button>
    <BoardSoundPreferences accountId="summary-fixture"><PlayerWorkspace key={player} playerName={player} /></BoardSoundPreferences>
  </AppearanceProvider>
}
createRoot(document.getElementById('root')).render(<Fixture />)
