import React, { useState } from 'react'
import { createRoot } from 'react-dom/client'
import PlayerAnalysisDisclosure from '/src/pages/players/PlayerAnalysisDisclosure.jsx'
import PlayerGameCatalog from '/src/pages/players/PlayerGameCatalog.jsx'
import PlayerGameExplorerModal from '/src/pages/players/PlayerGameExplorerModal.jsx'
import RatingChartDisclosure from '/src/pages/scored-positions/RatingChartDisclosure.jsx'
import BoardSoundPreferences from '/src/components/chess/BoardSoundPreferences.jsx'
import { appearanceCss } from '/src/appearance/styleCore.js'
import '/src/index.css'
import '/src/pages/scored-positions/scoredPositions.css'
import '/src/pages/players/player-game-explorer.css'

history.replaceState(null, '', '/')
const state = window.catalogRegression = { unexpected: [], selections: [], errors: [], gamePages: [], openedGames: [] }
window.addEventListener('error', event => state.errors.push(event.message))
window.addEventListener('unhandledrejection', event => state.errors.push(String(event.reason)))
window.fetch = async (input, options = {}) => {
  const url = new URL(input, location.href)
  const reply = value => new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } })
  if (url.pathname.endsWith('/analysis/behavioural/catalog')) {
    await new Promise(resolve => setTimeout(resolve, 50))
    return reply({ rows: Array.from({ length: 20 }, (_, index) => ({ key: `sample-${index}`, rank: index + 1,
      games: 100, wins: 50, draws: 20, losses: 30, unknown: 0 })), total_games: 2000,
      pagination: { page: 1, total_groups: 20, has_more: false } })
  }
  if (url.pathname.endsWith('/analysis/measures/explore-games')) {
    const selection = JSON.parse(options.body)
    state.selections.push(selection)
    if (selection.scope === 'result') {
      state.gamePages.push(selection.cursor || null)
      await new Promise(resolve => setTimeout(resolve, 50))
      const start = selection.cursor ? 30 : 0
      return reply({ games: Array.from({ length: start ? 5 : 30 }, (_, offset) => {
        const index = start + offset
        return { game_id: `fixture-game-${index}`, player_color: 'white', opponent_name: `Opponent ${index}`,
          result: 'win', player_rating: 2500,
          played_at_utc: new Date(Date.UTC(2026, 9, 21, 12) - index * 60000).toISOString() }
      }), pagination: { has_more: !start, next_cursor: start ? null : 'fixture-next-page' } })
    }
    return reply({ games: [], pagination: { has_more: false } })
  }
  state.unexpected.push(url.pathname)
  throw Error('No real APIs are contacted by this fixture')
}
const styles = document.createElement('style')
document.head.append(styles)
state.align = alignment => {
  styles.textContent = appearanceCss(window.regressionRegistry, [
    { scope: 'home', target: 'player.compare.toggle', property: 'textAlign', value: alignment },
    { scope: 'scored_positions', target: 'rating.toggle', property: 'textAlign', value: alignment },
  ])
}
const gameSelection = { scope: 'result', result: 'win', modes: ['blitz'], analyzed_only: false }
function ExplorerFixture() {
  const [open, setOpen] = useState(false)
  return <>
    <button id="explore-fixture-games" type="button" onClick={() => setOpen(true)}>Explore fixture games</button>
    {open && <PlayerGameExplorerModal playerName="fixture" selection={gameSelection}
      onClose={() => setOpen(false)} onOpenGame={id => state.openedGames.push(id)} />}
  </>
}
createRoot(document.getElementById('root')).render(<BoardSoundPreferences accountId="keyboard-fixture"><main style={{ margin: '120px 30px 0', minHeight: 2200 }}>
  {['opponents', 'openings'].map(kind => <PlayerAnalysisDisclosure key={kind} label={kind}
    buttonTarget={`player.${kind}.toggle`}>
    <PlayerGameCatalog playerName="fixture" kind={kind} />
  </PlayerAnalysisDisclosure>)}
  <PlayerAnalysisDisclosure label="Compare" buttonTarget="player.compare.toggle"><p>Placeholder</p></PlayerAnalysisDisclosure>
  <RatingChartDisclosure />
  <ExplorerFixture />
</main></BoardSoundPreferences>)
