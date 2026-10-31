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

const sortingFixture = new URLSearchParams(location.search).has('sorting')
history.replaceState(null, '', '/')
const state = window.catalogRegression = { unexpected: [], selections: [], errors: [], gamePages: [], openedGames: [], catalogPages: [] }
state.catalogRows = ['wins', 'losses', 'draws'].flatMap((outcome, outcomeIndex) =>
  Array.from({ length: 11 }, (_, rate) => Array.from({ length: 12 }, (_, sample) => {
    const row = { key: `${outcome}-${rate}-${sample}`, games: 100 * (sample + 1), wins: 0, losses: 0, draws: 0 }
    row[outcome] = rate * 10 * (sample + 1)
    row[['losses', 'draws', 'wins'][outcomeIndex]] = row.games - row[outcome]
    return row
  })).flat())
state.catalogRows.push({ key: 'single-game', games: 1, wins: 1, losses: 0, draws: 0 })
window.addEventListener('error', event => state.errors.push(event.message))
window.addEventListener('unhandledrejection', event => state.errors.push(String(event.reason)))
window.fetch = async (input, options = {}) => {
  const url = new URL(input, location.href)
  const reply = value => new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } })
  if (url.pathname.endsWith('/analysis/behavioural/catalog')) {
    if (sortingFixture) {
      const params = Object.fromEntries(url.searchParams)
      state.catalogPages.push(params)
      await new Promise(resolve => setTimeout(resolve, params.sort_by === 'losses' ? 200 : 50))
      const sort = params.sort_by || 'games', page = Number(params.page), limit = Number(params.limit)
      const bin = params.ratio_bin == null ? 9 : Number(params.ratio_bin)
      const ranked = state.catalogRows.filter(row => row.games >= 2 && (sort === 'games' || Math.min(9, Math.floor(10 * row[sort] / row.games)) === bin))
        .sort((a, b) => (sort === 'games' ? 0 : b[sort] / b.games - a[sort] / a.games)
          || b.games - a.games || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0))
        .map((row, index) => ({ ...row, rank: index + 1, unknown: row.games - row.wins - row.losses - row.draws,
          ...(params.kind === 'opponents' ? { latest_ratings: { bullet: 2401, blitz: 2502, rapid: 2603 } } : {}) }))
        .filter(row => row.key.includes(params.search))
      return reply({ sort_by: sort, summary_revision: 1, max_age_seconds: 0,
        rows: ranked.slice((page - 1) * limit, page * limit), total_games: ranked.reduce((sum, row) => sum + row.games, 0),
        pagination: { page, limit, total_groups: ranked.length, has_more: page * limit < ranked.length } })
    }
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
