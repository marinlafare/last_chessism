import { useEffect, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { gameAxisMax, gameBarLabel, gamePlayerPage, GAME_PLAYERS_PER_PAGE } from './summaryData.js'
import BarPlayerAvatar from './BarPlayerAvatar.jsx'
import useGameCoverage from './useGameCoverage.js'
import { DEFAULT_PLAYER_AVATAR_NOTE } from '../../components/players/usePlayerAvatar'

const tickNumber = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 })
const fullNumber = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 })
const LEFT = 54, RIGHT = 350, TOP = 80, BOTTOM = 235

export default function SummaryGamesChart({ players, ready, onSelectPlayer, titledOnly = true }) {
  const [requestedPage, setPage] = useState(0)
  const [tip, setTip] = useState(null)
  const data = gamePlayerPage(players, requestedPage)
  const coverage = useGameCoverage(data.players)
  const axisMax = gameAxisMax(data.maxGames)
  const slot = (RIGHT - LEFT) / GAME_PLAYERS_PER_PAGE
  useEffect(() => { setPage(page => Math.min(page, data.pages - 1)); setTip(null) }, [data.pages])
  const changePage = direction => { setTip(null); setPage(data.page + direction) }
  const show = (event, player) => {
    const rect = event.currentTarget.getBoundingClientRect()
    const entry = coverage[player.player_name]
    const scored = event.target.closest?.('[data-games-series="scored"]')
    const defaultPhoto = event.target.closest?.('[data-avatar-state="fallback"]')
    const text = defaultPhoto ? DEFAULT_PLAYER_AVATAR_NOTE : scored && Number.isFinite(entry?.scoredGames)
      ? `${player.player_name}\nscored: ${fullNumber.format(entry.scoredGames)}`
      : `${player.player_name}\n${fullNumber.format(player.games)}${entry?.unavailable ? '\nScored-game count unavailable; retrying shortly.' : ''}`
    setTip({ text,
      x: event.clientX || rect.x + rect.width / 2, y: event.clientY || rect.y })
  }
  const selectPlayer = player => { setTip(null); onSelectPlayer?.(player.player_name) }

  return (
    <figure {...pageStyleTarget('summary.games')} className="home-summary-games">
      <div className="home-games-navigation" role="group" aria-label="Browse players by games">
        <button {...pageStyleTarget('summary.games.previous')} type="button" aria-label="Previous four players" disabled={!data.page}
          onClick={() => changePage(-1)}>‹</button>
      <svg className="home-games-chart" viewBox="0 0 360 250" role="group" aria-label={`Games by ${titledOnly ? 'titled' : 'untitled'} player; overlaid bars show fully scored games`}>
        {[0, 1, 2, 3, 4].map(index => {
          const value = axisMax * index / 4
          const y = BOTTOM - index * (BOTTOM - TOP) / 4
          return <g key={index} className="home-games-tick">
            <line x1={LEFT} x2={RIGHT} y1={y} y2={y} />
            <text x={LEFT - 7} y={y + 4} textAnchor="end">{tickNumber.format(value)}</text>
          </g>
        })}
        {data.players.map((player, index) => {
          const x = LEFT + slot * (index + .5)
          const height = player.games / axisMax * (BOTTOM - TOP)
          const entry = coverage[player.player_name]
          const scoredGames = entry?.scoredGames
          const knownCoverage = Number.isFinite(scoredGames)
          // Both bars share zero and the same scale. Keep the overlay inside
          // the total bar while an older homepage snapshot catches up.
          const scoredHeight = knownCoverage ? Math.min(scoredGames, player.games) / axisMax * (BOTTOM - TOP) : 0
          const coverageLabel = knownCoverage ? `${fullNumber.format(scoredGames)} scored games.`
            : entry?.unavailable ? 'Scored-game count unavailable.' : 'Loading scored-game count.'
          return <g key={player.player_name} className="home-games-player" data-player={player.player_name}
            data-games={player.games} data-scored-games={knownCoverage ? scoredGames : undefined} role="button" tabIndex="0"
            aria-label={`${player.title ? player.title + ' ' : ''}${player.player_name}: ${fullNumber.format(player.games)} games. ${coverageLabel} Open player.`}
            onPointerMove={event => show(event, player)} onPointerLeave={() => setTip(null)}
            onFocus={event => show(event, player)} onBlur={() => setTip(null)}
            onClick={() => selectPlayer(player)} onKeyDown={event => {
              if (event.key === 'Escape') setTip(null)
              if (['Enter', ' '].includes(event.key)) { event.preventDefault(); selectPlayer(player) }
            }}>
            <rect className="home-games-bar" x={x - slot * .29} y={BOTTOM - height} width={slot * .58}
              height={height} />
            {scoredHeight > 0 && <rect className="home-games-scored-bar" data-games-series="scored"
              x={x - slot * .29} y={BOTTOM - scoredHeight} width={slot * .58} height={scoredHeight} />}
            <BarPlayerAvatar avatar={player.avatar} x={x - 20} y={BOTTOM - height - 48} />
            <text className="home-games-count" x={x} y={BOTTOM - height - 56} textAnchor="middle">{gameBarLabel(player.games)}</text>
          </g>
        })}
        {!data.total && <text className="home-games-empty home-summary-empty-label" x="200" y="155" textAnchor="middle">
          {ready ? 'No games yet' : 'Preparing…'}
        </text>}
      </svg>
        <button {...pageStyleTarget('summary.games.next')} type="button" aria-label="Next four players" disabled={data.page + 1 >= data.pages}
          onClick={() => changePage(1)}>›</button>
      </div>
      <figcaption {...pageStyleTarget('label')}>Games</figcaption>
      <div className="home-games-pagination">
        <span aria-live="polite">{data.total ? `${data.start + 1}–${data.start + data.players.length} / ${data.total}` : '0 / 0'}</span>
      </div>
      {tip && <div className="home-summary-tooltip home-summary-bar-tooltip" role="tooltip" style={{
        left: Math.max(8, Math.min(tip.x + 12, window.innerWidth - Math.min(280, window.innerWidth - 16) - 8)),
        top: Math.max(8, Math.min(tip.y + 16, window.innerHeight - 90)),
      }}>{tip.text}</div>}
    </figure>
  )
}
