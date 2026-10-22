import { useMemo, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { isPlainClick, normalizePlayer, playerHomeHref } from '../../services/navigation.js'
import { PLAYER_MODE_COLORS } from '../players/PlayerModeToggles.jsx'
import useRatingChartSize from './useRatingChartSize.js'
import BarPlayerAvatar from './BarPlayerAvatar.jsx'
import { DEFAULT_PLAYER_AVATAR_NOTE } from '../../components/players/usePlayerAvatar.js'
import { groupedRatingPlayers, overlappingRatingBars, playerDisplayName, ratingPlayerPage, RATING_BAR_MODES, RATING_PLAYERS_PER_PAGE } from './ratingChartData.js'

const LEFT = 62, TOP = 8
const number = new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 })
const modeColor = mode => `var(--chart-color-${{ bullet: 1, blitz: 2, rapid: 3 }[mode]}, ${PLAYER_MODE_COLORS[mode]})`
const modeStyle = mode => pageStyleTarget(`characters.chart.${mode}`)

export default function PlayerRatingsChart({ players, modeData, page, onPageChange, loading, error, selectedPlayer, onSelectPlayer }) {
  const [tip, setTip] = useState(null)
  const { ref, width, height } = useRatingChartSize()
  const right = Math.max(LEFT + 60, width - 10)
  const grouped = useMemo(() => groupedRatingPlayers(players, modeData.modes), [players, modeData.modes])
  const data = useMemo(() => ratingPlayerPage(grouped, page), [grouped, page])
  const slot = (right - LEFT) / RATING_PLAYERS_PER_PAGE
  const avatarSize = Math.min(40, slot * .8, Math.max(16, height * .16))
  // Only the photos and a small gap remain below the bars; the list still
  // defines the available height, never this chart's intrinsic dimensions.
  const bottom = Math.max(24, height - avatarSize - 9)
  const ticks = bottom - TOP < 110 ? [0, .5, 1] : [0, .25, .5, .75, 1]
  const describe = (player, mode) => {
    const value = player.ratings[mode]
    return `${mode[0].toUpperCase() + mode.slice(1)}: ${value == null
      ? (mode !== 'blitz' && modeData.loading ? 'Loading…' : 'Rating unavailable') : number.format(value)}`
  }
  const showTip = (event, player) => {
    const mode = event.target.closest?.('[data-rating-mode]')?.dataset.ratingMode
    const rect = event.currentTarget.getBoundingClientRect()
    const fallbackPhoto = event.target.closest?.('[data-avatar-state="fallback"]')
    const text = mode ? describe(player, mode)
      : [playerDisplayName(player), ...(fallbackPhoto ? [DEFAULT_PLAYER_AVATAR_NOTE] : [])].join('\n')
    setTip({ text,
      x: event.clientX || rect.x + rect.width / 2, y: event.clientY || rect.y })
  }
  const changePage = direction => { setTip(null); onPageChange(data.page + direction) }
  return <figure {...pageStyleTarget('characters.chart')} className="character-ratings-chart" aria-busy={loading || modeData.loading}>
    <div {...pageStyleTarget('characters.chart.legend')} className="character-chart-legend" aria-label="Rating modes">
      {RATING_BAR_MODES.map(mode => <span {...pageStyleTarget(`characters.chart.legend.${mode}`)} key={mode} data-legend-mode={mode}>
        <svg viewBox="0 0 12 12" aria-hidden="true"><rect {...modeStyle(mode)} className="character-chart-swatch"
          x="2" y="2" width="8" height="8" style={{ '--rating-mode-color': modeColor(mode) }} /></svg>{mode}
      </span>)}
    </div>
    <div className="character-chart-navigation">
      <button {...pageStyleTarget('characters.chart.previous')} type="button" aria-label="Previous ten players"
        disabled={!data.page || loading} onClick={() => changePage(-1)}>‹</button>
      <svg ref={ref} viewBox={`0 0 ${width} ${height}`} role="group" aria-label="Player ratings, sorted by blitz rating descending" data-page={data.page}>
        {ticks.map(ratio => {
          const y = bottom - ratio * (bottom - TOP)
          return <g className="character-chart-tick" key={ratio}>
            <line {...pageStyleTarget('characters.chart.grid')} x1={LEFT} x2={right} y1={y} y2={y} />
            <text {...pageStyleTarget('characters.chart.yTicks')} x={LEFT - 8} y={y + 4} textAnchor="end">{number.format(data.axisMax * ratio)}</text>
          </g>
        })}
        <text {...pageStyleTarget('characters.chart.yTitle')} className="character-chart-axis" transform={`translate(16 ${(TOP + bottom) / 2}) rotate(-90)`} textAnchor="middle">Rating</text>
        {data.players.map((player, index) => {
          const x = LEFT + slot * (index + .5), name = player.player_name
          return <a key={name} className="character-chart-player" data-player={name} href={playerHomeHref(name)}
            aria-current={normalizePlayer(name) === normalizePlayer(selectedPlayer) ? 'true' : undefined}
            aria-label={`${playerDisplayName(player)}. ${RATING_BAR_MODES.map(mode => describe(player, mode)).join('. ')}. Open player.`}
            onPointerMove={event => showTip(event, player)} onPointerLeave={() => setTip(null)}
            onFocus={event => showTip(event, player)} onBlur={() => setTip(null)}
            onKeyDown={event => { if (event.key === 'Escape') setTip(null) }}
            onClick={event => {
              if (!onSelectPlayer || !isPlainClick(event)) return
              event.preventDefault(); setTip(null); onSelectPlayer(name)
            }}>
            {overlappingRatingBars(LEFT + slot * index, slot).map(({ mode, x: barX, width: barWidth }) => {
              const rating = player.ratings[mode], barHeight = (rating ?? 0) / data.axisMax * (bottom - TOP)
              return rating == null ? <line {...pageStyleTarget('characters.chart.missing')} key={mode} className="character-chart-missing" data-rating-mode={mode}
                x1={barX} x2={barX + barWidth} y1={bottom - 2} y2={bottom - 2} />
                : <rect {...modeStyle(mode)} key={mode} className="character-chart-bar" data-rating-mode={mode} data-rating={rating}
                  tabIndex="0" role="img" aria-label={describe(player, mode)}
                  x={barX} y={bottom - barHeight} width={barWidth} height={barHeight} style={{ '--rating-mode-color': modeColor(mode) }} />
            })}
            <line {...pageStyleTarget('characters.chart.xTicks')} className="character-chart-player-tick" x1={x} x2={x} y1={bottom} y2={bottom + 4} />
            <BarPlayerAvatar appearanceTarget={pageStyleTarget('characters.chart.photos')} avatar={player.avatar}
              x={x - avatarSize / 2} y={bottom + 7} size={avatarSize} />
          </a>
        })}
        {data.total > 0 && Array.from({ length: data.players.length + 1 }, (_, index) =>
          <line {...pageStyleTarget('characters.chart.dividers')} key={index} className="character-chart-divider" x1={LEFT + slot * index} x2={LEFT + slot * index} y1={TOP} y2={bottom} />)}
        {!data.total && <text {...pageStyleTarget('characters.chart.empty')} x={width / 2} y={(TOP + bottom) / 2} textAnchor="middle" className="character-chart-empty">
          {loading ? 'Loading players…' : error ? 'Rankings unavailable' : 'No players with blitz games'}
        </text>}
      </svg>
      <button {...pageStyleTarget('characters.chart.next')} type="button" aria-label="Next ten players"
        disabled={data.page + 1 >= data.pages || loading} onClick={() => changePage(1)}>›</button>
    </div>
    {modeData.loading && <p {...pageStyleTarget('characters.chart.status')} className="character-chart-status" role="status">Loading bullet and rapid ratings…</p>}
    {!!modeData.failed.length && <p {...pageStyleTarget('characters.chart.status')} className="character-chart-status" role="status">
      {modeData.failed.join(' / ')} ratings unavailable. <button {...pageStyleTarget('characters.chart.retry')} type="button" onClick={modeData.retry}>Retry</button>
    </p>}
    <div {...pageStyleTarget('characters.chart.tooltip')} hidden={!tip} className="home-summary-tooltip home-summary-bar-tooltip" role="tooltip" style={{
      left: Math.max(8, Math.min((tip?.x || 0) + 12, window.innerWidth - Math.min(280, window.innerWidth - 16) - 8)),
      top: Math.max(8, Math.min((tip?.y || 0) + 16, window.innerHeight - 110)),
    }}>{tip?.text}</div>
  </figure>
}
