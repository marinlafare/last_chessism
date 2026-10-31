import { useId } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { formatNumber } from '../../utils/formatters'
import useSummaryChartWidth from './useSummaryChartWidth'

const LEFT = 64
const TOP = 20
const BOTTOM = 108
const count = value => Number.isFinite(Number(value)) ? Math.max(0, Math.trunc(Number(value))) : 0

export default function PlayerResultsChart({ totals, mode = 'all', onModeChange, onExplore }) {
  const titleId = useId()
  const chart = useSummaryChartWidth()
  const right = Math.max(LEFT + 1, chart.width - 12)
  const slotWidth = (right - LEFT) / 3
  const barWidth = slotWidth * 0.46
  const rows = [
    { key: 'win', label: 'Wins', count: count(totals.wins) },
    { key: 'draw', label: 'Draws', count: count(totals.draws) },
    { key: 'loss', label: 'Losses', count: count(totals.losses) },
  ]
  const maximum = Math.max(1, ...rows.map(row => row.count))
  const step = 10 ** Math.floor(Math.log10(maximum))
  const ceiling = Math.ceil(maximum / step) * step
  const ticks = [...new Set([0, Math.round(ceiling / 2), ceiling])]
  const y = value => BOTTOM - value / ceiling * (BOTTOM - TOP)
  const knownResults = rows.reduce((total, row) => total + row.count, 0)
  const unclassified = Math.max(0, count(totals.games) - knownResults)

  return (
    <div {...pageStyleTarget('chart.results')} className="behavior-results-summary">
      <div className="rating-distribution-heading">
        <span {...pageStyleTarget('label')} id={titleId}>Results</span>
        <select {...pageStyleTarget('field')} aria-label="Results game mode" value={mode} onChange={event => onModeChange?.(event.target.value)}>
          <option value="all">All</option><option value="bullet">Bullet</option>
          <option value="blitz">Blitz</option><option value="rapid">Rapid</option>
        </select>
      </div>
      <svg ref={chart.ref} className="behavior-results-chart" viewBox={`0 0 ${chart.width} 136`} role="group" aria-labelledby={titleId}>
        <desc>{rows.map(row => `${formatNumber(row.count)} ${row.label.toLowerCase()}`).join(', ')}. Recorded game results.</desc>
        {ticks.map(tick => (
          <g key={tick} className="behavior-results-grid" aria-hidden="true">
            <line x1={LEFT} x2={right} y1={y(tick)} y2={y(tick)} />
            <text x={LEFT - 8} y={y(tick) + 4} textAnchor="end">{formatNumber(tick)}</text>
          </g>
        ))}
        {rows.map((row, index) => {
          const center = LEFT + (index + 0.5) * slotWidth
          return (
            <g key={row.key} className="behavior-results-column" tabIndex={row.count ? 0 : -1}
              role="button" aria-disabled={!row.count} aria-label={`View ${formatNumber(row.count)} ${row.label.toLowerCase()} games`}
              onClick={() => { if (row.count) onExplore?.(row.key) }}
              onKeyDown={event => {
                if (row.count && (event.key === 'Enter' || event.key === ' ')) {
                  event.preventDefault()
                  onExplore?.(row.key)
                }
              }}>
              <title>{`${row.label}: ${formatNumber(row.count)}`}</title>
              <rect className={`behavior-bar-${row.key}`} x={center - barWidth / 2}
                y={y(row.count)} width={barWidth} height={BOTTOM - y(row.count)} rx="3" />
              <text className="behavior-results-value" x={center} y={y(row.count) - 8} textAnchor="middle">{formatNumber(row.count)}</text>
              <text x={center} y={BOTTOM + 22} textAnchor="middle">{row.label}</text>
            </g>
          )
        })}
      </svg>
      {unclassified > 0 ? <small {...pageStyleTarget('text')}>{formatNumber(unclassified)} games without a classified result.</small> : null}
    </div>
  )
}
