import { formatNumber } from '../../utils/formatters'
import { styleTarget } from '../../appearance/styleCore'
import { commonnessScale } from './commonness'
import useAppearancePlot from './useAppearancePlot'

const compactNumber = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 })
const tickLabel = value => compactNumber.format(value).replace('K', 'k')

export default function CommonnessBars({ rows, selected, onSelect, onClear }) {
  const { ref, width, height } = useAppearancePlot()
  const bars = rows.slice(0, 10)
  const scale = commonnessScale(bars)
  const left = Math.max(44, tickLabel(scale.upper).length * 7 + 14), right = width - 12, top = 8
  const slot = (right - left) / 10
  const angled = bars.some(bar => bar.label.length * 6 > slot * .9)
  const baseline = Math.max(top + 24, height - (angled ? 58 : 28))
  const y = value => baseline - value / scale.upper * (baseline - top)
  return <div className="commonness-chart-scroll" onMouseLeave={onClear} onScroll={onClear}>
    <svg {...styleTarget('chart.bars')} ref={ref} className="commonness-chart" viewBox={`0 0 ${width} ${height}`}
      role="group" aria-label="Ten most-common positions per page. Move on X, number of appearances on Y; linear scale.">
      {scale.ticks.map(tick => <g key={tick} className="commonness-y-tick">
        <line x1={left} x2={right} y1={y(tick)} y2={y(tick)} />
        <text {...styleTarget('position.count')} x={left - 8} y={y(tick) + 4} textAnchor="end">{tickLabel(tick)}</text>
      </g>)}
      {bars.map((bar, index) => {
        const x = left + (index + .5) * slot, barWidth = slot * .65
        return <g key={bar.fen} className="commonness-column" role="button" tabIndex={0}
          aria-label={`Rank ${bar.rank}, ${bar.label}: ${formatNumber(bar.appearances)} times`}
          aria-describedby={selected?.bar.fen === bar.fen ? 'common-position-preview' : undefined}
          data-common-rank={bar.rank} data-value={bar.appearances} data-fen={bar.fen}
          onMouseEnter={event => onSelect(bar, event)} onMouseLeave={onClear}
          onFocus={event => onSelect(bar, event)} onBlur={onClear} onClick={event => onSelect(bar, event)}
          onKeyDown={event => {
            if (event.key === 'Escape') onClear()
            if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); onSelect(bar, event) }
          }}>
          <rect className="commonness-bar" x={x - barWidth / 2} y={y(bar.appearances)}
            width={barWidth} height={baseline - y(bar.appearances)} />
          <text {...styleTarget('position.label')} className="commonness-move-label"
            transform={`translate(${x} ${baseline + 16})${angled ? ' rotate(-35)' : ''}`}
            textAnchor={angled ? 'end' : 'middle'}>{bar.label}</text>
        </g>
      })}
    </svg>
  </div>
}
