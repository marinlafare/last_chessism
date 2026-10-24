import { pageStyleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import UnexpectedEndingBar from './UnexpectedEndingBar'

const STYLE = 'player.endings'
const COUNTS = ['eligible_games', 'assessable_games', 'ahead_games', 'balanced_games', 'behind_games',
  'missing_final_position', 'missing_final_score', 'mate_games', 'tablebase_games']
const BANDS = [
  { id: 'ahead', label: 'Ahead', interval: '> +200 cp' },
  { id: 'balanced', label: 'Balanced', interval: '−200 to +200 cp, inclusive' },
  { id: 'behind', label: 'Behind', interval: '< −200 cp' },
]
const CHARTS = [
  { id: 'time', label: 'Time endings', endings: ['lost_on_time', 'won_on_time'] },
  { id: 'resignation', label: 'Resignations', endings: ['lost_by_resignation', 'won_by_resignation'] },
]
const percentage = value => value > 0 && value < .1 ? '<0.1%' : `${value.toFixed(1)}%`

function sumCounts(rows) {
  return Object.fromEntries(COUNTS.map(key => [key, rows.reduce((sum, row) => sum + Number(row?.[key] || 0), 0)]))
}

export default function UnexpectedEndingsCharts({ data, modes, disabled, onSelect }) {
  const endings = Object.fromEntries(data.endings.map(ending => [ending.id, ending]))
  return <div className="unexpected-endings-grid">
    {CHARTS.map(chart => {
      const rows = chart.endings.map(id => ({
        ...endings[id], counts: sumCounts(modes.map(mode => data.by_mode[mode]?.[id])),
      }))
      return <section {...pageStyleTarget(`${STYLE}.card`)} className="unexpected-ending-chart" key={chart.id}
        aria-label={`${chart.label}: final-position evaluation`}>
        <div className="unexpected-ending-chart-heading">
          <h4 {...pageStyleTarget(`${STYLE}.text`)} className="unexpected-ending-chart-title">{chart.label}</h4>
          <div className="unexpected-ending-legend" aria-label="Final-position evaluation categories">
            {BANDS.map(band => <span key={band.id} title={band.interval}>
              <i className={`unexpected-ending-swatch ${band.id}`} aria-hidden="true" />
              {band.label}
            </span>)}
          </div>
        </div>
        {rows.map(row => <div className="unexpected-ending-row" key={row.id}>
          <span {...pageStyleTarget(`${STYLE}.text`)} className="unexpected-ending-label">{row.label}</span>
          <UnexpectedEndingBar key={`${row.id}:${modes.join(',')}`} row={row} bands={BANDS} modes={modes}
            disabled={disabled} onSelect={onSelect} formatPercent={percentage} />
          <strong {...pageStyleTarget(`${STYLE}.text`)} className="unexpected-ending-total"
            title={`${formatNumber(row.counts.assessable_games)} assessed / ${formatNumber(row.counts.eligible_games)} recorded games\nExcluded: ${formatNumber(row.counts.missing_final_position + row.counts.missing_final_score)} missing · ${formatNumber(row.counts.mate_games)} mate · ${formatNumber(row.counts.tablebase_games)} tablebase`}
            aria-label={`${formatNumber(row.counts.assessable_games)} assessed games`}>
            {formatNumber(row.counts.assessable_games)}
          </strong>
        </div>)}
        <div className="unexpected-ending-axis" aria-hidden="true"><span>0%</span><span>50%</span><span>100%</span></div>
      </section>
    })}
  </div>
}
