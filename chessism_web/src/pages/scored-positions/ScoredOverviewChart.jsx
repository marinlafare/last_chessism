import { styleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'

const count = value => value == null || value === '' || !Number.isFinite(Number(value)) || Number(value) < 0
  ? null : Math.trunc(Number(value))
const label = value => value == null ? '—' : value >= 1000000
  ? `${formatNumber(value / 1000000, 1)} M` : formatNumber(value)

export default function ScoredOverviewChart({ overview, gameOverview, coverage }) {
  const groups = [
    { id: 'positions', total: count(coverage?.n_positions), bars: [
      { id: 'positions', target: 'metric.unscored_positions', title: 'Total positions', value: count(coverage?.n_positions), total: true },
      { id: 'scored_positions', target: 'metric.scored_positions', title: 'Scored positions', value: count(overview?.scored_positions) },
    ] },
    { id: 'games', total: count(coverage?.n_games_in_db), bars: [
      { id: 'games', target: 'metric.games', title: 'Total games', value: count(coverage?.n_games_in_db), total: true },
      { id: 'scored_games', target: 'metric.unscored_games', title: 'Scored games', value: count(gameOverview?.fully_analyzed_games) },
    ] },
  ]

  return <figure className="scored-summary-chart" aria-label="Position and game coverage. Each pair has its own scale, with the total at 100 percent.">
    {groups.map(group => <div key={group.id} className="scored-summary-group" data-group={group.id}
      role="group" aria-label={`${group.id}: total and scored share a scale`}>
      {group.bars.map(bar => {
        const percent = group.total > 0 && bar.value != null ? Math.min(100, bar.value / group.total * 100) : 0
        return <div className="scored-summary-row" key={bar.id} tabIndex={0}
          data-stat={bar.id} data-count={bar.value ?? undefined}
          title={`${bar.title}: ${bar.value == null ? 'not available' : formatNumber(bar.value)}`}>
          <div className="scored-summary-label">
            <span {...styleTarget('metric.title')}>{bar.title}</span>
            <strong {...styleTarget('metric.value')}>{label(bar.value)}</strong>
          </div>
          <div {...styleTarget(bar.target)} className={`scored-summary-track${bar.total && percent > 0 ? ' is-total' : ''}`} aria-hidden="true">
            <span className={`scored-summary-fill${bar.total ? ' is-total' : ' is-scored'}`}
              style={{ width: `${percent}%` }} />
          </div>
        </div>
      })}
    </div>)}
  </figure>
}
