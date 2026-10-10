import { useEffect, useState } from 'react'
import { formatNumber } from '../../utils/formatters'
import { fetchDashboardSummary } from './homeApi'

export function DashboardSummaryPanel() {
  const [generalities, setGeneralities] = useState(null)
  const [error, setError] = useState('')

  useEffect(() => {
    const controller = new AbortController()

    fetchDashboardSummary({ signal: controller.signal })
      .then((generalitiesPayload) => {
        setGeneralities(generalitiesPayload)
        setError('')
      })
      .catch((err) => {
        if (err?.name !== 'AbortError') {
          setError('Dashboard metrics unavailable.')
        }
      })

    return () => controller.abort()
  }, [])

  const metrics = [
    { label: 'Main Players', value: generalities?.main_characters },
    { label: 'Games', value: generalities?.n_games_in_db },
    { label: 'Positions', value: generalities?.n_positions },
    { label: 'Scored Positions', value: generalities?.scored_fens }
  ]

  return (
    <div className="dashboard-panel" aria-label="Database summary">
      <div className="metric-grid">
        {metrics.map((metric) => (
          <article className="metric-card" key={metric.label}>
            <span>{metric.label}</span>
            <strong>{metric.value === undefined ? '-' : formatNumber(metric.value)}</strong>
          </article>
        ))}
      </div>

      {error ? <p className="result-line">{error}</p> : null}
    </div>
  )
}
