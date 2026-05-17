import { useEffect, useMemo, useState } from 'react'
import { API_BASE_URL } from '../../config'

const formatNumber = (value) => {
  const numeric = Number(value || 0)
  return new Intl.NumberFormat().format(numeric)
}

const fetchJson = async (path, signal) => {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    signal,
    headers: { Accept: 'application/json' }
  })

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`)
  }

  return response.json()
}

function Hero() {
  const [generalities, setGeneralities] = useState(null)
  const [timeControls, setTimeControls] = useState(null)
  const [error, setError] = useState('')

  useEffect(() => {
    const controller = new AbortController()

    Promise.all([
      fetchJson('/games/generalities', controller.signal),
      fetchJson('/games/time_controls', controller.signal)
    ])
      .then(([generalitiesPayload, timeControlsPayload]) => {
        setGeneralities(generalitiesPayload)
        setTimeControls(timeControlsPayload)
        setError('')
      })
      .catch((err) => {
        if (err?.name !== 'AbortError') {
          setError('Dashboard metrics unavailable.')
        }
      })

    return () => controller.abort()
  }, [])

  const modeTotal = useMemo(() => {
    if (!timeControls) return 0
    return ['bullet', 'blitz', 'rapid'].reduce((sum, key) => sum + Number(timeControls[key] || 0), 0)
  }, [timeControls])

  const metrics = [
    { label: 'Games', value: generalities?.n_games_in_db },
    { label: 'Main Players', value: generalities?.main_characters },
    { label: 'Positions', value: generalities?.n_positions },
    { label: 'Scored FENs', value: generalities?.scored_fens }
  ]

  return (
    <section className="dashboard-hero" id="top">
      <div className="dashboard-hero-copy">
        <p className="eyebrow">Live database overview</p>
        <h2>Chess intelligence, organized for analysis.</h2>
        <p>
          Track games, players, positions, and engine coverage from one compact control surface.
        </p>
        <div className="hero-actions">
          <a className="btn btn-primary" href="/games">Open Games</a>
          <a className="btn btn-secondary" href="/main_characters">Review Players</a>
        </div>
        {error ? <p className="result-line">{error}</p> : null}
      </div>

      <div className="dashboard-panel" aria-label="Database summary">
        <div className="metric-grid">
          {metrics.map((metric) => (
            <article className="metric-card" key={metric.label}>
              <span>{metric.label}</span>
              <strong>{metric.value === undefined ? '-' : formatNumber(metric.value)}</strong>
            </article>
          ))}
        </div>

        <div className="mode-summary">
          <div className="mode-summary-head">
            <span>Time Controls</span>
            <strong>{formatNumber(modeTotal)}</strong>
          </div>
          {['bullet', 'blitz', 'rapid'].map((mode) => {
            const value = Number(timeControls?.[mode] || 0)
            const width = modeTotal ? Math.max(3, (value / modeTotal) * 100) : 0
            return (
              <div className="mode-row" key={mode}>
                <span>{mode}</span>
                <div className="mode-track">
                  <div className={`mode-fill ${mode}`} style={{ width: `${width}%` }} />
                </div>
                <strong>{timeControls ? formatNumber(value) : '-'}</strong>
              </div>
            )
          })}
        </div>
      </div>
    </section>
  )
}

export default Hero
