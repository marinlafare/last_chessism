import { useEffect, useMemo, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import { API_BASE_URL } from '../config'

const formatNumber = (value, digits = 0) => {
  const numeric = Number(value ?? 0)
  if (!Number.isFinite(numeric)) return '0'
  return numeric.toLocaleString('en-US', {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits
  })
}

async function fetchSummary() {
  const response = await fetch(`${API_BASE_URL}/analysis_times/summary?limit=10`)
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }
  return payload
}

function AnalyzeTimes() {
  const [data, setData] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(true)

  const load = async () => {
    setLoading(true)
    try {
      const payload = await fetchSummary()
      setData(payload)
      setError('')
    } catch (err) {
      setError(err.message || 'Analysis timing unavailable')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    load()
  }, [])

  const overall = data?.overall || {}
  const maxAvg = useMemo(
    () => Math.max(1, ...(data?.by_pieces || []).map((row) => Number(row.avg_ms || 0))),
    [data]
  )

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main>
          <section className="analysis-times-hero">
            <div className="section-head">
              <div>
                <p className="eyebrow">Stockfish telemetry</p>
                <h1>Analyze Times</h1>
              </div>
              <button className="btn btn-secondary btn-inline" type="button" onClick={load}>
                Refresh
              </button>
            </div>
            {error ? <div className="status-banner warn">{error}</div> : null}
            <div className="metric-grid">
              <article className="metric-card">
                <span>Samples</span>
                <strong>{loading ? '-' : `${formatNumber(overall.samples)} / ${formatNumber(overall.retention_limit || 10)}`}</strong>
              </article>
              <article className="metric-card">
                <span>Mean sec / FEN</span>
                <strong>{loading ? '-' : formatNumber(overall.avg_seconds, 3)}</strong>
              </article>
              <article className="metric-card">
                <span>Mean ms / FEN</span>
                <strong>{loading ? '-' : formatNumber(overall.avg_ms, 1)}</strong>
              </article>
              <article className="metric-card">
                <span>Latest ms</span>
                <strong>{loading ? '-' : formatNumber(data?.recent?.[0]?.elapsed_ms, 1)}</strong>
              </article>
            </div>
          </section>

          <section className="analysis-times-panel">
            <div className="section-head">
              <div>
                <p className="eyebrow">Piece count</p>
                <h2>Average Analysis Time</h2>
              </div>
            </div>
            <div className="time-bars">
              {(data?.by_pieces || []).map((row) => {
                const width = Math.max(2, (Number(row.avg_ms || 0) / maxAvg) * 100)
                return (
                  <div className="time-row" key={row.n_pieces}>
                    <span>{row.n_pieces}</span>
                    <div className="time-track">
                      <div className="time-fill" style={{ width: `${width}%` }} />
                    </div>
                    <strong>{formatNumber(Number(row.avg_ms || 0) / 1000, 3)} s</strong>
                    <small>{formatNumber(row.samples)} samples</small>
                  </div>
                )
              })}
              {!loading && !data?.by_pieces?.length ? <p className="result-line">No timing samples yet.</p> : null}
            </div>
          </section>

          <section className="analysis-times-panel">
            <div className="section-head">
              <div>
                <p className="eyebrow">Recent</p>
                <h2>Last 10 Samples</h2>
              </div>
            </div>
            <div className="analysis-table-wrap">
              <table className="analysis-times-table">
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Source</th>
                    <th>Pieces</th>
                    <th>Nodes</th>
                    <th>MultiPV</th>
                    <th>seconds</th>
                  </tr>
                </thead>
                <tbody>
                  {(data?.recent || []).map((row, index) => (
                    <tr key={`${row.created_at}-${index}`}>
                      <td>{row.created_at ? new Date(row.created_at).toLocaleString() : '-'}</td>
                      <td>{row.source}</td>
                      <td>{row.n_pieces}</td>
                      <td>{formatNumber(row.nodes_limit)}</td>
                      <td>{row.multipv}</td>
                      <td>{formatNumber(Number(row.elapsed_ms || 0) / 1000, 3)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default AnalyzeTimes
