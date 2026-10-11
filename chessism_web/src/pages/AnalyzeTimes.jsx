import { pageStyleTarget } from '../appearance/styleCore.js'
import { useEffect, useMemo, useState } from 'react'
import './analyze-times/analyzeTimes.css'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import { formatNumber } from '../utils/formatters'
import { fetchAnalysisTimesSummary } from './analyze-times/analyzeTimesApi'

function AnalyzeTimes() {
  const [data, setData] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(true)

  const load = async () => {
    setLoading(true)
    try {
      const payload = await fetchAnalysisTimesSummary()
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
    <div {...pageStyleTarget('page')} className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main>
          <section {...pageStyleTarget('panel')} className="analysis-times-hero">
            <div className="section-head">
              <div>
                <p {...pageStyleTarget('text')} className="eyebrow">Stockfish telemetry</p>
                <h1 {...pageStyleTarget('heading')}>Analyze Times</h1>
              </div>
              <button {...pageStyleTarget('button')} className="btn btn-secondary btn-inline" type="button" onClick={load}>
                Refresh
              </button>
            </div>
            {error ? <div className="status-banner warn">{error}</div> : null}
            <div className="metric-grid">
              <article {...pageStyleTarget('card')} className="metric-card">
                <span>Samples</span>
                <strong {...pageStyleTarget('value')}>{loading ? '-' : `${formatNumber(overall.samples)} / ${formatNumber(overall.retention_limit || 10)}`}</strong>
              </article>
              <article {...pageStyleTarget('card')} className="metric-card">
                <span>Mean sec / FEN</span>
                <strong {...pageStyleTarget('value')}>{loading ? '-' : formatNumber(overall.avg_seconds, 3)}</strong>
              </article>
              <article {...pageStyleTarget('card')} className="metric-card">
                <span>Mean ms / FEN</span>
                <strong {...pageStyleTarget('value')}>{loading ? '-' : formatNumber(overall.avg_ms, 1)}</strong>
              </article>
              <article {...pageStyleTarget('card')} className="metric-card">
                <span>Latest ms</span>
                <strong {...pageStyleTarget('value')}>{loading ? '-' : formatNumber(data?.recent?.[0]?.elapsed_ms, 1)}</strong>
              </article>
            </div>
          </section>

          <section {...pageStyleTarget('panel')} className="analysis-times-panel">
            <div className="section-head">
              <div>
                <p {...pageStyleTarget('text')} className="eyebrow">Piece count</p>
                <h2 {...pageStyleTarget('title')}>Average Analysis Time</h2>
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
                    <strong {...pageStyleTarget('value')}>{formatNumber(Number(row.avg_ms || 0) / 1000, 3)} s</strong>
                    <small {...pageStyleTarget('text')}>{formatNumber(row.samples)} samples</small>
                  </div>
                )
              })}
              {!loading && !data?.by_pieces?.length ? <p {...pageStyleTarget('text')} className="result-line">No timing samples yet.</p> : null}
            </div>
          </section>

          <section {...pageStyleTarget('panel')} className="analysis-times-panel">
            <div className="section-head">
              <div>
                <p {...pageStyleTarget('text')} className="eyebrow">Recent</p>
                <h2 {...pageStyleTarget('title')}>Last 10 Samples</h2>
              </div>
            </div>
            <div className="analysis-table-wrap">
              <table {...pageStyleTarget('table')} className="analysis-times-table">
                <thead>
                  <tr>
                    <th {...pageStyleTarget('table.header')}>Time</th>
                    <th {...pageStyleTarget('table.header')}>Source</th>
                    <th {...pageStyleTarget('table.header')}>Pieces</th>
                    <th {...pageStyleTarget('table.header')}>Nodes</th>
                    <th {...pageStyleTarget('table.header')}>MultiPV</th>
                    <th {...pageStyleTarget('table.header')}>seconds</th>
                  </tr>
                </thead>
                <tbody>
                  {(data?.recent || []).map((row, index) => (
                    <tr key={`${row.created_at}-${index}`}>
                      <td {...pageStyleTarget('table.cell')}>{row.created_at ? new Date(row.created_at).toLocaleString() : '-'}</td>
                      <td {...pageStyleTarget('table.cell')}>{row.source}</td>
                      <td {...pageStyleTarget('table.cell')}>{row.n_pieces}</td>
                      <td {...pageStyleTarget('table.cell')}>{formatNumber(row.nodes_limit)}</td>
                      <td {...pageStyleTarget('table.cell')}>{row.multipv}</td>
                      <td {...pageStyleTarget('table.cell')}>{formatNumber(Number(row.elapsed_ms || 0) / 1000, 3)}</td>
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
