import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import { fetchDashboardSummary } from './homeApi'
import SummaryPie from './SummaryPie.jsx'
import SummaryGamesChart from './SummaryGamesChart.jsx'
import { positionSlices } from './summaryData.js'

export function DashboardSummaryPanel({ onSelectPlayer }) {
  const [generalities, setGeneralities] = useState(null)
  const [error, setError] = useState('')

  useEffect(() => {
    const controller = new AbortController()

    let timer
    const load = async () => {
      let delay = 60000
      try {
        if (document.hidden) return
        const payload = await fetchDashboardSummary({ signal: controller.signal })
        if (controller.signal.aborted) return
        setGeneralities(payload)
        setError('')
        if (payload.stale) delay = 15000
      } catch (err) {
        if (err?.name !== 'AbortError') setError('Dashboard metrics unavailable. Showing the last snapshot if available.')
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(load, delay)
      }
    }
    load()
    return () => { controller.abort(); clearTimeout(timer) }
  }, [])

  const ready = Boolean(generalities?.refreshed_at)

  return (
    <div {...pageStyleTarget('summary.panel')} className="dashboard-panel" aria-label="Database summary">
      <div className="home-summary-charts">
        <SummaryPie items={positionSlices(generalities)} ready={ready} />
        <SummaryGamesChart players={generalities?.players} ready={ready} onSelectPlayer={onSelectPlayer} />
      </div>
      {(error || generalities?.refresh_error || generalities?.stale) && <p {...pageStyleTarget('text')} className="home-summary-state" role="status">
        {error || generalities?.refresh_error || (ready ? 'Updating summary… Previous snapshot shown.' : 'Preparing database summary…')}
      </p>}
    </div>
  )
}
