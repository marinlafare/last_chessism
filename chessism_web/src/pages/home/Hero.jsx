import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import { fetchDashboardSummary } from './homeApi'
import SummaryPie from './SummaryPie.jsx'
import SummaryGamesChart from './SummaryGamesChart.jsx'
import { positionSlices, summaryGamePlayers } from './summaryData.js'
import { summaryPolling } from './summaryPolling.js'

export function DashboardSummaryPanel({ onSelectPlayer, titledOnly = true, onTitleFilterChange }) {
  const [generalities, setGeneralities] = useState(null)
  const [error, setError] = useState('')

  useEffect(() => {
    const poller = summaryPolling({
      fetchSummary: fetchDashboardSummary,
      isHidden: () => document.hidden,
      onData: payload => {
        setGeneralities(payload)
        setError('')
      },
      onError: () => setError('Dashboard metrics unavailable. Showing the last snapshot if available.'),
    })
    const refreshVisible = () => { if (!document.hidden) poller.refresh() }
    poller.refresh()
    document.addEventListener('visibilitychange', refreshVisible)
    window.addEventListener('focus', refreshVisible)
    return () => {
      poller.stop()
      document.removeEventListener('visibilitychange', refreshVisible)
      window.removeEventListener('focus', refreshVisible)
    }
  }, [])

  const ready = Boolean(generalities?.refreshed_at)
  const gamePlayers = summaryGamePlayers(generalities, titledOnly)

  return (
    <div {...pageStyleTarget('summary.panel')} className="dashboard-panel" aria-label="Database summary">
      <div className="home-summary-charts">
        <SummaryPie items={positionSlices(generalities)} ready={ready} totalFens={generalities?.total_fens}
          titledOnly={titledOnly} onTitleFilterChange={onTitleFilterChange} />
        <SummaryGamesChart key={titledOnly ? 'titled' : 'untitled'} titledOnly={titledOnly}
          players={gamePlayers}
          ready={Array.isArray(gamePlayers)} onSelectPlayer={onSelectPlayer} />
      </div>
      {(error || generalities?.refresh_error || generalities?.stale) && <p {...pageStyleTarget('text')} className="home-summary-state" role="status">
        {error || generalities?.refresh_error || (ready ? 'Updating summary… Previous snapshot shown.' : 'Preparing database summary…')}
      </p>}
    </div>
  )
}
