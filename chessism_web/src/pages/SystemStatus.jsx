import { pageStyleTarget } from '../appearance/styleCore.js'
import { lazy, Suspense, useCallback, useState } from 'react'
import './system-status/systemStatus.css'
import SideRail from '../components/layout/SideRail'
import Footer from '../components/layout/Footer'
import StatusPanel from './system-status/StatusPanel'
import CloudStatusPanel from './system-status/CloudStatusPanel'
import CloudBillingPanel from './system-status/CloudBillingPanel'
import { fetchStatus, fetchCloudStatus } from './system-status/systemStatusApi'
import { usePoll } from '../hooks/usePoll'

const AnalyzeTimes = lazy(() => import('./AnalyzeTimes'))

export default function SystemStatus({ initialTimesOpen = false }) {
  const [timesOpen, setTimesOpen] = useState(initialTimesOpen)
  const [timesVisited, setTimesVisited] = useState(initialTimesOpen)
  const fetcher = useCallback(fetchStatus, [])
  const status = usePoll(fetcher, 10000)
  const cloudStatus = usePoll(fetchCloudStatus, 5000)
  return <div {...pageStyleTarget('page')} className="page-frame system-status-page">
    <SideRail />
    <div className="home-shell">
      <main aria-label="System status">
        <div className="system-status-row">
          <StatusPanel {...status} />
          <CloudStatusPanel {...cloudStatus} />
        </div>
        <section {...pageStyleTarget('times.panel')} className="system-times" id="analysis-times">
          <h2><button {...pageStyleTarget('times.toggle')} type="button" className="system-times-toggle"
            id="system-times-toggle" aria-expanded={timesOpen} aria-controls="system-times-content"
            onClick={() => { setTimesVisited(true); setTimesOpen(value => !value) }}>Times</button></h2>
          <div id="system-times-content" hidden={!timesOpen} role="region" aria-labelledby="system-times-toggle">
            {timesVisited && <Suspense fallback={<p role="status">Loading timing data…</p>}><AnalyzeTimes /></Suspense>}
          </div>
        </section>
        <CloudBillingPanel />
      </main>
      <Footer />
    </div>
  </div>
}
