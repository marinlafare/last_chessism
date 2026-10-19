import { pageStyleTarget } from '../appearance/styleCore.js'
import { useCallback } from 'react'
import './system-status/systemStatus.css'
import SideRail from '../components/layout/SideRail'
import Footer from '../components/layout/Footer'
import StatusPanel from './system-status/StatusPanel'
import CloudStatusPanel from './system-status/CloudStatusPanel'
import CloudBillingPanel from './system-status/CloudBillingPanel'
import { fetchStatus, fetchCloudStatus } from './system-status/systemStatusApi'
import { usePoll } from '../hooks/usePoll'

export default function SystemStatus() {
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
        <CloudBillingPanel />
      </main>
      <Footer />
    </div>
  </div>
}
