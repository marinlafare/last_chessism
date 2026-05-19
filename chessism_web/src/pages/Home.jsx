import { useCallback } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import MainCharactersCarousel from '../components/home/MainCharactersCarousel'
import { DashboardSummaryPanel } from '../components/home/Hero'
import StatusPanel from '../components/home/StatusPanel'
import ProofPoints from '../components/home/ProofPoints'
import { usePoll } from '../hooks/usePoll'
import { fetchStatus } from '../services/statusService'

function Home() {
  const statusFetcher = useCallback(fetchStatus, [])
  const status = usePoll(statusFetcher, 10000)

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main>
          <div className="home-top-grid">
            <MainCharactersCarousel />
            <StatusPanel {...status} />
          </div>
          <DashboardSummaryPanel />
          <ProofPoints />
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default Home
