import { useCallback } from 'react'
import './home/home.css'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import MainCharactersCarousel from './home/MainCharactersCarousel'
import { DashboardSummaryPanel } from './home/Hero'
import StatusPanel from './home/StatusPanel'
import ProofPoints from './home/ProofPoints'
import { usePoll } from '../hooks/usePoll'
import { fetchStatus } from './home/homeApi'

function Home() {
  const statusFetcher = useCallback(fetchStatus, [])
  const status = usePoll(statusFetcher, 10000)

  return (
    <div className="page-frame home-page">
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
