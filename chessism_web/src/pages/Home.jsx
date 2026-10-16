import { pageStyleTarget } from '../appearance/styleCore.js'
import { lazy, Suspense, useCallback, useEffect, useRef, useState } from 'react'
import './home/home.css'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import MainCharactersCarousel from './home/MainCharactersCarousel'
import { DashboardSummaryPanel } from './home/Hero'
import { playerFromLocation, playerHomeHref } from '../services/navigation'

const PlayerWorkspace = lazy(() => import('./players/PlayerWorkspace'))

function Home({ locationKey = `${window.location.pathname}${window.location.search}` }) {
  const selectedPlayer = playerFromLocation(locationKey)
  const playerSection = useRef(null)
  const pendingScroll = useRef(false)
  const [scrollRequest, setScrollRequest] = useState(0)
  const [titledOnly, setTitledOnly] = useState(true)
  const selectPlayer = useCallback((name) => {
    const href = playerHomeHref(name)
    if (`${window.location.pathname}${window.location.search}` !== href) {
      window.history.pushState(null, '', href)
      window.dispatchEvent(new PopStateEvent('popstate'))
    }
    pendingScroll.current = true
    setScrollRequest(value => value + 1)
  }, [])

  useEffect(() => {
    if (!pendingScroll.current || !playerSection.current) return
    pendingScroll.current = false
    playerSection.current.focus({ preventScroll: true })
    playerSection.current.scrollIntoView({ block: 'start',
      behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' })
  }, [selectedPlayer, scrollRequest])

  return (
    <div {...pageStyleTarget('page')} className="page-frame home-page">
      <SideRail />
      <div className="home-shell">
        <main>
          <div {...pageStyleTarget('overview.layout')} className="home-overview">
            <DashboardSummaryPanel onSelectPlayer={selectPlayer} titledOnly={titledOnly}
              onTitleFilterChange={setTitledOnly} />
            <MainCharactersCarousel selectedPlayer={selectedPlayer} onSelectPlayer={selectPlayer}
              titledOnly={titledOnly} onTitleFilterChange={setTitledOnly} />
          </div>
          <section {...pageStyleTarget('player.panel')} className="home-player-workspace" id="selected-player" ref={playerSection}
            tabIndex={-1} aria-label={selectedPlayer ? `Player workspace: ${selectedPlayer}` : 'Player workspace'}>
            {selectedPlayer ? <Suspense fallback={<p {...pageStyleTarget('text')} role="status">Loading player workspace…</p>}>
              <PlayerWorkspace key={selectedPlayer} playerName={selectedPlayer} onSelectPlayer={selectPlayer} />
            </Suspense> : <p {...pageStyleTarget('text')} className="home-player-empty">Select a player to explore their profile and analysis here.</p>}
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default Home
