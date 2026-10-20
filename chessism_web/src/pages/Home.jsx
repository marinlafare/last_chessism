import { pageStyleTarget } from '../appearance/styleCore.js'
import { lazy, Suspense, useCallback, useEffect, useRef, useState } from 'react'
import './home/home.css'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import MainCharactersCarousel from './home/MainCharactersCarousel'
import { DashboardSummaryPanel } from './home/Hero'
import { normalizePlayer, playerFromLocation, playerHomeHref } from '../services/navigation'

const PlayerWorkspace = lazy(() => import('./players/PlayerWorkspace'))
// Keep classification consistent with the existing player-ranking API.
const OFFICIAL_TITLES = new Set(['GM', 'IM', 'FM', 'CM', 'WGM', 'WIM', 'WFM', 'WCM', 'NM', 'WNM'])

function Home({ locationKey = `${window.location.pathname}${window.location.search}` }) {
  const selectedPlayer = playerFromLocation(locationKey)
  const playerSection = useRef(null)
  const pendingScroll = useRef(false)
  const [scrollRequest, setScrollRequest] = useState(0)
  const [titledOnly, setTitledOnly] = useState(true)
  const playerGroups = useRef(new Map())
  const rememberPlayerGroup = useCallback(profile => {
    const name = normalizePlayer(profile?.player_name)
    if (name) playerGroups.current.set(name, OFFICIAL_TITLES.has(String(profile.title || '').trim().toUpperCase()))
  }, [])
  const changeTitleFilter = useCallback(nextValue => {
    const next = Boolean(nextValue)
    const selectedGroup = playerGroups.current.get(selectedPlayer) ?? titledOnly
    setTitledOnly(next)
    if (!selectedPlayer || selectedGroup === next) return
    pendingScroll.current = false
    const url = new URL(window.location.href)
    url.searchParams.delete('player')
    window.history.pushState(null, '', `${url.pathname}${url.search}${url.hash}`)
    window.dispatchEvent(new PopStateEvent('popstate'))
  }, [selectedPlayer, titledOnly])
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
              onTitleFilterChange={changeTitleFilter} />
            <MainCharactersCarousel selectedPlayer={selectedPlayer} onSelectPlayer={selectPlayer}
              titledOnly={titledOnly} onTitleFilterChange={changeTitleFilter} />
          </div>
          <section {...pageStyleTarget('player.panel')} className="home-player-workspace" id="selected-player" ref={playerSection}
            tabIndex={-1} aria-label={selectedPlayer ? `Player workspace: ${selectedPlayer}` : 'Player workspace'}>
            {selectedPlayer ? <Suspense fallback={<p {...pageStyleTarget('text')} role="status">Loading player workspace…</p>}>
              <PlayerWorkspace key={selectedPlayer} playerName={selectedPlayer} onSelectPlayer={selectPlayer}
                onProfileLoaded={rememberPlayerGroup} />
            </Suspense> : <p {...pageStyleTarget('text')} className="home-player-empty">Select a player to explore their profile and analysis here.</p>}
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default Home
