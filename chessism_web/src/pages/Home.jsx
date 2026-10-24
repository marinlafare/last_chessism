import { pageStyleTarget } from '../appearance/styleCore.js'
import { lazy, Suspense, useCallback, useEffect, useRef, useState } from 'react'
import './home/home.css'
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
  const [minimized, setMinimized] = useState(() => ({ summary: Boolean(selectedPlayer), players: Boolean(selectedPlayer) }))
  const minimizeOverview = () => setMinimized({ summary: true, players: true })
  useEffect(() => {
    setMinimized({ summary: Boolean(selectedPlayer), players: Boolean(selectedPlayer) })
  }, [selectedPlayer])
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
  const selectPlayer = useCallback((name, { preserveScroll = false } = {}) => {
    const href = playerHomeHref(name)
    pendingScroll.current = !preserveScroll
    setMinimized({ summary: Boolean(normalizePlayer(name)), players: Boolean(normalizePlayer(name)) })
    if (`${window.location.pathname}${window.location.search}` !== href) {
      window.history.pushState(null, '', href)
      window.dispatchEvent(new PopStateEvent('popstate'))
    }
    setScrollRequest(value => value + 1)
  }, [])
  const switchPlayer = useCallback(name => selectPlayer(name, { preserveScroll: true }), [selectPlayer])

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
            <div className="home-overview-section">
            {selectedPlayer && minimized.summary && <button {...pageStyleTarget('overview.restore')} className="home-overview-restore"
              aria-expanded={false} aria-controls="home-summary-content" type="button"
              onClick={() => setMinimized(value => ({ ...value, summary: false }))}>Player overview</button>}
            <div id="home-summary-content" className="home-overview-content" hidden={Boolean(selectedPlayer && minimized.summary)}>
            {selectedPlayer && <button {...pageStyleTarget('overview.minimize')} className="home-overview-minimize" type="button"
              title="Minimize overview panels" aria-label="Minimize both overview panels" onClick={minimizeOverview}>
              <svg viewBox="0 0 20 20" aria-hidden="true"><path d="M4 14h12" /></svg>
            </button>}
            <DashboardSummaryPanel onSelectPlayer={selectPlayer} titledOnly={titledOnly}
              onTitleFilterChange={changeTitleFilter} />
            </div>
            </div>
            <div className="home-overview-section">
            {selectedPlayer && minimized.players && <button {...pageStyleTarget('overview.restore')} className="home-overview-restore"
              aria-expanded={false} aria-controls="home-rankings-content" type="button"
              onClick={() => setMinimized(value => ({ ...value, players: false }))}>Player rankings</button>}
            <div id="home-rankings-content" className="home-overview-content" hidden={Boolean(selectedPlayer && minimized.players)}>
            {selectedPlayer && <button {...pageStyleTarget('overview.minimize')} className="home-overview-minimize" type="button"
              title="Minimize overview panels" aria-label="Minimize both overview panels" onClick={minimizeOverview}>
              <svg viewBox="0 0 20 20" aria-hidden="true"><path d="M4 14h12" /></svg>
            </button>}
            <MainCharactersCarousel selectedPlayer={selectedPlayer} onSelectPlayer={selectPlayer}
              titledOnly={titledOnly} onTitleFilterChange={changeTitleFilter} />
            </div>
            </div>
          </div>
          <section {...pageStyleTarget('player.panel')} className="home-player-workspace" id="selected-player" ref={playerSection}
            data-player-selected={Boolean(selectedPlayer)}
            tabIndex={-1} aria-label={selectedPlayer ? `Player workspace: ${selectedPlayer}` : 'Player workspace'}>
            {selectedPlayer ? <Suspense fallback={<p {...pageStyleTarget('text')} role="status">Loading player workspace…</p>}>
              <PlayerWorkspace key={selectedPlayer} playerName={selectedPlayer} onSelectPlayer={switchPlayer}
                onProfileLoaded={rememberPlayerGroup} />
            </Suspense> : <p {...pageStyleTarget('text')} className="home-player-empty">Select a player to explore their profile and analysis here.</p>}
          </section>
        </main>
      </div>
    </div>
  )
}

export default Home
