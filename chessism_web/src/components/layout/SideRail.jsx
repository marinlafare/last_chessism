function SideRail() {
  const path = window.location.pathname.replace(/\/+$/, '') || '/'
  const navItems = [
    { href: '/', label: 'Home', short: 'H' },
    { href: '/games', label: 'Games', short: 'G' },
    { href: '/main_characters', label: 'Players', short: 'P' },
    { href: '/positions', label: 'Positions', short: 'F' },
    { href: '/analyze_times', label: 'Analyze Times', short: 'T' },
    { href: '/download_new_games', label: 'Ingest', short: '+' }
  ]

  return (
    <aside className="left-rail" aria-label="Primary">
      <div className="left-rail-menu">
        <a className="rail-brand" href="/" aria-label="Home">
          <img className="left-rail-icon" src="/chessism.jpg" alt="Chessism icon" />
          <span>Chessism</span>
        </a>
        <nav className="rail-nav" aria-label="Main navigation">
          {navItems.map((item) => (
            <a
              key={item.href}
              className={`rail-btn ${path === item.href ? 'active' : ''}`}
              href={item.href}
              title={item.label}
            >
              <span className="rail-btn-icon" aria-hidden="true">{item.short}</span>
              <span>{item.label}</span>
            </a>
          ))}
        </nav>
      </div>
    </aside>
  )
}

export default SideRail
