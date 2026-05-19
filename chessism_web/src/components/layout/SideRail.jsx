function SideRail() {
  const path = window.location.pathname.replace(/\/+$/, '') || '/'
  const navItems = [
    { href: '/scored_positions', label: 'Scored Positions', short: 'S' },
    { href: '/analize_positions', label: 'Analyze Positions', short: 'F' },
    { href: '/live_analysis', label: 'Live Analysis', short: 'L' },
    { href: '/analyze_times', label: 'Analyze Times', short: 'T' },
    { href: '/add_games', label: 'Add Games', short: '+' }
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
