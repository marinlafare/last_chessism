import { styleTarget } from '../../appearance/styleCore.js'
function SideRail() {
  const path = window.location.pathname.replace(/\/+$/, '') || '/'
  const navItems = [
    { href: '/scored_positions', label: 'Positions', short: 'P' },
    { href: '/analize_positions', label: 'Analyze', short: 'A' },
    { href: '/research', label: 'Research', short: 'R' },
    { href: '/database', label: 'Database', short: 'D' },
    { href: '/live_analysis', label: 'Live', short: 'L' },
    { href: '/analyze_times', label: 'Times', short: 'T' },
    { href: '/hegel', label: 'hegel', short: 'H' },
    { href: '/backups', label: 'Backups', short: 'B' },
    { href: '/system_status', label: 'System Status', short: '●' }
  ]

  return (
    <aside {...styleTarget('sidebar', 'navigation')} className="left-rail" aria-label="Primary">
      <div className="left-rail-menu">
        <a {...styleTarget('link', 'navigation')} className="rail-brand" href="/" aria-label="Home">
          <img className="left-rail-icon" src="/chessism.jpg" alt="Chessism icon" />
          <span>Chessism</span>
        </a>
        <nav {...styleTarget('toolbar', 'navigation')} className="rail-nav" aria-label="Main navigation">
          {navItems.map((item) => (
            <a {...styleTarget('button', 'navigation')}
              key={item.href}
              className={`rail-btn ${path === item.href || (item.href === '/research' && path.startsWith('/research/')) ? 'active' : ''}`}
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
