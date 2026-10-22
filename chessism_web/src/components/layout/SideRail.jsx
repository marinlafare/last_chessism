import { styleTarget } from '../../appearance/styleCore.js'
function SideRail() {
  const path = window.location.pathname.replace(/\/+$/, '') || '/'
  const navItems = [
    { id: 'positions', href: '/scored_positions', label: 'Positions' },
    { id: 'analyze', href: '/analize_positions', label: 'Analyze' },
    { id: 'research', href: '/research', label: 'Research' },
    { id: 'database', href: '/database', label: 'Database' },
    { id: 'live', href: '/live_analysis', label: 'Live' },
    { id: 'hegel', href: '/hegel', label: 'Hegel' },
    { id: 'backups', href: '/backups', label: 'Backups' },
    { id: 'system', href: '/system_status', label: 'System Status' }
  ]

  return (
    <aside {...styleTarget('sidebar', 'navigation')} className="left-rail" aria-label="Primary">
      <div className="left-rail-menu">
        <a {...styleTarget('link', 'navigation')} className="rail-brand" href="/" aria-label="Home">
          <img className="left-rail-icon" src="/chessism.jpg" alt="Chessism icon" />
          <span>Chessism</span>
        </a>
        <nav {...styleTarget('menu', 'navigation')} className="rail-nav" aria-label="Main navigation">
          {navItems.map((item) => (
            <a {...styleTarget(`item.${item.id}`, 'navigation')}
              key={item.href}
              className={`rail-btn ${path === item.href || (item.href === '/research' && path.startsWith('/research/'))
                || (item.href === '/system_status' && path === '/analyze_times') ? 'active' : ''}`}
              href={item.href}
              title={item.label}
            >
              <span>{item.label}</span>
            </a>
          ))}
        </nav>
      </div>
    </aside>
  )
}

export default SideRail
