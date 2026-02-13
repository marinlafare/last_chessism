const navLinks = [
  { label: 'Home', href: '/' },
  { label: 'Games', href: '/games' },
  { label: 'Players', href: '/players' },
  { label: 'API Docs', href: '/api/docs' }
]

function Header() {
  return (
    <header className="site-header">
      <a className="brand" href="/" aria-label="Chessism home">
        <img className="brand-mark" src="/chessism.jpg" alt="Chessism icon" />
        <span className="brand-text">Chessism</span>
      </a>
      <nav className="site-nav" aria-label="Primary">
        {navLinks.map((link) => (
          <a key={link.label} href={link.href} className="nav-link">
            {link.label}
          </a>
        ))}
      </nav>
    </header>
  )
}

export default Header
