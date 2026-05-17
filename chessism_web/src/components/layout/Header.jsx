function Header() {
  return (
    <header className="site-header">
      <div>
        <p className="eyebrow">Chess analytics workstation</p>
        <h1>Chessism</h1>
      </div>
      <div className="header-actions">
        <a className="btn btn-secondary btn-inline" href="/api/docs">API Docs</a>
        <a className="btn btn-primary btn-inline" href="/download_new_games">Ingest Games</a>
      </div>
    </header>
  )
}

export default Header
