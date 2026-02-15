function SideRail() {
  return (
    <aside className="left-rail" aria-label="Primary">
      <div className="left-rail-menu">
        <a href="/" aria-label="Home">
          <img className="left-rail-icon" src="/chessism.jpg" alt="Chessism icon" />
        </a>
        <a className="rail-btn" href="/games">
          Games
        </a>
        <a className="rail-btn" href="/players">
          Players
        </a>
        <a className="rail-btn" href="http://localhost:6789/download_new_games">
          + games
        </a>
      </div>
    </aside>
  )
}

export default SideRail
