function Hero() {
  const jumpToStatus = (event) => {
    event.preventDefault()
    document.getElementById('status')?.scrollIntoView({ behavior: 'smooth' })
  }

  return (
    <section className="hero" id="top">
      <p className="hero-kicker">Engine analysis for real players</p>
      <h1>Engine-powered chess analysis for practical improvement.</h1>
      <p className="hero-subhead">
        Turn raw games into critical moments, blunders, and repeatable insights.
      </p>
      <div className="hero-games-link">
        <a className="btn btn-games" href="/games">GAMES</a>
        <a className="btn btn-players" href="/players">PLAYER</a>
      </div>
      <div className="hero-actions">
        <a className="btn btn-primary" href="/api/docs#/Analysis/run_analysis_job_analysis_run_job_post">Analyze a PGN</a>
        <a className="btn btn-secondary" href="#status" onClick={jumpToStatus}>
          View system status
        </a>
      </div>
    </section>
  )
}

export default Hero
