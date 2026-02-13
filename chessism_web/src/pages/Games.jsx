import { useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import { API_BASE_URL } from '../config'

async function sendPlayerAction(path, playerName) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ player_name: playerName })
  })

  const payload = await response.json().catch(() => ({ message: `HTTP ${response.status}` }))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchPlayerGameCount(playerName) {
  const response = await fetch(`${API_BASE_URL}/games/${encodeURIComponent(playerName)}/count`)
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchRecentGamesPage(playerName, page, pageSize = 10) {
  const response = await fetch(
    `${API_BASE_URL}/games/${encodeURIComponent(playerName)}/recent?page=${page}&page_size=${pageSize}`
  )
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

async function fetchGameSummary(playerName) {
  const response = await fetch(`${API_BASE_URL}/games/${encodeURIComponent(playerName)}/summary`)
  const payload = await response.json().catch(() => ({}))

  if (!response.ok) {
    throw new Error(payload.detail || payload.message || `HTTP ${response.status}`)
  }

  return payload
}

function Games() {
  const [downloadPlayer, setDownloadPlayer] = useState('')
  const [updatePlayer, setUpdatePlayer] = useState('')
  const [countPlayer, setCountPlayer] = useState('')

  const [downloadResult, setDownloadResult] = useState('')
  const [updateResult, setUpdateResult] = useState('')
  const [countResult, setCountResult] = useState('')
  const [selectedPlayer, setSelectedPlayer] = useState('')
  const [recentPage, setRecentPage] = useState(1)
  const [recentData, setRecentData] = useState(null)
  const [recentError, setRecentError] = useState('')
  const [summaryData, setSummaryData] = useState(null)
  const [summaryError, setSummaryError] = useState('')
  const [summaryLoading, setSummaryLoading] = useState(false)

  const [downloadLoading, setDownloadLoading] = useState(false)
  const [updateLoading, setUpdateLoading] = useState(false)
  const [countLoading, setCountLoading] = useState(false)
  const [recentLoading, setRecentLoading] = useState(false)

  const loadRecentGames = async (player, page) => {
    setRecentLoading(true)
    setRecentError('')
    try {
      const payload = await fetchRecentGamesPage(player, page, 10)
      setRecentData(payload)
      setRecentPage(payload.page || page)
    } catch (error) {
      setRecentError(error instanceof Error ? error.message : 'Request failed.')
      setRecentData(null)
    } finally {
      setRecentLoading(false)
    }
  }

  const handleDownload = async () => {
    const player = downloadPlayer.trim()
    if (!player) {
      setDownloadResult('Enter a player name first.')
      return
    }

    setDownloadLoading(true)
    setDownloadResult('')
    try {
      const payload = await sendPlayerAction('/games', player)
      setDownloadResult(payload.message || 'Games download started.')
    } catch (error) {
      setDownloadResult(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      setDownloadLoading(false)
    }
  }

  const handleUpdate = async () => {
    const player = updatePlayer.trim()
    if (!player) {
      setUpdateResult('Enter a player name first.')
      return
    }

    setUpdateLoading(true)
    setUpdateResult('')
    try {
      const payload = await sendPlayerAction('/games/update', player)
      setUpdateResult(payload.message || 'Games update started.')
    } catch (error) {
      setUpdateResult(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      setUpdateLoading(false)
    }
  }

  const handleCount = async () => {
    const player = countPlayer.trim()
    if (!player) {
      setCountResult('Enter a player name first.')
      return
    }

    setCountLoading(true)
    setCountResult('')
    try {
      const payload = await fetchPlayerGameCount(player)
      const total = payload.total_games ?? 0
      setCountResult(`${payload.player_name || player}: ${total} games in the system.`)
      setSelectedPlayer(payload.player_name || player)
      await loadRecentGames(payload.player_name || player, 1)
      setSummaryLoading(true)
      setSummaryError('')
      const summary = await fetchGameSummary(payload.player_name || player)
      setSummaryData(summary)
    } catch (error) {
      setCountResult(error instanceof Error ? error.message : 'Request failed.')
      setSelectedPlayer('')
      setRecentData(null)
      setRecentError('')
      setSummaryData(null)
      setSummaryError(error instanceof Error ? error.message : 'Request failed.')
    } finally {
      setCountLoading(false)
      setSummaryLoading(false)
    }
  }

  const handlePrevious = async () => {
    if (!selectedPlayer || recentPage <= 1 || recentLoading) return
    await loadRecentGames(selectedPlayer, recentPage - 1)
  }

  const handleNext = async () => {
    if (!selectedPlayer || recentLoading || !recentData) return
    const totalPages = recentData.total_pages || 0
    if (recentPage >= totalPages) return
    await loadRecentGames(selectedPlayer, recentPage + 1)
  }

  return (
    <div className="home-shell">
      <Header />
      <main>
        <section className="games-hero">
          <h1>Games</h1>
          <p>Download games, update games, and inspect how many games exist for a player.</p>
        </section>

        <section className="games-actions" aria-label="Games actions">
          <article className="game-card">
            <h2>Download Games of a Player</h2>
            <label htmlFor="download-player">Player name</label>
            <input
              id="download-player"
              className="text-input"
              type="text"
              value={downloadPlayer}
              onChange={(event) => setDownloadPlayer(event.target.value)}
              onKeyDown={(event) => event.key === 'Enter' && handleDownload()}
              placeholder="e.g. hikaru"
            />
            <button className="btn btn-primary" type="button" onClick={handleDownload} disabled={downloadLoading}>
              {downloadLoading ? 'Working...' : 'Download Games'}
            </button>
            <p className="result-line">{downloadResult || '-'}</p>
          </article>

          <article className="game-card">
            <h2>Update Games of a Player</h2>
            <label htmlFor="update-player">Player name</label>
            <input
              id="update-player"
              className="text-input"
              type="text"
              value={updatePlayer}
              onChange={(event) => setUpdatePlayer(event.target.value)}
              onKeyDown={(event) => event.key === 'Enter' && handleUpdate()}
              placeholder="e.g. magnuscarlsen"
            />
            <button className="btn btn-primary" type="button" onClick={handleUpdate} disabled={updateLoading}>
              {updateLoading ? 'Working...' : 'Update Games'}
            </button>
            <p className="result-line">{updateResult || '-'}</p>
          </article>
        </section>

        <section className="games-count" aria-label="Games explorer">
          <div className="section-head">
            <h2>Games Explorer</h2>
            <p>Check how many games exist, then view stats and recent games.</p>
          </div>
          <div className="count-row">
            <div className="count-controls">
              <label htmlFor="count-player">Player name</label>
              <input
                id="count-player"
                className="text-input"
                type="text"
                value={countPlayer}
                onChange={(event) => setCountPlayer(event.target.value)}
                onKeyDown={(event) => event.key === 'Enter' && handleCount()}
                placeholder="e.g. gothamchess"
              />
              <button className="btn btn-secondary" type="button" onClick={handleCount} disabled={countLoading}>
                {countLoading ? 'Checking...' : 'Check Count'}
              </button>
              <p className="result-line strong">{countResult || '-'}</p>
            </div>

            {summaryLoading ? <p className="result-line">Loading stats...</p> : null}
            {summaryError ? <p className="result-line">{summaryError}</p> : null}
            {summaryData ? (
              <div className="games-stats">
                <div className="stat-chip stat-win">Wins: {summaryData.wins ?? 0}</div>
                <div className="stat-chip stat-loss">Losses: {summaryData.losses ?? 0}</div>
                <div className="stat-chip stat-draw">Draws: {summaryData.draws ?? 0}</div>
                <div className="stat-chip">From: {summaryData.date_from ? summaryData.date_from.slice(0, 10) : '—'}</div>
                <div className="stat-chip">To: {summaryData.date_to ? summaryData.date_to.slice(0, 10) : '—'}</div>
                <div className="stat-chip">Modes: {summaryData.time_controls?.length || 0}</div>
                {summaryData.time_controls?.length ? (
                  <div className="modes-list">
                    {summaryData.time_controls.map((item) => (
                      <span key={item.time_control} className="mode-chip">
                        {item.time_control}: {item.total}
                      </span>
                    ))}
                  </div>
                ) : null}
              </div>
            ) : null}
          </div>
        </section>

        {selectedPlayer ? (
          <section className="games-recent" aria-label="Recent player games">
            <div className="section-head">
              <h2>Recent Games</h2>
              <p>Last 10 games for {selectedPlayer}. Use pagination to browse the next ten.</p>
            </div>

            <div className="recent-pagination">
              <button className="btn btn-secondary" type="button" onClick={handlePrevious} disabled={recentLoading || recentPage <= 1}>
                Previous 10
              </button>
              <span>
                Page {recentData?.page || recentPage} / {recentData?.total_pages || 0}
              </span>
              <button
                className="btn btn-secondary"
                type="button"
                onClick={handleNext}
                disabled={recentLoading || !recentData || recentPage >= (recentData.total_pages || 0)}
              >
                Next 10
              </button>
            </div>

            {recentLoading ? <p className="result-line">Loading games...</p> : null}
            {recentError ? <p className="result-line">{recentError}</p> : null}

            {!recentLoading && !recentError ? (
              <div className="recent-list">
                {recentData?.games?.length ? (
                  recentData.games.map((game, index) => {
                    const [datePart, timePart] = game.played_at.split(' ')
                    const [year, monthNum, day] = datePart.split('-')
                    const month = new Intl.DateTimeFormat('en', { month: 'short' }).format(
                      new Date(parseInt(year, 10), parseInt(monthNum, 10) - 1, 1)
                    ).toUpperCase()
                    const resultClass =
                      game.result === 'win' ? 'result-win' : game.result === 'loss' ? 'result-loss' : 'result-draw'

                    return (
                    <a
                      key={`${game.link || index}-${game.played_at}`}
                      className="recent-line"
                      href={`/games/${game.link || ''}`}
                      aria-label={`Open game ${game.link || ''}`}
                    >
                      <span className="recent-date date-chip">{year}</span>
                      <span className="recent-date date-chip">{month}</span>
                      <span className="recent-date date-chip">{day}</span>
                      <span className="time-chip">{timePart}</span>
                      <span className={`color-chip ${game.color === 'white' ? 'color-white' : 'color-black'}`}>
                        {game.color}
                      </span>
                      <span className={resultClass}>{game.result}</span>
                      <span className="recent-open">Open</span>
                    </a>
                    )
                  })
                ) : (
                  <p className="recent-line">No games found.</p>
                )}
              </div>
            ) : null}
          </section>
        ) : null}
      </main>
      <Footer />
    </div>
  )
}

export default Games
