import { useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
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

function DownloadNewGames() {
  const [downloadPlayer, setDownloadPlayer] = useState('')
  const [updatePlayer, setUpdatePlayer] = useState('')
  const [downloadResult, setDownloadResult] = useState('')
  const [updateResult, setUpdateResult] = useState('')
  const [downloadLoading, setDownloadLoading] = useState(false)
  const [updateLoading, setUpdateLoading] = useState(false)
  const actionBusy = downloadLoading || updateLoading

  const handleDownload = async () => {
    if (actionBusy) return
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
    if (actionBusy) return
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

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main>
          <section className="games-hero">
            <h1>download_new_games</h1>
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
              <button className="btn btn-primary" type="button" onClick={handleDownload} disabled={actionBusy}>
                {downloadLoading ? 'Working...' : actionBusy ? 'Busy...' : 'Download Games'}
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
              <button className="btn btn-primary" type="button" onClick={handleUpdate} disabled={actionBusy}>
                {updateLoading ? 'Working...' : actionBusy ? 'Busy...' : 'Update Games'}
              </button>
              <p className="result-line">{updateResult || '-'}</p>
            </article>
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default DownloadNewGames
