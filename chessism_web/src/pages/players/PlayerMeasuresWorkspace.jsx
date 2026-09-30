import { useEffect, useState } from 'react'
import PlayerMeasuresPanel from './PlayerMeasuresPanel'
import { fetchQualityCalendar } from './playerAnalysisApi'
import './player-measures.css'
import './player-game-explorer.css'

export default function PlayerMeasuresWorkspace({ playerName, disabled = false }) {
  const [quality, setQuality] = useState(null)
  const [qualityError, setQualityError] = useState('')
  const [qualityLoading, setQualityLoading] = useState(false)

  useEffect(() => {
    if (!playerName || disabled) {
      setQuality(null)
      setQualityError('')
      setQualityLoading(false)
      return undefined
    }

    const controller = new AbortController()
    setQualityLoading(true)
    setQualityError('')
    fetchQualityCalendar({ playerName, signal: controller.signal, bypassCache: true })
      .then((payload) => {
        if (!controller.signal.aborted) setQuality(payload)
      })
      .catch((requestError) => {
        if (requestError.name !== 'AbortError') {
          setQualityError(requestError.message || 'Unable to load Stockfish measures.')
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setQualityLoading(false)
      })
    return () => {
      controller.abort()
    }
  }, [playerName, disabled])

  return (
    <section className="games-mode-detail player-measures-workspace">
      <div className="player-analysis-heading">
        <div>
          <p className="eyebrow">STOCKFISH ANALYSIS</p>
          <h2>Position quality</h2>
        </div>
      </div>

      {qualityError ? <p className="player-analysis-error" role="alert">{qualityError}</p> : null}
      {qualityLoading ? <div className="player-analysis-loading" aria-live="polite"><span />Calculating engine measures…</div> : null}
      {!qualityLoading && !qualityError && quality ? (
        <PlayerMeasuresPanel
          playerName={playerName}
          quality={quality}
        />
      ) : null}
    </section>
  )
}
