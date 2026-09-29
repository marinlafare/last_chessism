import { useEffect, useState } from 'react'
import './player-analysis.css'
import './player-hero-analytics.css'
import PlayerBehaviouralPanel from './PlayerBehaviouralPanel'
import PlayerMeasuresWorkspace from './PlayerMeasuresWorkspace'
import {
  fetchBehaviouralActivity,
  fetchBehaviouralRatings,
} from './playerAnalysisApi'

export default function PlayerAnalysisWorkspace({ playerName, disabled = false }) {
  const [activity, setActivity] = useState(null)
  const [ratings, setRatings] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    if (!playerName || disabled) {
      setActivity(null)
      setRatings(null)
      return undefined
    }

    const controller = new AbortController()
    setLoading(true)
    setError('')
    Promise.all([
      fetchBehaviouralActivity({ playerName, signal: controller.signal }),
      fetchBehaviouralRatings({ playerName, signal: controller.signal }),
    ]).then(([nextActivity, nextRatings]) => {
      if (controller.signal.aborted) return
      setActivity(nextActivity)
      setRatings(nextRatings)
    }).catch((requestError) => {
      if (requestError.name !== 'AbortError') {
        setError(requestError.message || 'Unable to load playing patterns.')
      }
    }).finally(() => {
      if (!controller.signal.aborted) setLoading(false)
    })
    return () => controller.abort()
  }, [playerName, disabled])

  return (
    <>
      <section className="games-mode-detail player-analysis-workspace">
        <div className="player-analysis-heading">
          <div>
            <p className="eyebrow">BEHAVIORAL ANALYSIS</p>
            <h2>Patterns</h2>
          </div>
        </div>

        {error ? <p className="player-analysis-error" role="alert">{error}</p> : null}
        {loading ? <div className="player-analysis-loading" aria-live="polite"><span />Calculating playing patterns…</div> : null}
        {!loading && !error && activity && ratings ? (
          <PlayerBehaviouralPanel
            activity={activity}
            ratings={ratings}
          />
        ) : null}
      </section>
      <PlayerMeasuresWorkspace playerName={playerName} disabled={disabled} />
    </>
  )
}
