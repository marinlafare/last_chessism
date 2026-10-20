import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import './player-analysis.css'
import './player-hero-analytics.css'
import PlayerBehaviouralPanel from './PlayerBehaviouralPanel'
import PlayerMeasuresWorkspace from './PlayerMeasuresWorkspace'
import PlayerAnalysisDisclosure from './PlayerAnalysisDisclosure'
import PlayerGameCatalog from './PlayerGameCatalog'
import {
  fetchBehaviouralActivity,
  fetchBehaviouralRatings,
} from './playerAnalysisApi'

export default function PlayerAnalysisWorkspace({ playerName, disabled = false }) {
  return <>
    <PlayerAnalysisDisclosure key={playerName} label="Behavioral analysis" target="player.analysis"
      buttonTarget="player.analysis.toggle" level={2} disabled={disabled}>
      <BehaviouralContent playerName={playerName} disabled={disabled} />
    </PlayerAnalysisDisclosure>
    <PlayerMeasuresWorkspace key={`measures-${playerName}`} playerName={playerName} disabled={disabled} />
  </>
}

function BehaviouralContent({ playerName, disabled }) {
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
    <div className="player-analysis-chart-list">
      {error ? <p {...pageStyleTarget('text')} className="player-analysis-error" role="alert">{error}</p> : null}
      {loading ? <div className="player-analysis-loading" aria-live="polite"><span />Calculating playing patterns…</div> : null}
      {!loading && !error && activity && ratings ? (
        <PlayerBehaviouralPanel
          activity={activity}
          playerName={playerName}
          ratings={ratings}
        />
      ) : null}
      <PlayerAnalysisDisclosure label="Opponents" buttonTarget="player.opponents.toggle">
        <PlayerGameCatalog key={`${playerName}:opponents`} playerName={playerName} kind="opponents" />
      </PlayerAnalysisDisclosure>
      <PlayerAnalysisDisclosure label="Opening repertoire" buttonTarget="player.openings.toggle">
        <PlayerGameCatalog key={`${playerName}:openings`} playerName={playerName} kind="openings" />
      </PlayerAnalysisDisclosure>
    </div>
  )
}
