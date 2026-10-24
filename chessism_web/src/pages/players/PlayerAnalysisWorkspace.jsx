import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import './player-analysis.css'
import './player-hero-analytics.css'
import PlayerBehaviouralPanel from './PlayerBehaviouralPanel'
import PlayerMeasuresWorkspace from './PlayerMeasuresWorkspace'
import PlayerAnalysisDisclosure, { useAnalysisVisible } from './PlayerAnalysisDisclosure'
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
    <PlayerAnalysisDisclosure key={`compare-${playerName}`} label="Compare" target="player.compare.panel"
      buttonTarget="player.compare.toggle" level={2} disabled={disabled}>
      <p {...pageStyleTarget('player.compare.text')} className="player-compare-placeholder">Not yet implemented.</p>
    </PlayerAnalysisDisclosure>
  </>
}

function BehaviouralContent({ playerName, disabled }) {
  const visible = useAnalysisVisible()
  const [activity, setActivity] = useState(null)
  const [ratings, setRatings] = useState(null)
  const [ratingRequest, setRatingRequest] = useState(0)
  const [ratingLoading, setRatingLoading] = useState(false)
  const [ratingError, setRatingError] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    if (!playerName || disabled || !visible) return undefined

    const controller = new AbortController()
    setLoading(true)
    setError('')
    let timer
    let inFlight = false
    const load = () => {
      if (inFlight || controller.signal.aborted) return
      inFlight = true
      window.clearTimeout(timer)
      fetchBehaviouralActivity({ playerName, signal: controller.signal }).then(nextActivity => {
      if (controller.signal.aborted) return
      setActivity(nextActivity)
      setError('')
      timer = window.setTimeout(load, nextActivity.summary_stale ? 2000 : 30000)
    }).catch((requestError) => {
      if (requestError.name !== 'AbortError') {
        setError(requestError.message || 'Unable to load playing patterns.')
        timer = window.setTimeout(load, 30000)
      }
    }).finally(() => {
      inFlight = false
      if (!controller.signal.aborted) setLoading(false)
    })
    }
    load()
    const focus = () => { window.clearTimeout(timer); load() }
    window.addEventListener('focus', focus)
    return () => { controller.abort(); window.clearTimeout(timer); window.removeEventListener('focus', focus) }
  }, [playerName, disabled, visible])

  useEffect(() => {
    if (!ratingRequest || !visible || disabled) return undefined
    const controller = new AbortController()
    let timer
    setRatingLoading(true)
    setRatingError('')
    const load = () => fetchBehaviouralRatings({ playerName, signal: controller.signal }).then(payload => {
      if (controller.signal.aborted) return
      setRatings(payload)
      if (payload.summary_stale) timer = window.setTimeout(load, 2000)
    }).catch(failure => {
      if (failure.name !== 'AbortError') setRatingError(failure.message)
    }).finally(() => { if (!controller.signal.aborted) setRatingLoading(false) })
    load()
    return () => { controller.abort(); window.clearTimeout(timer) }
  }, [playerName, ratingRequest, disabled, visible, activity?.summary_revision])

  return (
    <div className="player-analysis-chart-list">
      <PlayerBehaviouralPanel
        activity={activity}
        playerName={playerName}
        ratings={ratings}
        pending={loading || !activity}
        onRatingOpen={() => setRatingRequest(value => value + 1)}
        ratingLoading={ratingLoading}
        ratingError={ratingError}
        error={error}
      />
      {error ? <p {...pageStyleTarget('text')} className="player-analysis-error" role="alert">{error}</p> : null}
      <PlayerAnalysisDisclosure label="Opponents" buttonTarget="player.opponents.toggle">
        <PlayerGameCatalog key={`${playerName}:opponents`} playerName={playerName} kind="opponents" summaryRevision={activity?.summary_revision} />
      </PlayerAnalysisDisclosure>
      <PlayerAnalysisDisclosure label="Opening repertoire" buttonTarget="player.openings.toggle">
        <PlayerGameCatalog key={`${playerName}:openings`} playerName={playerName} kind="openings" summaryRevision={activity?.summary_revision} />
      </PlayerAnalysisDisclosure>
    </div>
  )
}
