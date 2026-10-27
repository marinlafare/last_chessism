import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import PlayerMeasuresPanel from './PlayerMeasuresPanel'
import PlayerScoreDistribution from './PlayerScoreDistribution'
import PlayerBlunderDistribution from './PlayerBlunderDistribution'
import PlayerUnexpectedEndings from './PlayerUnexpectedEndings'
import PlayerAnalysisDisclosure, { useAnalysisVisible } from './PlayerAnalysisDisclosure'
import { fetchStockfishReadiness } from './playerAnalysisApi'
import './player-measures.css'
import './player-game-explorer.css'

export default function PlayerMeasuresWorkspace({ playerName, disabled = false }) {
  return <PlayerAnalysisDisclosure label="Stockfish analysis" target="player.measures"
    buttonTarget="player.measures.toggle" level={2} disabled={disabled}>
    <MeasuresContent key={playerName} playerName={playerName} disabled={disabled} />
  </PlayerAnalysisDisclosure>
}

function MeasuresContent({ playerName, disabled }) {
  const visible = useAnalysisVisible()
  const [quality, setQuality] = useState(null)
  const [qualityError, setQualityError] = useState('')
  const [qualityLoading, setQualityLoading] = useState(false)

  useEffect(() => {
    if (!visible) return undefined
    if (!playerName || disabled) {
      setQuality(null)
      setQualityError('')
      setQualityLoading(false)
      return undefined
    }

    const controller = new AbortController()
    setQualityLoading(true)
    setQualityError('')
    let timer
    const load = async () => {
      try {
        const payload = await fetchStockfishReadiness({ playerName, signal: controller.signal })
        if (controller.signal.aborted) return
        setQuality(current => JSON.stringify(current) === JSON.stringify(payload) ? current : payload)
        setQualityError('')
        setQualityLoading(Boolean(payload.summary_pending))
        // Small saved counters only; no calendar calculations or repairs.
        timer = window.setTimeout(load, 30000)
      } catch (requestError) {
        if (controller.signal.aborted) return
        setQualityError(requestError.message || 'Unable to load Stockfish measures.')
        setQualityLoading(false)
        timer = window.setTimeout(load, 30000)
      }
    }
    load()
    return () => {
      window.clearTimeout(timer)
      controller.abort()
    }
  }, [playerName, disabled, visible])

  return (
    <div className="player-analysis-chart-list">
      <div className="player-stockfish-summary">
        <PlayerScoreDistribution key={`scores:${playerName}`} playerName={playerName} />
        <PlayerBlunderDistribution key={`blunders:${playerName}`} playerName={playerName} />
      </div>
      {qualityError ? <p {...pageStyleTarget('text')} className="player-analysis-error" role="alert">{qualityError}</p> : null}
      <PlayerMeasuresPanel playerName={playerName} quality={quality}
        loading={!disabled && (qualityLoading || (!quality && !qualityError))}
        disabled={disabled || Boolean(qualityError)} />
      <PlayerUnexpectedEndings playerName={playerName} />
    </div>
  )
}
