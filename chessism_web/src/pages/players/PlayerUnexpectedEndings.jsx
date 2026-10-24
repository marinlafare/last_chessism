import { useEffect, useMemo, useRef, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'
import { fetchPlayerUnexpectedEndings } from './playerAnalysisApi'
import PlayerAnalysisDisclosure, { useAnalysisVisible } from './PlayerAnalysisDisclosure'
import PlayerModeToggles, { PLAYER_ANALYSIS_MODES } from './PlayerModeToggles'
import PlayerGameExplorerModal from './PlayerGameExplorerModal'
import PlayerGameReviewModal from './PlayerGameReviewModal'
import UnexpectedEndingsCharts from './UnexpectedEndingsCharts'
import './player-unexpected-endings.css'

const STYLE = 'player.endings'

export default function PlayerUnexpectedEndings({ playerName }) {
  return <PlayerAnalysisDisclosure label="Unexpected endings" buttonTarget={`${STYLE}.toggle`}>
    <EndingsContent key={playerName} playerName={playerName} />
  </PlayerAnalysisDisclosure>
}

function EndingsContent({ playerName }) {
  const visible = useAnalysisVisible()
  const [activeModes, setActiveModes] = useState(() => new Set(PLAYER_ANALYSIS_MODES))
  const [request, setRequest] = useState({ data: null, loading: false, error: '' })
  const [retry, setRetry] = useState(0)
  const [selection, setSelection] = useState(null)
  const [reviewGameId, setReviewGameId] = useState(null)
  const completed = useRef(null)
  const modes = PLAYER_ANALYSIS_MODES.filter(mode => activeModes.has(mode))
  const availableModes = useMemo(() => new Set(PLAYER_ANALYSIS_MODES.filter(mode =>
    Object.values(request.data?.by_mode?.[mode] || {}).some(item => item.eligible_games > 0))), [request.data])

  useEffect(() => {
    if (!visible || !playerName) return undefined
    const key = `v2:${playerName}:${retry}`
    if (completed.current?.key === key && Date.now() - completed.current.at < 60000) return undefined
    const controller = new AbortController()
    setRequest(current => ({ ...current, loading: true, error: '' }))
    const timeout = window.setTimeout(() => {
      controller.abort()
      setRequest(current => ({ ...current, loading: false, error: 'Reading final positions took too long. Please retry.' }))
    }, 25000)
    fetchPlayerUnexpectedEndings({ playerName, signal: controller.signal }).then(data => {
      if (controller.signal.aborted) return
      if (data.version !== 2) throw new Error('The updated ending charts are awaiting the API update. Please retry shortly.')
      completed.current = { key, at: Date.now() }
      setRequest({ data, loading: false, error: '' })
    }).catch(error => {
      if (!controller.signal.aborted) setRequest(current => ({ ...current, loading: false,
        error: error.status === 404 && error.message === 'Not Found'
          ? 'Unexpected endings is awaiting the API update.'
          : error.message || 'Unable to load unexpected endings.' }))
    }).finally(() => window.clearTimeout(timeout))
    return () => { window.clearTimeout(timeout); controller.abort() }
  }, [playerName, retry, visible])

  useEffect(() => { if (!visible) { setSelection(null); setReviewGameId(null) } }, [visible])
  const toggleMode = mode => {
    setSelection(null)
    setReviewGameId(null)
    setActiveModes(current => {
      const next = new Set(current)
      if (next.has(mode)) next.delete(mode)
      else next.add(mode)
      return next
    })
  }
  const data = request.data?.version === 2 ? request.data : null

  return <section {...pageStyleTarget(`${STYLE}.panel`)} className="player-unexpected-endings" aria-busy={request.loading}>
    <div className="unexpected-endings-controls">
      <PlayerModeToggles activeModes={activeModes} availableModes={availableModes} onToggle={toggleMode}
        styleId={`${STYLE}.modes`} label="Unexpected endings game modes" />
    </div>
    {request.loading ? <p {...pageStyleTarget(`${STYLE}.text`)} role="status">
      {request.data ? 'Updating final-position statistics…' : 'Reading final positions…'}</p> : null}
    {request.error ? <p className="player-analysis-error" role="alert">{request.error}{' '}
      <button {...pageStyleTarget(`${STYLE}.action`)} type="button" onClick={() => setRetry(value => value + 1)}>Retry</button>
    </p> : null}
    {data ? <UnexpectedEndingsCharts data={data} modes={modes} disabled={request.loading} onSelect={setSelection} /> : null}
    {!reviewGameId ? <PlayerGameExplorerModal playerName={playerName} selection={selection}
      onClose={() => setSelection(null)} onOpenGame={setReviewGameId} /> : null}
    <PlayerGameReviewModal gameId={reviewGameId} playerName={playerName} onBack={() => setReviewGameId(null)}
      onClose={() => { setReviewGameId(null); setSelection(null) }} />
  </section>
}
