import { useEffect, useMemo, useRef, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import { fetchPlayerBlunderDistribution } from './playerAnalysisApi'
import { useAnalysisVisible } from './PlayerAnalysisDisclosure'
import PlayerModeToggles, { PLAYER_ANALYSIS_MODES } from './PlayerModeToggles'
import PlayerGameExplorerModal from './PlayerGameExplorerModal'
import PlayerGameReviewModal from './PlayerGameReviewModal'
import useSummaryChartWidth from './useSummaryChartWidth'
import './player-blunder-distribution.css'

const LEFT = 58, TOP = 12, BOTTOM = 104, HEIGHT = 144
const interval = bin => `${bin.label} blunder${bin.id === 1 ? '' : 's'}`
// Retain the existing style keys so saved appearance overrides survive replacement.
const STYLE = 'player.accuracyDistribution'

export default function PlayerBlunderDistribution({ playerName }) {
  const visible = useAnalysisVisible()
  const chart = useSummaryChartWidth()
  const [activeModes, setActiveModes] = useState(() => new Set(PLAYER_ANALYSIS_MODES))
  const [request, setRequest] = useState({ data: null, error: '', loading: false })
  const [retry, setRetry] = useState(0)
  const [selection, setSelection] = useState(null)
  const [reviewGameId, setReviewGameId] = useState(null)
  const completed = useRef(null)
  const modes = PLAYER_ANALYSIS_MODES.filter(mode => activeModes.has(mode))
  const availableModes = useMemo(() => new Set(PLAYER_ANALYSIS_MODES.filter(mode =>
    Number(request.data?.by_mode?.[mode]?.games_count || 0) > 0)), [request.data])
  const counts = (request.data?.bins || []).map((_, index) => modes.reduce((sum, mode) =>
    sum + Number(request.data?.by_mode?.[mode]?.counts[index] || 0), 0))
  const total = counts.reduce((sum, count) => sum + count, 0)

  useEffect(() => {
    if (!visible || !playerName) return undefined
    const key = `${playerName}:${retry}`
    if (completed.current?.key === key && Date.now() - completed.current.at < 60000) return undefined
    const controller = new AbortController()
    setRequest(current => ({ ...current, loading: true, error: '' }))
    const timeout = window.setTimeout(() => {
      controller.abort()
      setRequest(current => ({ ...current, loading: false, error: 'Reading blunder summaries took too long. Please retry.' }))
    }, 20000)
    fetchPlayerBlunderDistribution({ playerName, signal: controller.signal }).then(data => {
      if (controller.signal.aborted) return
      completed.current = { key, at: Date.now() }
      setRequest({ data, loading: false, error: '' })
    }).catch(error => {
      if (!controller.signal.aborted) setRequest(current => ({ ...current, loading: false,
        error: error.status === 404 && error.message === 'Not Found'
          ? 'Blunders per game is awaiting the API update.'
          : error.message || 'Unable to load blunders per game.' }))
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
  const openBin = bin => {
    if (counts[bin.id]) setSelection({ scope: 'blunders', blunder_bin: bin.id,
      modes, analyzed_only: true, label: interval(bin) })
  }
  const maximum = Math.max(1, ...counts)
  const step = 10 ** Math.floor(Math.log10(maximum))
  const ceiling = Math.ceil(maximum / step) * step
  const ticks = [...new Set([0, Math.round(ceiling / 2), ceiling])]
  const right = Math.max(LEFT + 1, chart.width - 12)
  const slot = (right - LEFT) / 7
  const y = value => BOTTOM - value / ceiling * (BOTTOM - TOP)

  return <section {...pageStyleTarget(`${STYLE}.chart`)} className="player-blunder-distribution"
    aria-label="Blunders per game" aria-busy={request.loading}>
    <div className="blunder-distribution-heading">
      <span {...pageStyleTarget('label')} className="stockfish-summary-title">Blunders per game</span>
      <PlayerModeToggles activeModes={activeModes} availableModes={availableModes}
        label="Blunders per game modes" styleId={`${STYLE}.modes`} onToggle={toggleMode} />
    </div>
    {request.loading && !request.data ? <p className="blunder-distribution-message" role="status">Loading saved blunders…</p> : null}
    {request.loading && request.data ? <small role="status">Updating…</small> : null}
    {request.error ? <p className="blunder-distribution-error" role="alert">{request.error}{' '}
      <button {...pageStyleTarget(`${STYLE}.action`)} type="button" onClick={() => setRetry(value => value + 1)}>Retry</button>
    </p> : null}
    {request.data ? <div className="blunder-distribution-plot">
      <small {...pageStyleTarget('text')} className="blunder-distribution-total">{formatNumber(total)} games</small>
      <svg ref={chart.ref} viewBox={`0 0 ${chart.width} ${HEIGHT}`} role="group" aria-label="Blunders per game: zero through five, then six or more">
        <desc>Only fully analyzed games with saved blunder counts and measured player moves. Missing summaries are excluded, not zero. Zero means no detected blunders, not perfect play. Select a bar to browse games, newest first.</desc>
        {ticks.map(tick => <g className="blunder-distribution-grid" key={tick} aria-hidden="true">
          <line x1={LEFT} x2={right} y1={y(tick)} y2={y(tick)} />
          <text x={LEFT - 8} y={y(tick) + 4} textAnchor="end">{formatNumber(tick)}</text>
        </g>)}
        <text x="13" y={(TOP + BOTTOM) / 2} textAnchor="middle" transform={`rotate(-90 13 ${(TOP + BOTTOM) / 2})`}>Games</text>
        {request.data.bins.map((bin, index) => {
          const count = counts[index], center = LEFT + slot * (index + .5)
          const description = `${interval(bin)} · ${formatNumber(count)} games · ${total ? (count / total * 100).toFixed(1) : '0.0'}%`
          return <g className="blunder-distribution-column" key={bin.id} role="button" tabIndex={count ? 0 : -1}
            aria-disabled={!count} aria-label={`${description}. Browse games.`} onClick={() => openBin(bin)}
            onKeyDown={event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); openBin(bin) } }}>
            <title>{description}</title>
            {count > 0 ? <rect className="blunder-distribution-hit" x={center - slot * .46} y={Math.min(y(count), BOTTOM - 14)}
              width={slot * .92} height={Math.max(BOTTOM - y(count), 14)} /> : null}
            <rect className="blunder-distribution-bar" x={center - slot * .39} y={y(count)} width={slot * .78} height={BOTTOM - y(count)} rx="2" />
          </g>
        })}
        {request.data.bins.map((bin, index) => <text key={bin.id} x={LEFT + slot * (index + .5)}
          y={BOTTOM + 15} textAnchor="middle">{bin.label}</text>)}
        <text x={(LEFT + right) / 2} y={HEIGHT - 4} textAnchor="middle">Blunders</text>
        {!total ? <text x={(LEFT + right) / 2} y={(TOP + BOTTOM) / 2} textAnchor="middle">
          {modes.length ? 'No saved blunder counts in this selection.' : 'Select a game type.'}
        </text> : null}
      </svg>
    </div> : null}
    {!reviewGameId ? <PlayerGameExplorerModal playerName={playerName} selection={selection}
      onClose={() => setSelection(null)} onOpenGame={setReviewGameId} /> : null}
    <PlayerGameReviewModal gameId={reviewGameId} playerName={playerName} onBack={() => setReviewGameId(null)}
      onClose={() => { setReviewGameId(null); setSelection(null) }} />
  </section>
}
