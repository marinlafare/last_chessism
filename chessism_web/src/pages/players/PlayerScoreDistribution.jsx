import { useEffect, useMemo, useRef, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import { fetchPlayerScoreDistribution } from './playerAnalysisApi'
import { useAnalysisVisible } from './PlayerAnalysisDisclosure'
import PlayerModeToggles, { PLAYER_ANALYSIS_MODES } from './PlayerModeToggles'
import PlayerScorePositions from './PlayerScorePositions'
import useSummaryChartWidth from './useSummaryChartWidth'
import { combineScoreModes, scoreInterval, SCORE_ENDINGS } from './scoreDistribution'
import './player-score-distribution.css'
import './player-chart-hover.css'

const LEFT = 64, TOP = 12, BOTTOM = 104, HEIGHT = 144
const tickFormat = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 })

export default function PlayerScoreDistribution({ playerName }) {
  const visible = useAnalysisVisible()
  const [activeModes, setActiveModes] = useState(() => new Set(PLAYER_ANALYSIS_MODES))
  const [request, setRequest] = useState({ data: null, loading: false, error: '' })
  const [progress, setProgress] = useState(null)
  const [retry, setRetry] = useState(0)
  const [selection, setSelection] = useState(null)
  const completed = useRef(null)
  const chart = useSummaryChartWidth()
  const modes = useMemo(() => PLAYER_ANALYSIS_MODES.filter(mode => activeModes.has(mode)), [activeModes])
  const availableModes = useMemo(() => new Set(PLAYER_ANALYSIS_MODES.filter(mode => (
    Number(request.data?.by_mode?.[mode]?.endings?.games || 0) > 0
  ))), [request.data])
  const totals = useMemo(() => combineScoreModes(request.data, modes), [request.data, modes])

  useEffect(() => {
    if (!visible) return undefined
    if (!completed.current?.managed && completed.current?.key === `${playerName}:${retry}` && Date.now() - completed.current.at < 120000) return undefined
    const controller = new AbortController()
    let pollTimer
    setProgress(null)
    setRequest(current => ({ ...current, loading: true, error: '' }))
    const timeout = window.setTimeout(() => {
      controller.abort()
      setRequest(current => ({ ...current, loading: false, error: 'Score distribution took too long to load. Please retry.' }))
    }, 300000)
    const options = { playerName, signal: controller.signal,
      onProgress: value => { if (!controller.signal.aborted) setProgress(value) } }
    fetchPlayerScoreDistribution(options).then(async data => {
      if (controller.signal.aborted) return
      if (data.stale && !data.summary_managed) {
        // Keep the previous complete histogram interactive while its
        // replacement is calculated. Never blank the chart during refresh.
        setRequest({ data, loading: true, error: '' })
        data = await fetchPlayerScoreDistribution({ ...options, refresh: true })
        if (controller.signal.aborted) return
      }
      completed.current = { key: `${playerName}:${retry}`, at: Date.now(), managed: data.summary_managed }
      setProgress(null)
      setRequest({ data, loading: false, error: '' })
      if (data.summary_managed) pollTimer = window.setTimeout(() => setRetry(value => value + 1), 30000)
    }).catch(error => {
      if (!controller.signal.aborted) setRequest(current => ({ ...current, loading: false, error: error.message || 'Unable to load score distribution.' }))
    }).finally(() => window.clearTimeout(timeout))
    return () => { window.clearTimeout(timeout); window.clearTimeout(pollTimer); controller.abort() }
  }, [playerName, retry, visible])

  useEffect(() => { if (!visible) setSelection(null) }, [visible])
  const toggleMode = mode => {
    setSelection(null)
    setActiveModes(current => {
      const next = new Set(current)
      if (next.has(mode)) next.delete(mode)
      else next.add(mode)
      return next
    })
  }
  const refresh = () => { setSelection(null); setRetry(value => value + 1) }
  const maximum = Math.max(1, ...totals.counts)
  const magnitude = 10 ** Math.floor(Math.log10(maximum))
  const ceiling = Math.ceil(maximum / magnitude) * magnitude
  const ticks = [...new Set([0, Math.round(ceiling / 2), ceiling])]
  const right = Math.max(LEFT + 1, chart.width - 18)
  const slot = (right - LEFT) / 13
  const y = count => BOTTOM - count / ceiling * (BOTTOM - TOP)
  const openBin = bin => {
    if (!totals.counts[bin.id]) return
    setSelection({ bin, modes, snapshot: request.data.snapshot })
  }
  const endingDescription = `Recorded game endings in the selected modes, including games without engine analysis.${totals.endings.stalemates_unattributed ? ` ${formatNumber(totals.endings.stalemates_unattributed)} stalemates lack a reliable final move and cannot be attributed.` : ''}`
  const progressText = progress?.total_games != null
    ? `${formatNumber(progress.completed_games)} / ${formatNumber(progress.total_games)} games`
    : progress?.phase === 'queued' ? 'Waiting for the current calculation…' : ''

  return <section {...pageStyleTarget('player.scores.chart')} className="player-score-distribution" aria-label="Score distribution" aria-busy={request.loading}>
    <div className="score-distribution-heading">
      <div className="score-distribution-controls">
        <span {...pageStyleTarget('label')} className="stockfish-summary-title">Score distribution</span>
        <PlayerModeToggles activeModes={activeModes} availableModes={availableModes}
          label="Score distribution game modes" styleId="player.scores.modes" onToggle={toggleMode} />
      </div>
      {request.loading && request.data ? <small className="score-distribution-refresh" role="status">Updating… {progressText}</small> : null}
      {request.data && !request.data.summary_pending ? <div className="score-distribution-endings" title={endingDescription}>
        {SCORE_ENDINGS.map(([key, label]) => <div key={key}>
          <span>{label}</span><strong>{formatNumber(totals.endings[key] || 0)}</strong>
        </div>)}
      </div> : null}
    </div>
    {request.loading && !request.data ? <p className="score-distribution-message" role="status">Calculating scored positions… {progressText}</p> : null}
    {request.data?.summary_pending ? <p {...pageStyleTarget('player.scores.status')} role="status">Summary queued. It will appear here automatically after background preparation.</p> : request.data?.summary_managed && request.data.stale ? <p {...pageStyleTarget('player.scores.status')} role="status">Showing the last complete summary; an update is queued.</p> : null}
    {request.error ? <p role="alert">{request.error}{' '}<button {...pageStyleTarget('player.scores.action')} type="button" onClick={refresh}>Retry</button></p> : null}
    {request.data && !request.data.summary_pending ? <div className="score-distribution-scroll"><div className="score-distribution-canvas">
      <svg ref={chart.ref} viewBox={`0 0 ${chart.width} ${HEIGHT}`} role="group" aria-label="Scored position occurrences by player-perspective centipawn score">
        <desc>Each repeated position counts again. Positive favors {playerName}. {formatNumber(totals.cp)} centipawn occurrences plotted; {formatNumber(totals.unscored)} unscored, {formatNumber(totals.mates)} mate evaluations and {formatNumber(totals.tablebases)} tablebase evaluations excluded. Select a bar to browse positions.</desc>
        {ticks.map(tick => <g key={tick} className="score-distribution-grid" aria-hidden="true">
          <line x1={LEFT} x2={right} y1={y(tick)} y2={y(tick)} />
          <text x={LEFT - 9} y={y(tick) + 4} textAnchor="end">{tickFormat.format(tick)}</text>
        </g>)}
        <text className="score-distribution-axis" x="16" y={(TOP + BOTTOM) / 2} textAnchor="middle" transform={`rotate(-90 16 ${(TOP + BOTTOM) / 2})`}>Positions</text>
        {request.data.bins.map((bin, index) => {
          const count = totals.counts[index], center = LEFT + slot * (index + 0.5)
          return <g key={bin.id} className={`score-distribution-column ${index < 6 ? 'negative' : index > 6 ? 'positive' : 'equal'}`}
            role="button" tabIndex={count ? 0 : -1} aria-disabled={!count}
            aria-label={`${scoreInterval(bin)}: ${formatNumber(count)} positions. Browse positions.`}
            onClick={() => openBin(bin)} onKeyDown={event => {
              if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); openBin(bin) }
            }}>
            <title>{`${scoreInterval(bin)}\n${formatNumber(count)} positions`}</title>
            {count > 0 ? <rect className="score-distribution-hit" x={center - slot * .42} y={Math.min(y(count) - 4, BOTTOM - 14)} width={slot * .84} height={Math.max(BOTTOM - y(count) + 4, 14)} /> : null}
            <rect className="score-distribution-bar" x={center - slot * .34} y={y(count)} width={slot * .68} height={BOTTOM - y(count)} rx="2" />
            <text x={center} y={BOTTOM + 16} textAnchor="middle">{bin.label}</text>
          </g>
        })}
        {!totals.cp ? <text x={(LEFT + right) / 2} y={(TOP + BOTTOM) / 2} textAnchor="middle">{modes.length ? 'No centipawn-scored positions in this selection.' : 'Select a game type.'}</text> : null}
        <text x={(LEFT + right) / 2} y={HEIGHT - 4} textAnchor="middle">Score (centipawns)</text>
      </svg>
    </div></div> : null}
    {request.data?.summary_managed && !request.data.summary_pending && !request.data.mate_distances_pending ? <div {...pageStyleTarget('player.scores.mates')} className="score-mate-summary" aria-label="Mate evaluations by distance">
      <div {...pageStyleTarget('player.scores.mates.row')} className="score-mate-row">
        <span {...pageStyleTarget('player.scores.mates.label')}>Mate distance</span>
        <span {...pageStyleTarget('player.scores.mates.label')}>For {playerName}</span>
        <span {...pageStyleTarget('player.scores.mates.label')}>Against {playerName}</span>
      </div>
      {totals.mateDistances.map(row => <div {...pageStyleTarget('player.scores.mates.row')} className="score-mate-row" key={row.moves ?? 'unknown'}>
        <span {...pageStyleTarget('player.scores.mates.label')}>{row.moves === null ? 'Distance unavailable' : row.moves === 0 ? 'Checkmate' : `Mate in ${row.moves}`}</span>
        <strong {...pageStyleTarget('player.scores.mates.for')}>{formatNumber(row.for)}</strong>
        <strong {...pageStyleTarget('player.scores.mates.against')}>{formatNumber(row.against)}</strong>
      </div>)}
      <small {...pageStyleTarget('player.scores.status')}>Position occurrences · {formatNumber(totals.unscored)} unscored · {formatNumber(totals.tablebases)} tablebase</small>
    </div> : null}
    {selection ? <PlayerScorePositions key={JSON.stringify(selection)} playerName={playerName} selection={selection}
      onClose={() => setSelection(null)} /> : null}
  </section>
}
