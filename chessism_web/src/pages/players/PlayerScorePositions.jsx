import { useCallback, useEffect, useId, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import ScorePositionBoard from './ScorePositionBoard'
import PlayerGameExplorerModal from './PlayerGameExplorerModal'
import PlayerGameReviewModal from './PlayerGameReviewModal'
import './player-game-explorer.css'
import { pageStyleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import { fetchPlayerScorePositions } from './playerAnalysisApi'
import { scoreInterval } from './scoreDistribution'

const pendingRequest = { loading: true, data: null, error: '', changed: false }
const positionKey = row => `${row.fen_id}:${row.player_color}`

export default function PlayerScorePositions({ playerName, selection, onClose }) {
  const [retry, setRetry] = useState(0)
  const [color, setColor] = useState(null)
  const [storedRequest, setRequest] = useState(pendingRequest)
  const request = storedRequest.playerName === playerName && storedRequest.selection === selection
    && storedRequest.color === color ? storedRequest : pendingRequest
  const [activeKey, setActiveKey] = useState(null)
  const [gameSelection, setGameSelection] = useState(null)
  const [reviewGameId, setReviewGameId] = useState(null)
  const rows = request.data?.rows || []
  const active = rows.find(row => positionKey(row) === activeKey) || rows[0]
  const panel = useRef(null), close = useRef(onClose), list = useRef(null)
  const paginationController = useRef(null), dataSource = useRef(null)
  const rowNavigation = useRef(null), pointerPosition = useRef(null)
  const listSnapshot = useRef(selection.snapshot)
  const titleId = useId(), previewId = useId()
  close.current = onClose

  useEffect(() => {
    if (gameSelection) return undefined
    const previous = document.activeElement
    const overflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    panel.current?.focus({ preventScroll: true })
    const keydown = event => {
      if (event.target?.closest?.('[data-appearance-tools]')) return
      if (event.key === 'Escape') { event.stopPropagation(); close.current() }
      if (!event.defaultPrevented && !event.altKey && !event.ctrlKey && !event.metaKey
        && !event.target?.closest?.('input, textarea, select, [contenteditable="true"]')) {
        rowNavigation.current?.(event)
      }
      if (event.key !== 'Tab' || !panel.current) return
      const nodes = [...panel.current.querySelectorAll('button:not(:disabled), [tabindex="0"]')]
      const first = nodes[0], last = nodes.at(-1)
      if (event.shiftKey && (document.activeElement === first || document.activeElement === panel.current)) {
        event.preventDefault(); last?.focus()
      } else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus() }
    }
    document.addEventListener('keydown', keydown)
    return () => {
      document.body.style.overflow = overflow
      document.removeEventListener('keydown', keydown)
      if (previous?.isConnected) previous.focus({ preventScroll: true })
    }
  }, [Boolean(gameSelection)])

  useEffect(() => {
    setActiveKey(null)
    dataSource.current = null
    listSnapshot.current = selection.snapshot
    if (list.current) list.current.scrollTop = 0
    const controller = new AbortController()
    const source = { playerName, selection, color }
    setRequest({ ...pendingRequest, ...source })
    const timeout = window.setTimeout(() => {
      controller.abort()
      setRequest({ ...source, loading: false, data: null, error: 'Positions took too long to load. Please retry.', changed: false })
    }, 10000)
    fetchPlayerScorePositions({ playerName, signal: controller.signal, selection: {
      bin_id: selection.bin.id, modes: selection.modes, snapshot: listSnapshot.current, color,
    } }).then(data => {
      if (controller.signal.aborted) return
      listSnapshot.current = data.snapshot
      dataSource.current = source
      setRequest({ ...source, loading: false, data, error: '', changed: false })
    }).catch(error => {
      if (!controller.signal.aborted) setRequest({ ...source, loading: false, data: null, error: error.message || 'Unable to load positions.', changed: error.status === 409 })
    }).finally(() => window.clearTimeout(timeout))
    return () => {
      window.clearTimeout(timeout)
      controller.abort()
      paginationController.current?.abort()
      paginationController.current = null
      dataSource.current = null
    }
  }, [playerName, selection, color, retry])

  const loadMore = useCallback(async () => {
    const cursor = request.data?.next_cursor
    if (!cursor || request.loading || paginationController.current
      || dataSource.current?.playerName !== playerName || dataSource.current?.selection !== selection
      || dataSource.current?.color !== color) return
    const controller = new AbortController()
    paginationController.current = controller
    setRequest(current => ({ ...current, loading: true, error: '', changed: false }))
    const timeout = window.setTimeout(() => {
      if (controller.signal.aborted || paginationController.current !== controller) return
      controller.abort()
      setRequest(current => ({ ...current, loading: false, error: 'More positions took too long to load. Please retry.' }))
    }, 10000)
    try {
      const next = await fetchPlayerScorePositions({ playerName, signal: controller.signal, selection: {
        bin_id: selection.bin.id, modes: selection.modes, snapshot: listSnapshot.current, cursor, color,
      } })
      if (!controller.signal.aborted) {
        listSnapshot.current = next.snapshot
        setRequest(current => {
          const seen = new Set(current.data.rows.map(positionKey))
          return { ...current, loading: false, error: '', changed: false,
            data: { ...next, rows: [...current.data.rows, ...next.rows.filter(row => !seen.has(positionKey(row)))] } }
        })
      }
    } catch (error) {
      if (!controller.signal.aborted) setRequest(current => ({ ...current, loading: false,
        error: error.message || 'Unable to load more positions.', changed: error.status === 409 }))
    } finally {
      window.clearTimeout(timeout)
      if (paginationController.current === controller) paginationController.current = null
    }
  }, [playerName, selection, color, request.data?.next_cursor, request.loading])

  const show = row => setActiveKey(positionKey(row))
  const followPointer = (event, row) => {
    // Scrolling with the keyboard can move another row under a stationary
    // pointer. Only real pointer movement should take over the selection.
    const previous = pointerPosition.current
    pointerPosition.current = { x: event.clientX, y: event.clientY }
    if (!previous || previous.x !== event.clientX || previous.y !== event.clientY) show(row)
  }
  const navigateRows = event => {
    if (!['ArrowUp', 'ArrowDown', 'Home', 'End'].includes(event.key)) return
    event.preventDefault()
    const current = rows.indexOf(active)
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? rows.length - 1
      : Math.max(0, Math.min(rows.length - 1, current + (event.key === 'ArrowUp' ? -1 : 1)))
    const item = list.current?.querySelectorAll('[role="option"]')[next]
    if (!item) return
    show(rows[next])
    item.focus({ preventScroll: true })
    const viewport = list.current.getBoundingClientRect(), box = item.getBoundingClientRect()
    if (box.top < viewport.top) list.current.scrollTop -= viewport.top - box.top
    else if (box.bottom > viewport.bottom) list.current.scrollTop += box.bottom - viewport.bottom
  }
  rowNavigation.current = navigateRows
  const restartList = () => setRetry(value => value + 1)
  const score = value => `${value > 0 ? '+' : ''}${formatNumber(value)}`
  if (gameSelection) return reviewGameId ? <PlayerGameReviewModal gameId={reviewGameId} playerName={playerName}
    startingPosition={gameSelection.position} onBack={() => setReviewGameId(null)} onClose={() => setReviewGameId(null)} />
    : <PlayerGameExplorerModal playerName={playerName} selection={gameSelection}
      onClose={() => setGameSelection(null)} onOpenGame={setReviewGameId} />
  return createPortal(<div {...pageStyleTarget('scorePositions.backdrop')} className="score-positions-backdrop" onClick={event => { if (event.target === event.currentTarget) onClose() }}>
    <section ref={panel} {...pageStyleTarget('scorePositions.panel')} className="score-positions-modal" role="dialog" aria-modal="true"
      aria-labelledby={titleId} tabIndex={-1} aria-busy={request.loading}>
      <header {...pageStyleTarget('scorePositions.header')}>
        <h3 {...pageStyleTarget('scorePositions.title')} id={titleId}>{scoreInterval(selection.bin)}</h3>
        <button {...pageStyleTarget('scorePositions.close')} type="button" onClick={onClose} aria-label="Close position list">×</button>
      </header>
      <div {...pageStyleTarget('scorePositions.filters')} className="score-positions-filters" role="group" aria-label={`${playerName}'s color`}>
        {[[null, 'Both'], ['white', 'White'], ['black', 'Black']].map(([value, label]) => <button
          {...pageStyleTarget(`scorePositions.color.${value || 'both'}`)} key={label} type="button"
          aria-pressed={color === value} onClick={() => setColor(value)}>{label}</button>)}
      </div>
      {request.data?.ranking_pending ? <small {...pageStyleTarget('scorePositions.status.pending')}>
        Showing matching examples. Frequency ranking will be available after Run CUDA summaries.
      </small> : request.data?.summary_stale ? <small {...pageStyleTarget('scorePositions.status.stale')}>
        Showing the last complete frequency summary.
      </small> : null}
      <div {...pageStyleTarget('scorePositions.body')} className="score-positions-body">
        <div {...pageStyleTarget('scorePositions.listPane')} ref={list} className="score-positions-list-pane" onScroll={event => {
          const node = event.currentTarget
          if (!request.error && node.scrollTop > 0 && node.scrollTop + node.clientHeight >= node.scrollHeight - 40) loadMore()
        }}>
          <div {...pageStyleTarget('scorePositions.labels')} className="score-positions-labels">
            <span {...pageStyleTarget('scorePositions.labels.appearances')}>Appearances</span>
            <span {...pageStyleTarget('scorePositions.labels.score')}>Score</span>
          </div>
          <ol {...pageStyleTarget('scorePositions.list')} className="score-positions-list" role="listbox" aria-label="Matching positions">
            {rows.map((row, index) => <li {...pageStyleTarget('scorePositions.row')} key={positionKey(row)}
              role="option" aria-selected={row === active} tabIndex={row === active ? 0 : -1}
              aria-label={`Position ${index + 1}, ${row.appearances ? row.appearances + ' appearances, ' : ''}${playerName} as ${row.player_color}, ${score(row.score_min)} centipawns from the player's perspective.`}
              aria-describedby={row === active ? previewId : undefined}
              onPointerMove={event => followPointer(event, row)} onFocus={() => show(row)} onClick={() => show(row)}>
              <span {...pageStyleTarget('scorePositions.rank')} className="score-positions-rank">#{index + 1}</span>
              <span {...pageStyleTarget('scorePositions.identity')} className="score-positions-identity">FEN {row.fen_id}</span>
              <span {...pageStyleTarget('scorePositions.appearances')}>{row.appearances != null ? formatNumber(row.appearances) : '—'}</span>
              <strong {...pageStyleTarget('scorePositions.row.score')}>{score(row.score_min)}</strong>
            </li>)}
          </ol>
          {request.loading ? <p {...pageStyleTarget(request.data ? 'scorePositions.loadingMore' : 'scorePositions.loading')} role="status">{request.data ? 'Loading more positions…' : 'Loading positions…'}</p> : null}
          {request.error ? <p {...pageStyleTarget('scorePositions.error')} role="alert">{request.error}{' '}
            <button {...pageStyleTarget(request.changed ? 'scorePositions.update' : 'scorePositions.retry')} type="button"
              onClick={request.changed || !request.data ? restartList : loadMore}>{request.changed ? 'Update position list' : 'Retry'}</button>
          </p> : null}
          {!request.loading && !request.error && !request.data?.rows.length ? <p {...pageStyleTarget('scorePositions.empty')}>{request.data?.next_cursor
            ? 'No matches in this batch. Continue to check more positions.' : 'No matching positions.'}</p> : null}
          {!request.loading && !request.error && request.data?.next_cursor ? <button
            {...pageStyleTarget('scorePositions.paging')} type="button" onClick={loadMore}>Load more</button> : null}
        </div>
        <aside {...pageStyleTarget('scorePositions.preview')} className="score-positions-preview" id={previewId} aria-label="Position preview">
          {active ? <>
            <button {...pageStyleTarget('scorePositions.seeGames')} className="score-positions-see-games" type="button" onClick={() => setGameSelection({
              scope: 'position', fen_id: active.fen_id, color: active.player_color, modes: selection.modes,
              analyzed_only: false, position: { fen: active.fen },
            })}>See games</button>
            <ScorePositionBoard fen={active.fen} orientation={active.player_color} />
            <div {...pageStyleTarget('scorePositions.preview.details')} className="score-positions-preview-details">
              <small {...pageStyleTarget('scorePositions.preview.score')} aria-live="polite">{score(active.score_min)} cp</small>
              <small {...pageStyleTarget('scorePositions.preview.player')}>{playerName} · {active.player_color === 'black' ? 'Black' : 'White'}</small>
            </div>
          </> : <span {...pageStyleTarget('scorePositions.preview.empty')}>{request.loading ? 'Loading position…' : 'No position selected'}</span>}
        </aside>
      </div>
    </section>
  </div>, document.body)
}
