import { useCallback, useEffect, useId, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import PositionBoard from '../../components/chess/PositionBoard'
import { pageStyleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import { fetchPlayerScorePositions } from './playerAnalysisApi'
import { scoreInterval } from './scoreDistribution'

export default function PlayerScorePositions({ playerName, selection, onClose }) {
  const [retry, setRetry] = useState(0)
  const [request, setRequest] = useState({ loading: true, data: null, error: '', changed: false })
  const [hover, setHover] = useState(null)
  const [boardWidth, setBoardWidth] = useState(() => Math.min(280, window.innerWidth - 60))
  const panel = useRef(null), close = useRef(onClose), list = useRef(null), sentinel = useRef(null)
  const paginationController = useRef(null), dataSource = useRef(null)
  const listSnapshot = useRef(selection.snapshot)
  const titleId = useId(), previewId = useId()
  close.current = onClose

  useEffect(() => {
    const previous = document.activeElement
    const overflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    panel.current?.focus()
    const resize = () => { setHover(null); setBoardWidth(Math.min(280, window.innerWidth - 60)) }
    const keydown = event => {
      if (event.target?.closest?.('[data-appearance-tools]')) return
      if (event.key === 'Escape') { event.stopPropagation(); close.current() }
      if (event.key !== 'Tab' || !panel.current) return
      const nodes = [...panel.current.querySelectorAll('button:not(:disabled), [tabindex="0"]')]
      const first = nodes[0], last = nodes.at(-1)
      if (event.shiftKey && (document.activeElement === first || document.activeElement === panel.current)) {
        event.preventDefault(); last?.focus()
      } else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus() }
    }
    window.addEventListener('resize', resize)
    document.addEventListener('keydown', keydown)
    return () => {
      document.body.style.overflow = overflow
      window.removeEventListener('resize', resize)
      document.removeEventListener('keydown', keydown)
      if (previous?.isConnected) previous.focus({ preventScroll: true })
    }
  }, [])

  useEffect(() => {
    setHover(null)
    dataSource.current = null
    listSnapshot.current = selection.snapshot
    if (list.current) list.current.scrollTop = 0
    const controller = new AbortController()
    setRequest({ loading: true, data: null, error: '', changed: false })
    const timeout = window.setTimeout(() => {
      controller.abort()
      setRequest({ loading: false, data: null, error: 'Positions took too long to load. Please retry.', changed: false })
    }, 60000)
    fetchPlayerScorePositions({ playerName, signal: controller.signal, selection: {
      bin_id: selection.bin.id, modes: selection.modes, snapshot: listSnapshot.current,
    } }).then(data => {
      if (controller.signal.aborted) return
      listSnapshot.current = data.snapshot
      dataSource.current = { playerName, selection }
      setRequest({ loading: false, data, error: '', changed: false })
    }).catch(error => {
      if (!controller.signal.aborted) setRequest({ loading: false, data: null, error: error.message || 'Unable to load positions.', changed: error.status === 409 })
    }).finally(() => window.clearTimeout(timeout))
    return () => {
      window.clearTimeout(timeout)
      controller.abort()
      paginationController.current?.abort()
      paginationController.current = null
      dataSource.current = null
    }
  }, [playerName, selection, retry])

  const loadMore = useCallback(async () => {
    const cursor = request.data?.next_cursor
    if (!cursor || request.loading || paginationController.current
      || dataSource.current?.playerName !== playerName || dataSource.current?.selection !== selection) return
    const controller = new AbortController()
    paginationController.current = controller
    setRequest(current => ({ ...current, loading: true, error: '', changed: false }))
    const timeout = window.setTimeout(() => {
      if (controller.signal.aborted || paginationController.current !== controller) return
      controller.abort()
      setRequest(current => ({ ...current, loading: false, error: 'More positions took too long to load. Please retry.' }))
    }, 60000)
    try {
      const next = await fetchPlayerScorePositions({ playerName, signal: controller.signal, selection: {
        bin_id: selection.bin.id, modes: selection.modes, snapshot: listSnapshot.current, cursor,
      } })
      if (!controller.signal.aborted) {
        listSnapshot.current = next.snapshot
        setRequest(current => ({ loading: false, error: '', changed: false,
          data: { ...next, rows: [...current.data.rows, ...next.rows] } }))
      }
    } catch (error) {
      if (!controller.signal.aborted) setRequest(current => ({ ...current, loading: false,
        error: error.message || 'Unable to load more positions.', changed: error.status === 409 }))
    } finally {
      window.clearTimeout(timeout)
      if (paginationController.current === controller) paginationController.current = null
    }
  }, [playerName, selection, request.data?.next_cursor, request.loading])

  useEffect(() => {
    if (!list.current || !sentinel.current || request.loading || request.error || !request.data?.next_cursor) return undefined
    let active = true
    const observer = new IntersectionObserver(entries => {
      if (active && entries.some(entry => entry.isIntersecting)) loadMore()
    }, { root: list.current, rootMargin: '0px 0px 40px 0px' })
    observer.observe(sentinel.current)
    return () => { active = false; observer.disconnect() }
  }, [loadMore, request.loading, request.error, request.data?.next_cursor])

  const show = row => setHover(row)
  const restartList = () => setRetry(value => value + 1)
  const score = value => `${value > 0 ? '+' : ''}${formatNumber(value)}`
  return createPortal(<div className="score-positions-backdrop" onClick={event => { if (event.target === event.currentTarget) onClose() }}>
    <section ref={panel} {...pageStyleTarget('scorePositions.panel')} className="score-positions-modal" role="dialog" aria-modal="true"
      aria-labelledby={titleId} tabIndex={-1} aria-busy={request.loading}>
      <header>
        <h3 id={titleId}>{scoreInterval(selection.bin)}</h3>
        <button {...pageStyleTarget('scorePositions.close')} type="button" onClick={onClose} aria-label="Close position list">×</button>
      </header>
      <div className="score-positions-body">
        <div ref={list} className="score-positions-list-pane" onScroll={() => setHover(null)}>
          <div className="score-positions-labels"><span>Position</span><span>Appearances ↓</span></div>
          <ol className="score-positions-list" onPointerLeave={() => setHover(null)}>
            {request.data?.rows.map((row, index) => <li {...pageStyleTarget('scorePositions.row')} key={row.fen_id} tabIndex={0}
              aria-label={`Position ${index + 1}, ${formatNumber(row.appearances)} appearances. Focus to preview.`}
              aria-describedby={hover?.fen_id === row.fen_id ? previewId : undefined}
              onPointerEnter={() => show(row)} onFocus={() => show(row)} onBlur={() => setHover(null)}>
              <span className="score-positions-rank">#{index + 1}</span>
              <span className="score-positions-identity">FEN {row.fen_id}</span>
              <strong>{formatNumber(row.appearances)}</strong>
            </li>)}
          </ol>
          <div ref={sentinel} className="score-positions-sentinel" aria-hidden="true" />
          {request.loading ? <p role="status">{request.data ? 'Loading more positions…' : 'Loading positions…'}</p> : null}
          {request.error ? <p role="alert">{request.error}{' '}
            <button {...pageStyleTarget('scorePositions.paging')} type="button"
              onClick={request.changed || !request.data ? restartList : loadMore}>{request.changed ? 'Update position list' : 'Retry'}</button>
          </p> : null}
          {!request.loading && !request.error && !request.data?.rows.length ? <p>No matching positions.</p> : null}
        </div>
        <aside className="score-positions-preview" id={previewId} aria-label="Position preview" aria-live="polite">
          {hover ? <>
            <PositionBoard fen={hover.fen} width={boardWidth} />
            <small>{score(hover.score_min)}{hover.score_min !== hover.score_max ? ` to ${score(hover.score_max)}` : ''} cp · {formatNumber(hover.appearances)} appearances</small>
          </> : <span>Hover a position</span>}
        </aside>
      </div>
    </section>
  </div>, document.body)
}
