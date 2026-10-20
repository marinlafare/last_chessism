import { useEffect, useId, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import PositionBoard from '../../components/chess/PositionBoard'
import { pageStyleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import { fetchPlayerScorePositions } from './playerAnalysisApi'
import { scoreInterval } from './scoreDistribution'

export default function PlayerScorePositions({ playerName, selection, onClose }) {
  const [page, setPage] = useState(0)
  const [cursors, setCursors] = useState([null])
  const [retry, setRetry] = useState(0)
  const [request, setRequest] = useState({ loading: true, data: null, error: '', changed: false })
  const [hover, setHover] = useState(null)
  const [boardWidth, setBoardWidth] = useState(() => Math.min(280, window.innerWidth - 60))
  const cache = useRef(new Map()), panel = useRef(null), close = useRef(onClose)
  const listSnapshot = useRef(selection.snapshot)
  const titleId = useId(), previewId = useId()
  close.current = onClose
  const cursor = cursors[page]

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
    if (cache.current.has(page)) {
      listSnapshot.current = cache.current.get(page).snapshot
      setRequest({ loading: false, data: cache.current.get(page), error: '', changed: false })
      return undefined
    }
    const controller = new AbortController()
    setRequest({ loading: true, data: null, error: '', changed: false })
    const timeout = window.setTimeout(() => {
      controller.abort()
      setRequest({ loading: false, data: null, error: 'Positions took too long to load. Please retry.', changed: false })
    }, 60000)
    fetchPlayerScorePositions({ playerName, signal: controller.signal, selection: {
      bin_id: selection.bin.id, modes: selection.modes, snapshot: listSnapshot.current, cursor,
    } }).then(data => {
      if (controller.signal.aborted) return
      listSnapshot.current = data.snapshot
      cache.current.set(page, data)
      if (cache.current.size > 20) cache.current.delete(cache.current.keys().next().value)
      setRequest({ loading: false, data, error: '', changed: false })
    }).catch(error => {
      if (!controller.signal.aborted) setRequest({ loading: false, data: null, error: error.message || 'Unable to load positions.', changed: error.status === 409 })
    }).finally(() => window.clearTimeout(timeout))
    return () => { window.clearTimeout(timeout); controller.abort() }
  }, [playerName, selection, page, cursor, retry])

  const show = row => { if (!request.loading) setHover(row) }
  const restartList = () => {
    cache.current.clear()
    listSnapshot.current = selection.snapshot
    setCursors([null])
    setPage(0)
    setRetry(value => value + 1)
  }
  const score = value => `${value > 0 ? '+' : ''}${formatNumber(value)}`
  return createPortal(<div className="score-positions-backdrop" onClick={event => { if (event.target === event.currentTarget) onClose() }}>
    <section ref={panel} {...pageStyleTarget('scorePositions.panel')} className="score-positions-modal" role="dialog" aria-modal="true"
      aria-labelledby={titleId} tabIndex={-1} aria-busy={request.loading}>
      <header>
        <h3 id={titleId}>{scoreInterval(selection.bin)}</h3>
        <button {...pageStyleTarget('scorePositions.close')} type="button" onClick={onClose} aria-label="Close position list">×</button>
      </header>
      <div className="score-positions-body">
        <div className="score-positions-list-pane">
          <div className="score-positions-labels"><span>Position</span><span>Appearances ↓</span></div>
          {request.loading ? <p role="status">Loading positions…</p> : null}
          {request.error ? <p role="alert">{request.error}{' '}
            <button {...pageStyleTarget('scorePositions.paging')} type="button" onClick={request.changed ? restartList : () => setRetry(value => value + 1)}>{request.changed ? 'Update position list' : 'Retry'}</button>
          </p> : null}
          <ol className="score-positions-list" start={page * 10 + 1} onPointerLeave={() => setHover(null)} onScroll={() => setHover(null)}>
            {request.data?.rows.map((row, index) => <li {...pageStyleTarget('scorePositions.row')} key={row.fen_id} tabIndex={0}
              aria-label={`Position ${page * 10 + index + 1}, ${formatNumber(row.appearances)} appearances. Focus to preview.`}
              aria-describedby={hover?.fen_id === row.fen_id ? previewId : undefined}
              onPointerEnter={() => show(row)} onFocus={() => show(row)} onBlur={() => setHover(null)}>
              <span className="score-positions-rank">#{page * 10 + index + 1}</span>
              <span className="score-positions-identity">FEN {row.fen_id}</span>
              <strong>{formatNumber(row.appearances)}</strong>
            </li>)}
          </ol>
          {!request.loading && !request.error && !request.data?.rows.length ? <p>No matching positions.</p> : null}
        </div>
        <aside className="score-positions-preview" id={previewId} aria-label="Position preview" aria-live="polite">
          {hover ? <>
            <PositionBoard fen={hover.fen} width={boardWidth} />
            <small>{score(hover.score_min)}{hover.score_min !== hover.score_max ? ` to ${score(hover.score_max)}` : ''} cp · {formatNumber(hover.appearances)} appearances</small>
          </> : <span>Hover a position</span>}
        </aside>
      </div>
      <footer>
        <button {...pageStyleTarget('scorePositions.paging')} type="button" disabled={!page || request.loading} onClick={() => setPage(value => value - 1)}>Previous</button>
        <span>{page + 1}</span>
        <button {...pageStyleTarget('scorePositions.paging')} type="button" disabled={!request.data?.next_cursor || request.loading} onClick={() => {
          setCursors(current => [...current.slice(0, page + 1), request.data.next_cursor])
          setPage(value => value + 1)
        }}>Next 10</button>
      </footer>
    </section>
  </div>, document.body)
}
