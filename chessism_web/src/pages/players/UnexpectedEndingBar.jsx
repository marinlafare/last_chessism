import { useEffect, useId, useRef, useState } from 'react'
import { formatNumber } from '../../utils/formatters'
import useSummaryChartWidth from './useSummaryChartWidth'

const NARROW_PIXELS = 24
const HOVER_MARGIN = 18
const ZOOM_PIXELS = 112

export default function UnexpectedEndingBar({ row, bands, modes, disabled, onSelect, formatPercent }) {
  const chart = useSummaryChartWidth()
  const [zoomId, setZoomId] = useState(null)
  const root = useRef(null)
  const closeTimer = useRef(null)
  const touch = useRef(false)
  const skipFocusZoom = useRef(false)
  const popupId = useId()
  let offset = 0
  const segments = bands.flatMap(band => {
    const count = row.counts[`${band.id}_games`]
    if (!count || !row.counts.assessable_games) return []
    const percent = count / row.counts.assessable_games * 100
    const segment = { ...band, count, percent, start: offset, end: offset + percent }
    offset += percent
    return [segment]
  })
  const narrow = segment => segment.percent / 100 * chart.width < NARROW_PIXELS
  const smallest = segments.reduce((minimum, segment) => !minimum || segment.count < minimum.count ? segment : minimum, null)
  // Each bar gets its own scale, always determined by its smallest nonzero
  // band. A nearby, larger band must not leave the rarest band tiny in the lens.
  const selected = zoomId !== null && smallest && narrow(smallest) ? smallest : null
  const zoomWidth = Math.min(320, chart.width)
  const plotWidth = Math.max(1, zoomWidth - 22)
  // Center the cropped window on the smallest band and enlarge it to 112px
  // (half the available lens width on small screens). Original shares stay true.
  const span = selected ? Math.min(100, selected.percent * plotWidth / Math.min(ZOOM_PIXELS, plotWidth / 2)) : 100
  const center = selected ? (selected.start + selected.end) / 2 : 50
  const start = Math.max(0, Math.min(100 - span, center - span / 2))
  const end = start + span
  const zoomLeft = Math.max(0, Math.min(chart.width - zoomWidth, center / 100 * chart.width - zoomWidth / 2))
  const zoomSegments = selected ? segments.map(segment => ({ ...segment,
    zoomPercent: Math.max(0, Math.min(end, segment.end) - Math.max(start, segment.start)) / span * 100,
  })).filter(segment => segment.zoomPercent > 0) : []
  const describe = segment => `${row.label} · ${segment.label} (${segment.interval})\n${formatNumber(segment.count)} games · ${formatPercent(segment.percent)}`
  const cancelClose = () => window.clearTimeout(closeTimer.current)
  const scheduleClose = () => {
    cancelClose()
    closeTimer.current = window.setTimeout(() => setZoomId(null), 180)
  }
  const openGames = segment => {
    if (disabled) return
    cancelClose()
    setZoomId(null)
    onSelect({ scope: 'unexpected_endings', ending: row.id, score_band: segment.id,
      modes, analyzed_only: false, label: `${row.label} · ${segment.label}` })
  }
  const nearbySegment = event => {
    if (disabled) return null
    const bounds = event.currentTarget.getBoundingClientRect()
    if (!bounds.width) return null
    const x = Math.max(0, Math.min(bounds.width, event.clientX - bounds.left))
    const nearSmallSection = segments.filter(narrow).some(segment => {
      const left = segment.start / 100 * bounds.width, right = segment.end / 100 * bounds.width
      return Math.max(left - x, x - right, 0) <= HOVER_MARGIN
    })
    return nearSmallSection ? smallest : null
  }

  useEffect(() => {
    setZoomId(null)
    window.clearTimeout(closeTimer.current)
  }, [chart.width, disabled, row.counts.ahead_games, row.counts.balanced_games, row.counts.behind_games])
  useEffect(() => () => window.clearTimeout(closeTimer.current), [])
  useEffect(() => {
    if (!zoomId) return undefined
    const dismiss = () => setZoomId(null)
    window.addEventListener('scroll', dismiss, true)
    window.addEventListener('resize', dismiss)
    return () => {
      window.removeEventListener('scroll', dismiss, true)
      window.removeEventListener('resize', dismiss)
    }
  }, [zoomId])

  return <div ref={root} className="unexpected-ending-bar" onPointerEnter={cancelClose} onFocusCapture={cancelClose}
    onPointerLeave={() => { if (!root.current?.contains(document.activeElement)) scheduleClose() }}
    onBlur={event => { if (!event.currentTarget.contains(event.relatedTarget)) scheduleClose() }}
    onKeyDown={event => {
      if (event.key !== 'Escape' || !selected) return
      event.stopPropagation()
      event.preventDefault()
      cancelClose()
      if (event.target.closest('.unexpected-ending-zoom')) {
        skipFocusZoom.current = true
        root.current?.querySelector(`[data-ending-band="${selected.id}"]`)?.focus()
      }
      setZoomId(null)
    }}>
    <div ref={chart.ref} className="unexpected-ending-stack" role="group" aria-label={`${row.label}: 100% of assessed games`}
      onPointerDown={event => { touch.current = event.pointerType === 'touch' }}
      onPointerMove={event => {
        if (event.pointerType === 'touch') return
        const near = nearbySegment(event)
        if (near) { cancelClose(); setZoomId(near.id) }
      }}
      onClickCapture={event => {
        const near = touch.current && event.detail > 0 ? nearbySegment(event) : null
        touch.current = false
        if (!near) return
        event.preventDefault()
        event.stopPropagation()
        cancelClose()
        setZoomId(near.id)
      }}>
      {segments.length ? segments.map(segment => <button type="button"
        className={`unexpected-ending-segment ${segment.id}`} key={segment.id} data-ending-band={segment.id}
        disabled={disabled} style={{ width: `${segment.percent}%` }} title={selected ? undefined : describe(segment)}
        aria-label={`${describe(segment)}. Browse games, newest first.`}
        aria-controls={narrow(segment) && selected ? popupId : undefined}
        onFocus={() => {
          cancelClose()
          if (skipFocusZoom.current) { skipFocusZoom.current = false; return }
          if (narrow(segment)) setZoomId(smallest.id)
        }} onClick={() => openGames(segment)}>
        {segment.percent >= 14 ? <span aria-hidden="true">{formatPercent(segment.percent)}</span> : null}
      </button>) : <span className="unexpected-ending-empty">{modes.length ? 'No assessed games' : 'Select a mode'}</span>}
    </div>
    {selected && !disabled ? <div id={popupId} className="unexpected-ending-zoom" role="group"
      aria-label={`${row.label}: enlarged small sections`} style={{ left: zoomLeft, width: zoomWidth }}>
      <div className="unexpected-ending-zoom-track">
        {zoomSegments.map(segment => <button type="button" key={segment.id}
          className={`unexpected-ending-segment ${segment.id}`} style={{ width: `${segment.zoomPercent}%` }}
          title={describe(segment)} aria-label={describe(segment)} onClick={() => openGames(segment)}>
          {segment.id === selected.id ? <span aria-hidden="true">{formatPercent(segment.percent)}</span> : null}
        </button>)}
      </div>
      <div className="unexpected-ending-zoom-options">
        {zoomSegments.map(segment => <button type="button" key={segment.id}
          className="unexpected-ending-zoom-option" title={segment.interval} onClick={() => openGames(segment)}
          onFocus={cancelClose} aria-label={`${describe(segment)}. Browse games.`}>
          <i className={`unexpected-ending-swatch ${segment.id}`} aria-hidden="true" />
          <span>{segment.label}</span>
          <strong>{formatNumber(segment.count)} <small>{formatPercent(segment.percent)}</small></strong>
        </button>)}
      </div>
    </div> : null}
  </div>
}
