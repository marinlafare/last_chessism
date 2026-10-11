import { lazy, Suspense, useEffect, useLayoutEffect, useMemo, useState } from 'react'
import { styleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import AppearanceChartDetails from './AppearanceChartDetails'
import { circleDistribution, pieSegments } from './appearanceCircles'
import useCircleRotation from './useCircleRotation'
import { circlePercentage } from './circleRotation'
import AppearanceCircleTooltip from './AppearanceCircleTooltip'
const FenPositionExplorer=lazy(()=>import('./FenPositionExplorer'))

const COLORS = ['#17bc91', '#56a9e4', '#e0b956', '#ba8bdb', '#e88374',
  '#80bd5c', '#59d2d5', '#df91be', '#8294e8', '#caac83']

export default function AppearanceCircles({ data, error, view, active }) {
  const [selected, setSelected] = useState(null)
  const [explorer,setExplorer]=useState(null)
  const { highlightValue, nonUniqueValue, items } = useMemo(() => circleDistribution(view.bars), [view])
  const uniqueSegments = useMemo(() => pieSegments([
    { key: 'unique', value: highlightValue }, { key: 'non-unique', value: nonUniqueValue },
  ]), [highlightValue, nonUniqueValue])
  const segments = useMemo(() => pieSegments(items), [items])
  const uniqueRotation = useCircleRotation(), pieRotation = useCircleRotation()
  useEffect(() => { setSelected(null) }, [data?.refreshed_at, active])
  useEffect(() => { if(!active)setExplorer(null) },[active])
  useEffect(() => {
    const clear = () => setSelected(null)
    window.addEventListener('scroll', clear, true)
    window.addEventListener('resize', clear)
    return () => { window.removeEventListener('scroll', clear, true); window.removeEventListener('resize', clear) }
  }, [])
  useLayoutEffect(() => {
    // Rotation can move a different slice under a stationary cursor.
    setSelected(current => {
      if (!current?.pointer) return current
      const slice = document.elementFromPoint(current.x, current.y)?.closest('[data-circle-slice]')
      if (!slice) return null
      const label = slice.dataset.tooltipLabel, percentage = circlePercentage(Number(slice.dataset.fraction))
      return label === current.label && percentage === current.percentage ? current : { ...current, label, percentage }
    })
  }, [uniqueRotation.angle, pieRotation.angle])
  const pieTitle = '2–10 appearances'
  const interactions = (label, fraction, scope) => {
    const percentage = circlePercentage(fraction)
    const show = (event, pointer = true) => {
      const rect = event.currentTarget.getBoundingClientRect()
      setSelected({ label, percentage, pointer, x: pointer ? event.clientX : rect.left + rect.width / 2,
        y: pointer ? event.clientY : rect.top + rect.height / 2 })
    }
    const open=event=>{
      const rect=event.currentTarget.getBoundingClientRect()
      setSelected(null)
      setExplorer({scope,key:performance.now(),x:event.detail?event.clientX+12:rect.right+12,y:event.detail?event.clientY+12:rect.top})
    }
    return {
      tabIndex: 0, role: 'button', 'aria-label': `${label} · ${percentage}`,
      'data-circle-slice': '', 'data-tooltip-label': label,
      'aria-describedby': selected?.label === label ? 'appearance-circle-tooltip' : undefined,
      onMouseEnter: event => show(event), onMouseMove: event => show(event), onMouseLeave: () => setSelected(null),
      onFocus: event => show(event, false), onBlur: () => setSelected(null),
      onClick: open, onKeyDown: event => {
        if (event.key === 'Escape') setSelected(null)
        if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); open(event) }
      },
    }
  }
  return <div className="appearance-circles-panel">
    <div className="pending-repetition-heading">
      <AppearanceChartDetails>
        <p>The left pie compares distinct FENs appearing exactly once (green) with all FENs appearing more than once (red), including those appearing more than ten times. FENs without recorded appearances are excluded.</p>
        <p>The right pie counts distinct FENs with 2–10 appearances, not their combined appearances. FENs appearing more than ten times are excluded from the right pie.</p>
        <p>Both views include scored and unscored FENs. Equal circle sizes do not compare totals. Hover, tap or focus for percentages and exact counts; each percentage is relative to its own pie.</p>
        <p>Scroll over either circle to rotate it, without zooming. Double-click to reset. Keyboard: arrow keys to rotate, 0 to reset.</p>
        <p>Click a slice for ten sampled positions at a time. Select a position to explore the games containing it.</p>
        {data?.refreshed_at && <p>Snapshot: <time dateTime={data.refreshed_at}>{new Date(data.refreshed_at).toLocaleString()}</time>
          {data.stale ? ' · Changes pending; summary refreshes in the background.' : ''}</p>}
      </AppearanceChartDetails>
    </div>
    <AppearanceCircleTooltip selection={selected} />
    {explorer&&<Suspense fallback={<p role="status">Opening positions…</p>}><FenPositionExplorer key={explorer.key} scope={explorer.scope} anchor={explorer} onClose={()=>setExplorer(null)}/></Suspense>}
    {(error || data?.refresh_error) && <div className="status-banner warn" role="alert">{error || data.refresh_error}</div>}
    {!data?.refreshed_at ? <p role="status">{error || data?.refresh_error ? 'Summary unavailable.'
      : data ? 'Preparing appearance summary in the background…' : 'Loading appearance summary…'}</p>
      : <div className="appearance-circles">
        <figure>
          <div className="appearance-circle-frame">
            <svg {...styleTarget('unique.outline')} className="appearance-circle appearance-uniqueness" {...uniqueRotation.svg} role="group" aria-label="Unique and non-unique FENs">
              {!uniqueSegments.length && <><circle className="appearance-empty" cx="100" cy="100" r="94" />
                <text x="100" y="105" textAnchor="middle">No FENs</text></>}
              <g transform={`rotate(${uniqueRotation.angle} 100 100)`}>
              {uniqueSegments.map(segment => {
                const label = `${segment.key}: ${formatNumber(segment.value)}`
                return <g key={segment.key} {...interactions(label, segment.fraction,segment.key==='unique'?'unique':'repeated')}
                  {...styleTarget(segment.key==='unique'?'unique.slice':'nonunique.slice')} data-unique-part={segment.key}
                  data-highlight={segment.key === 'unique' ? 'uniqueness' : undefined}
                  data-value={segment.value} data-fraction={segment.fraction}
                  className={segment.key === 'unique' ? 'appearance-highlight' : 'appearance-non-unique'}>
                  {segment.full ? <circle cx="100" cy="100" r="94" /> : <path d={segment.path} />}
                </g>
              })}
              </g>
              <circle className="appearance-circle-outline" cx="100" cy="100" r="94"/>
            </svg>
          </div>
          <figcaption {...styleTarget('circle.caption')}>Unique</figcaption>
        </figure>
        <figure>
          <div className="appearance-circle-frame">
            <svg {...styleTarget('repeated.outline')} className="appearance-circle appearance-pie" {...pieRotation.svg} role="group" aria-label={pieTitle}>
              {!segments.length && <><circle className="appearance-empty" cx="100" cy="100" r="94" />
                <text x="100" y="105" textAnchor="middle">No FENs</text></>}
              <g transform={`rotate(${pieRotation.angle} 100 100)`}>
              {segments.map(segment => {
                const label = `${segment.appearances} times: ${formatNumber(segment.value)}`
                return <g key={segment.key} {...interactions(label, segment.fraction,String(segment.appearances))}
                  {...styleTarget(`slice.${segment.appearances}`)} data-slice={segment.key}
                  data-value={segment.value} data-fraction={segment.fraction}
                  data-slice-appearances={segment.appearances} fill={COLORS[segment.appearances - 2]}>
                  {segment.full ? <circle cx="100" cy="100" r="94" /> : <path d={segment.path} />}
                </g>
              })}
              </g>
              <circle className="appearance-circle-outline" cx="100" cy="100" r="94"/>
            </svg>
          </div>
          <figcaption {...styleTarget('circle.caption')}>{pieTitle}</figcaption>
        </figure>
      </div>}
  </div>
}
