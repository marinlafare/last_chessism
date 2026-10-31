import { lazy, Suspense, useEffect, useLayoutEffect, useMemo, useState } from 'react'
import { styleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import { circleDistribution, coverageDistribution, pieSegments } from './appearanceCircles'
import useCircleRotation from '../../hooks/useCircleRotation'
import { circlePercentage } from '../../utils/circleRotation'
import AppearanceCircleTooltip from './AppearanceCircleTooltip'
import LessCommonBars from './LessCommonBars'
const loadPositionExplorer = () => import('./FenPositionExplorer')
const FenPositionExplorer = lazy(loadPositionExplorer)

export default function AppearanceCircles({ data, error, view, active }) {
  const [selected, setSelected] = useState(null)
  const [explorer,setExplorer]=useState(null)
  const [showLessCommon, setShowLessCommon] = useState(false)
  const { highlightValue, nonUniqueValue, items } = useMemo(() => circleDistribution(view.bars), [view])
  const uniqueSegments = useMemo(() => pieSegments([
    { key: 'unique', value: highlightValue }, { key: 'non-unique', value: nonUniqueValue },
  ]), [highlightValue, nonUniqueValue])
  const coverageSegments = useMemo(() => pieSegments(coverageDistribution(data?.rows)), [data?.rows])
  const uniqueRotation = useCircleRotation(), coverageRotation = useCircleRotation()
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
  }, [uniqueRotation.angle, coverageRotation.angle])
  const interactions = (label, fraction, scope) => {
    const percentage = circlePercentage(fraction)
    const preload = () => {
      // Fetch the popup code on intent, without querying positions until clicked.
      if (scope && scope !== 'repeated') void loadPositionExplorer().catch(() => {})
    }
    const show = (event, pointer = true) => {
      const rect = event.currentTarget.getBoundingClientRect()
      setSelected({ label, percentage, pointer, x: pointer ? event.clientX : rect.left + rect.width / 2,
        y: pointer ? event.clientY : rect.top + rect.height / 2 })
    }
    const open=event=>{
      if (!scope) return
      const rect=event.currentTarget.getBoundingClientRect()
      setSelected(null)
      if (scope === 'repeated') {
        setExplorer(null)
        setShowLessCommon(true)
        return
      }
      setExplorer({scope,key:performance.now(),x:event.detail?event.clientX+12:rect.right+12,y:event.detail?event.clientY+12:rect.top})
    }
    return {
      tabIndex: 0, role: scope ? 'button' : 'img', 'aria-label': `${label} · ${percentage}`,
      'aria-expanded': scope === 'repeated' ? showLessCommon : undefined,
      'data-circle-slice': '', 'data-tooltip-label': label,
      'aria-describedby': selected?.label === label ? 'appearance-circle-tooltip' : undefined,
      onMouseEnter: event => { preload(); show(event) }, onMouseMove: event => show(event), onMouseLeave: () => setSelected(null),
      onFocus: event => { preload(); show(event, false) }, onBlur: () => setSelected(null),
      onClick: open, onKeyDown: event => {
        if (event.key === 'Escape') setSelected(null)
        if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); open(event) }
      },
    }
  }
  return <div className="appearance-circles-panel">
    <AppearanceCircleTooltip selection={selected} />
    {explorer&&<Suspense fallback={null}><FenPositionExplorer key={explorer.key} scope={explorer.scope} anchor={explorer} onClose={()=>setExplorer(null)}/></Suspense>}
    {(error || data?.refresh_error) && <div className="status-banner warn" role="alert">{error || data.refresh_error}</div>}
    {!data?.refreshed_at ? <p role="status">{error || data?.refresh_error ? 'Summary unavailable.'
      : data ? 'Preparing appearance summary in the background…' : 'Loading appearance summary…'}</p>
      : <div className={`appearance-circles${showLessCommon ? ' has-less-common' : ''}`}>
        <figure className="appearance-unique-figure">
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
        <div className="appearance-less-common-slot" aria-hidden={!showLessCommon}>
          {showLessCommon && <LessCommonBars items={items} interactions={interactions} />}
        </div>
        <figure className="appearance-coverage-figure">
          <div className="appearance-circle-frame">
            <svg {...styleTarget('coverage.outline')} className="appearance-circle appearance-coverage" {...coverageRotation.svg} role="group" aria-label="Scored and unscored FENs">
              {!coverageSegments.length && <><circle className="appearance-empty" cx="100" cy="100" r="94" />
                <text x="100" y="105" textAnchor="middle">No FENs</text></>}
              <g transform={`rotate(${coverageRotation.angle} 100 100)`}>
                {coverageSegments.map(segment => <g key={segment.key}
                  {...interactions(`${segment.key}: ${formatNumber(segment.value)}`, segment.fraction, segment.key)}
                  {...styleTarget(segment.key === 'scored' ? 'coverage.scored' : 'coverage.unscored')}
                  data-coverage-part={segment.key} data-value={segment.value} data-fraction={segment.fraction}
                  fill={segment.key === 'scored' ? '#17bc91' : '#e76e65'}>
                  {segment.full ? <circle cx="100" cy="100" r="94" /> : <path d={segment.path} />}
                </g>)}
              </g>
              <circle className="appearance-circle-outline" cx="100" cy="100" r="94" />
            </svg>
          </div>
          <figcaption {...styleTarget('circle.caption')}>Positions</figcaption>
        </figure>
      </div>}
  </div>
}
