import { useEffect, useMemo, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import { appearanceWindow } from './pendingRepetitions'
import useAppearancePlot from './useAppearancePlot'

export default function AllFenAppearanceChart({ data, error, mode, view, scale, logarithmic, onScaleChange }) {
  const [page, setPage] = useState(0)
  const [selected, setSelected] = useState('')
  const isCommon = mode === 'commonness'
  const title = isCommon ? 'Communess' : 'Uniqueness'
  const { ref, width, height } = useAppearancePlot()
  useEffect(() => { setSelected('') }, [data?.refreshed_at, page, logarithmic])
  const { bars, currentPage, lastPage } = useMemo(() => appearanceWindow(view.bars, page, mode), [view, page, mode])
  const left = 50, right = 12, top = 8
  const step = (width - left - right) / Math.max(bars.length, 1)
  const angledLabels = bars.some(bar => formatNumber(bar.appearances).length * 7 > step * .85)
  const baseline = Math.max(top + 24, height - (angledLabels ? 68 : 34))
  const yFraction = fraction => baseline - fraction * (baseline - top)
  const y = count => yFraction(scale.fraction(count))
  return <div className="all-appearance-panel" data-appearance-view={mode}>
    <div className="pending-repetition-heading">
      <h2 className="panel-title">{title}</h2>
      <label className="appearance-scale-control">
        <select aria-label={`${title} Y scale`} value={logarithmic ? 'log' : 'linear'}
          onChange={e => onScaleChange(e.target.value === 'log')}>
          <option value="log">Logarithmic</option><option value="linear">Linear</option>
        </select>
      </label>
    </div>
    {(error || data?.refresh_error) && <div className="status-banner warn" role="alert">{error || data.refresh_error}</div>}
    {!data?.refreshed_at ? <p role="status">{error || data?.refresh_error
      ? 'Summary unavailable.' : data ? 'Preparing appearance summary in the background…' : 'Loading appearance summary…'}</p>
      : !bars.length ? <p>{isCommon ? 'No repeated FENs with recorded appearances.' : 'No FENs with recorded appearances.'}</p> : <>
        <div className="pending-repetition-scroll" onMouseLeave={() => setSelected('')}>
          {selected && <div className="appearance-tooltip" role="status">{selected}</div>}
          <svg {...pageStyleTarget('chart.appearances')} ref={ref} className="pending-repetition-chart all-appearance-chart" viewBox={`0 0 ${width} ${height}`} role="img"
            aria-label={`${title}. Compressed X-axis shows only counts that occur. ${logarithmic ? 'Logarithmic' : 'Linear'} frequency normalized from 0 to 1, not a per-FEN score.`}>
            {scale.ticks.map(tick => <g key={tick} className="appearance-y-tick">
              <line className="repetition-grid" x1={left} x2={width - right} y1={yFraction(tick)} y2={yFraction(tick)} />
              <text x={left - 10} y={yFraction(tick) + 4} textAnchor="end">{formatNumber(tick, 1)}</text>
            </g>)}
            {bars.map((bar, index) => {
              const x = left + (index + .5) * step, barWidth = Math.min(64, step * .72)
              const label = `${formatNumber(bar.appearances)} ${bar.appearances === 1 ? 'appearance' : 'appearances'}: ${formatNumber(bar.count)} distinct FENs; ${formatNumber(bar.count - bar.unscored)} scored, ${formatNumber(bar.unscored)} unscored`
              return <g key={bar.appearances} tabIndex={0} role="button" aria-label={label}
                data-all-appearances={bar.appearances} data-count={bar.count}
                onMouseEnter={() => setSelected(label)} onFocus={() => setSelected(label)} onClick={() => setSelected(label)}
                onBlur={() => setSelected('')}
                onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setSelected(label) } }}>
                <title>{label}</title>
                <rect className="repetition-bar" x={x - barWidth / 2} y={Math.min(baseline - 1, y(bar.count))}
                  width={barWidth} height={Math.max(1, baseline - y(bar.count))} />
                <text transform={`translate(${x} ${baseline + 16})${angledLabels ? ' rotate(-45)' : ''}`}
                  textAnchor={angledLabels ? 'end' : 'middle'}>{formatNumber(bar.appearances)}</text>
              </g>
            })}
            <text x={width / 2} y={height - 4} textAnchor="middle">Appearances</text>
            <text className="appearance-y-title" transform={`translate(12 ${(top + baseline) / 2}) rotate(-90)`} textAnchor="middle">{title}</text>
          </svg>
        </div>
        {lastPage > 0 && <nav className="appearance-pages" aria-label="Browse nonempty appearance counts">
          <button type="button" disabled={currentPage === 0} onClick={() => setPage(0)}>{isCommon ? 'Most' : 'First'}</button>
          <button type="button" disabled={currentPage === 0} onClick={() => setPage(currentPage - 1)}>{isCommon ? 'More' : 'Previous'}</button>
          <span aria-live="polite">{formatNumber(currentPage + 1)} / {formatNumber(lastPage + 1)}</span>
          <button type="button" disabled={currentPage === lastPage} onClick={() => setPage(currentPage + 1)}>{isCommon ? 'Less' : 'Next'}</button>
          <button type="button" disabled={currentPage === lastPage} onClick={() => setPage(lastPage)}>{isCommon ? 'Least' : 'Last'}</button>
        </nav>}
      </>}
  </div>
}
