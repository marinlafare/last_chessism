import { useEffect, useMemo, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import { appearanceHistogram, APPEARANCE_Y_LIMIT } from './pendingRepetitions'
import useAppearancePlot from './useAppearancePlot'

const compactNumber = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 })

export default function PendingRepetitionChart({ data, error }) {
  const [selected, setSelected] = useState(null)
  useEffect(() => { setSelected(null) }, [data?.refreshed_at])
  const view = useMemo(() => appearanceHistogram(data?.rows), [data])
  const { ref, width, height } = useAppearancePlot()
  const left = 58, right = 12, top = 8, baseline = Math.max(top + 24, height - 24), step = (width - left - right) / view.bars.length
  const y = value => baseline - (value / APPEARANCE_Y_LIMIT) * (baseline - top)
  return <div {...pageStyleTarget('unscored.panel')} className="pending-repetition-panel unscored-panel">
    <div className="pending-repetition-heading">
      <h2 {...pageStyleTarget('unscored.title')} className="panel-title">UNSCORED</h2>
    </div>
    {(error || data?.refresh_error) && <div className="status-banner warn" role="alert">{error || data.refresh_error}</div>}
    {!data?.refreshed_at ? <p role="status">{error || data?.refresh_error
      ? 'Summary unavailable.' : data ? 'Preparing appearance summary in the background…' : 'Loading appearance summary…'}</p>
      : !view.total ? <p>No unscored FENs with recorded appearances.</p> : <>
        <div className="pending-repetition-scroll" onMouseLeave={() => setSelected(null)}>
          {selected && <div className="appearance-tooltip" role="status">{selected}</div>}
          <svg {...pageStyleTarget('chart.pending')} ref={ref} className="pending-repetition-chart" viewBox={`0 0 ${width} ${height}`} role="img"
            aria-label="Unscored FENs with 1 to 10 appearances. Y-axis is capped at 1,000; select a bar for its exact count.">
            {[0, 250, 500, 750, 1000].map(value => <g key={value}>
              <line className="repetition-grid" x1={left} x2={width - right} y1={y(value)} y2={y(value)} />
              <text x={left - 10} y={y(value) + 4} textAnchor="end">{compactNumber.format(value).replace('K', 'k')}</text>
            </g>)}
            {view.bars.map((bar, index) => {
              const barWidth = Math.min(64, step * .72), x = left + (index + .5) * step - barWidth / 2
              const label = `${formatNumber(bar.appearances)} ${bar.appearances === 1 ? 'appearance' : 'appearances'}: ${formatNumber(bar.count)} distinct unscored FENs${bar.clipped ? ' (bar clipped at 1,000)' : ''}`
              return <g key={bar.appearances} tabIndex={0} role="button" aria-label={label}
                data-appearances={bar.appearances} data-count={bar.count} data-clipped={bar.clipped}
                onMouseEnter={() => setSelected(label)} onFocus={() => setSelected(label)}
                onBlur={() => setSelected(null)}
                onClick={() => setSelected(label)} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); setSelected(label) } }}>
                <title>{label}</title>
                <rect className="repetition-bar" x={x} y={y(bar.height)} width={barWidth}
                  height={bar.count ? Math.max(1, baseline - y(bar.height)) : 0} />
                {bar.clipped && <path className="repetition-clipped" d={`M${x},${top + 4} l${barWidth / 3},-4 l${barWidth / 3},4 l${barWidth / 3},-4`} />}
                <text x={x + barWidth / 2} y={baseline + 20} textAnchor="middle">{formatNumber(bar.appearances)}</text>
              </g>
            })}
            <text transform={`translate(12 ${(top + baseline) / 2}) rotate(-90)`} textAnchor="middle">n positions</text>
          </svg>
        </div>
        <div {...pageStyleTarget('unscored.axisTitle')} className="unscored-axis-title">Appearances</div>
      </>}
  </div>
}
