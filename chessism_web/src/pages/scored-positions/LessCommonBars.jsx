import { styleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import useAppearancePlot from './useAppearancePlot'
import { commonnessScale } from './commonness'

const compact = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 })

export default function LessCommonBars({ items, interactions }) {
  const { ref, width, height } = useAppearancePlot()
  const bars = Array.from({ length: 9 }, (_, index) => {
    const appearances = index + 2
    return { appearances, value: items.find(item => item.appearances === appearances)?.value || 0 }
  })
  const total = bars.reduce((sum, bar) => sum + bar.value, 0)
  const scale = commonnessScale(bars.map(bar => ({ appearances: bar.value })))
  const left = width < 180 ? 30 : 43, right = Math.max(left + 9, width - 8), top = 10
  const bottom = Math.max(top + 20, height - 22), slot = (right - left) / bars.length
  const y = value => bottom - value / scale.upper * (bottom - top)

  return <figure className="less-common-figure">
    <svg {...styleTarget('repeated.outline')} className="less-common-chart" ref={ref}
      viewBox={`0 0 ${width} ${height}`} role="group"
      aria-label="almos unique FENs: appearances from 2 to 10 on X, distinct FEN counts on Y, linear scale">
      {scale.ticks.map(tick => <g key={tick} className="commonness-y-tick" aria-hidden="true">
        <line x1={left} x2={right} y1={y(tick)} y2={y(tick)} />
        <text x={left - 5} y={y(tick) + 3} textAnchor="end">{compact.format(tick)}</text>
      </g>)}
      {bars.map((bar, index) => {
        const center = left + slot * (index + .5), barWidth = slot * .65
        const label = `${bar.appearances} times: ${formatNumber(bar.value)}`
        return <g key={bar.appearances} {...(bar.value ? interactions(label, bar.value / total, String(bar.appearances))
          : { role: 'img', 'aria-label': label })} {...styleTarget(`slice.${bar.appearances}`)}
          className="less-common-column" fill="var(--accent)" data-value={bar.value}
          data-fraction={total ? bar.value / total : 0} data-slice-appearances={bar.appearances}>
          {bar.value > 0 && <rect fill="transparent" x={left + slot * index} y={Math.min(y(bar.value), bottom - 14)}
            width={slot} height={Math.max(bottom - y(bar.value), 14)} />}
          <rect className="less-common-bar" x={center - barWidth / 2} y={y(bar.value)} width={barWidth}
            height={bottom - y(bar.value)} rx="1" />
        </g>
      })}
      {bars.filter((_, index) => width >= 200 || index % 2 === 0).map(bar => <text key={bar.appearances}
        x={left + slot * (bar.appearances - 1.5)} y={bottom + 14} textAnchor="middle">{bar.appearances}</text>)}
      {!total && <text x={(left + right) / 2} y={(top + bottom) / 2} textAnchor="middle">No FENs</text>}
    </svg>
    <figcaption {...styleTarget('circle.caption')}>almos unique</figcaption>
  </figure>
}
