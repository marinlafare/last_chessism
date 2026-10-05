import { number } from './algorithmForm'

export default function BuilderChart({ output }) {
  const points = output.points
  if (!points.length) return <p>No finite points available.</p>
  const low = Math.min(0, ...points.map((item) => item.y))
  const high = Math.max(0, ...points.map((item) => item.y))
  const span = high - low || 1
  const y = (value) => 310 - (value - low) / span * 270
  const dateAxis = points.every((point) => typeof point.x === 'string'
    && /^\d{4}-\d{2}(-\d{2})?$/.test(point.x) && Number.isFinite(Date.parse(point.x)))
  const numericAxis = points.every((point) => typeof point.x === 'number')
  const values = points.map((point, index) => dateAxis ? Date.parse(point.x) : numericAxis ? point.x : index)
  const minimum = Math.min(...values), maximum = Math.max(...values)
  const x = (index) => output.type === 'bar'
    ? 80 + (index + .5) / points.length * 780
    : 80 + (values[index] - minimum) / (maximum - minimum || 1) * 780
  const labelEvery = Math.max(1, Math.ceil(points.length / 8))
  return <div className="algorithm-scatter">
    <svg viewBox="0 0 920 400" role="img" aria-label={`${output.type} chart: ${output.name}`}>
      {Array.from({ length: 5 }, (_, index) => {
        const value = low + span * index / 4
        return <g key={index}>
          <line x1="80" x2="860" y1={y(value)} y2={y(value)} className="algorithm-gridline" />
          <text x="70" y={y(value) + 4} textAnchor="end">{number(value)}</text>
        </g>
      })}
      {output.type === 'line' && <polyline fill="none" stroke="#59b6e5" strokeWidth="2"
        points={points.map((point, index) => `${x(index)},${y(point.y)}`).join(' ')} />}
      {points.map((point, index) => <g key={index}>
        {output.type === 'bar'
          ? <rect x={x(index) - 780 / points.length * .35} y={Math.min(y(0), y(point.y))}
            width={780 / points.length * .7} height={Math.abs(y(point.y) - y(0))} fill="#59b6e5">
            <title>{`${point.x}: ${number(point.y)}\n${point.row_key}`}</title>
          </rect>
          : <circle cx={x(index)} cy={y(point.y)} r="3" fill="#59b6e5">
            <title>{`${point.x}: ${number(point.y)}\n${point.row_key}`}</title>
          </circle>}
        {index % labelEvery === 0 && <text x={x(index)} y="337" textAnchor="middle">
          {typeof point.x === 'number' ? number(point.x) : point.x}
        </text>}
      </g>)}
      <text x="470" y="386" textAnchor="middle">{output.x}</text>
      <text transform="translate(18 175) rotate(-90)" textAnchor="middle">{output.y}</text>
    </svg>
  </div>
}
