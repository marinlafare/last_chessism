import { number } from './algorithmForm'

function domain(values) {
  const low = Math.min(...values), high = Math.max(...values)
  const margin = (high - low) * 0.06 || Math.max(Math.abs(low) * 0.05, 1)
  return [low - margin, high + margin]
}

export default function RelationshipScatter({ scatter }) {
  if (!scatter.points.length) return <p>No points available.</p>
  const [xmin, xmax] = domain(scatter.points.map((point) => point.x))
  const [ymin, ymax] = domain(scatter.points.map((point) => point.y))
  const x = (value) => 90 + (value - xmin) / (xmax - xmin) * 770
  const y = (value) => 330 - (value - ymin) / (ymax - ymin) * 290
  return (
    <div className="algorithm-scatter">
      <h3>{scatter.y} vs {scatter.x}</h3>
      <p>{scatter.points.length.toLocaleString()} {scatter.sampled ? 'uniformly sampled points' : 'points'} · {scatter.units}. Hover a point for its source row identifier.</p>
      <svg viewBox="0 0 910 400" role="img" aria-label={`${scatter.y} versus ${scatter.x} scatter plot`}>
        {Array.from({ length: 5 }, (_, index) => {
          const xv = xmin + (xmax - xmin) * index / 4, yv = ymin + (ymax - ymin) * index / 4
          return <g key={index}><line x1="90" x2="860" y1={y(yv)} y2={y(yv)} className="algorithm-gridline" /><text x="80" y={y(yv) + 4} textAnchor="end">{number(yv)}</text><text x={x(xv)} y="354" textAnchor="middle">{number(xv)}</text></g>
        })}
        {scatter.points.map((point, index) => <circle key={index} cx={x(point.x)} cy={y(point.y)} r="3.8" fill="#59b6e5" fillOpacity="0.6"><title>{`${point.row_key}\n${scatter.x}: ${number(point.x)}\n${scatter.y}: ${number(point.y)}`}</title></circle>)}
        <text x="475" y="388" textAnchor="middle">{scatter.x}{scatter.units === 'z-score' ? ' (z-score)' : ''}</text>
        <text transform="translate(18 185) rotate(-90)" textAnchor="middle">{scatter.y}</text>
      </svg>
    </div>
  )
}
