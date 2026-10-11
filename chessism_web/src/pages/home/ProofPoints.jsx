import { pageStyleTarget } from '../../appearance/styleCore.js'
const points = [
  'Stockfish at scale',
  'Critical moments and turning points',
  'Player trends over time',
  'FEN surfacing for key positions',
  'Parallel jobs with Dockerized consistency'
]

function ProofPoints() {
  return (
    <section {...pageStyleTarget('panel')} className="proof-points" aria-label="Core capabilities">
      <ul>
        {points.map((point) => (
          <li key={point}>{point}</li>
        ))}
      </ul>
    </section>
  )
}

export default ProofPoints
