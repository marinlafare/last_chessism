import { pageStyleTarget } from '../../../appearance/styleCore.js'

export default function MoveScoreFields() {
  return <div {...pageStyleTarget('text')} className="matrix-score-schema" aria-label="Formula columns">
    <code>player | game | result | move_score_1 | … | move_score_n</code>
  </div>
}
