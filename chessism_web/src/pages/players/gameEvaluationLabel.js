export function evaluationLabel(evaluation, orientation = 'white') {
  if (!Number.isFinite(evaluation?.white_cp)) return 'N/A'
  const score = evaluation.white_cp * (orientation === 'black' ? -1 : 1)
  if (evaluation.kind === 'mate') {
    if (evaluation.mate_moves === 0) return 'CHECKMATE'
    return `M${score > 0 ? '+' : '−'}${Number.isInteger(evaluation.mate_moves) ? evaluation.mate_moves : '?'}`
  }
  if (evaluation.kind === 'tablebase') return score > 0 ? 'TB WIN' : score < 0 ? 'TB LOSS' : 'TB DRAW'
  return `${score >= 0 ? '+' : ''}${(score / 100).toFixed(2)}`
}

export function evaluationDescription(evaluation, orientation = 'white') {
  if (evaluation?.kind !== 'mate') return evaluationLabel(evaluation, orientation)
  const positive = evaluation.white_cp * (orientation === 'black' ? -1 : 1) > 0
  const distance = evaluation.mate_moves === 0 ? 'Checkmate'
    : Number.isInteger(evaluation.mate_moves) ? `Mate in ${evaluation.mate_moves}` : 'Mate; distance unavailable'
  return `${distance} ${positive ? 'for' : 'against'} ${orientation === 'black' ? 'Black' : 'White'}`
}
