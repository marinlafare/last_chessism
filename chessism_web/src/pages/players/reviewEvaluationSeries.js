// Display coordinates only: keep the actual evaluation for labels/tooltips.
// Mate/tablebase encodings are outcomes, not thousands of centipawns.
export const REVIEW_CP_LIMIT = 500
export function reviewPlotValue(evaluation) {
  if (!Number.isFinite(evaluation?.white_cp)) return null
  if (evaluation.kind === 'mate' || evaluation.kind === 'tablebase') {
    return Math.sign(evaluation.white_cp) * REVIEW_CP_LIMIT
  }
  if (evaluation.kind !== 'cp') return null
  return Math.max(-REVIEW_CP_LIMIT, Math.min(REVIEW_CP_LIMIT, evaluation.white_cp))
}

export function reviewEvaluationSeries(moves, width = 480) {
  const left = 4, right = Math.max(left + 1, width - 4), top = 4, bottom = 96
  const zero = (top + bottom) / 2
  const x = index => left + (index - 1) / Math.max(1, moves.length - 1) * (right - left)
  const points = moves.map((move, index) => {
    const value = reviewPlotValue(move.evaluation_after)
    return { index: index + 1, move, x: x(index + 1),
      y: value === null ? null : zero - value / REVIEW_CP_LIMIT * (bottom - top) / 2 }
  })
  const segments = []
  let segment = []
  for (const point of points) {
    if (point.y === null) { if (segment.length) segments.push(segment); segment = [] }
    else segment.push(point)
  }
  if (segment.length) segments.push(segment)
  return { points, segments, left, right, top, bottom, zero }
}
