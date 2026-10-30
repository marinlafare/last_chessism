export const SALIENCE_BAND_LABELS = ['0–50', '>50–60', '>60–70', '>70–80', '>80–90', '>90–100']
export const SALIENCE_BAND_BOUNDARIES = [0, 50, 60, 70, 80, 90, 100]

export function salienceBoundStops(range, index) {
  // Keep at least one complete bin selected; handles cannot meet or cross.
  return SALIENCE_BAND_BOUNDARIES.filter(value => index === 0 ? value < range[1] : value > range[0])
}

export function moveSalienceBandBound(range, index, value) {
  if (!Number.isFinite(value)) return range
  const stops = salienceBoundStops(range, index)
  const nearest = stops.reduce((best, stop) => Math.abs(stop - value) < Math.abs(best - value) ? stop : best)
  return range.map((bound, i) => i === index ? nearest : bound)
}

export function attachSalienceBands(points, snapshot) {
  const days = new Map()
  for (const row of snapshot?.status === 'ready' ? snapshot.points || [] : []) {
    const [day, modes, counts, games] = row
    if (!Array.isArray(counts) || counts.length !== 6 || !counts.every(n => Number.isSafeInteger(n) && n >= 0)
      || !Number.isSafeInteger(games) || games <= 0 || counts.reduce((a, b) => a + b, 0) !== games
      || !Array.isArray(modes) || !modes.length || !modes.every(n => Number.isInteger(n) && n >= 0 && n < 6)) {
      days.delete(day); continue
    }
    days.set(day, { modalBands: modes, counts, games })
  }
  return points.map(point => ({ ...point, salienceBands: days.get(point.date) || null }))
}

export function filterSalienceBands(points, range, enabled) {
  if (!enabled || (range[0] === 0 && range[1] === 100)) return points
  // A range selects whole bins, not daily averages or approximated bin midpoints.
  // Any tied winning bin can match, without adding another copy of the point.
  return points.filter(point => point.salienceBands?.modalBands.some(band =>
    SALIENCE_BAND_BOUNDARIES[band] >= range[0] && SALIENCE_BAND_BOUNDARIES[band + 1] <= range[1]))
}

export function salienceBandNote(point) {
  const summary = point.salienceBands
  if (!summary) return '\nDaily salience bands unavailable'
  const labels = summary.modalBands.map(band => SALIENCE_BAND_LABELS[band]).join(', ')
  return `\n${summary.modalBands.length > 1 ? 'Tied most common salience bands' : 'Most common salience band'}: ${labels}`
    + `\n${summary.games} games with calculated salience (including unscored games)\n`
    + SALIENCE_BAND_LABELS.map((label, i) => `Salience ${label}: ${summary.counts[i]} games`).join('\n')
}
