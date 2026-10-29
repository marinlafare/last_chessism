export const DAILY_ACCURACY_LABELS = ['0–50', '51–60', '61–70', '71–80', '81–90', '91–100']

export function accuracyBandPoints(points, salienceReady) {
  return (points || []).flatMap(([date, winners, counts, salience, games]) =>
    (winners || []).filter(band => Number.isInteger(band) && band >= 0 && band < 6).map(band => ({
      date, band, counts, modalBands: winners, games: Number(games), gameEfficiency: band,
      dailySalience: salienceReady && salience != null && Number.isFinite(Number(salience)) ? Number(salience) : null,
      timestamp: Date.parse(`${date}T00:00:00Z`),
    })))
}

export function accuracyBandNote(point) {
  const winners = point.modalBands.map(band => DAILY_ACCURACY_LABELS[band]).join(', ')
  return `${point.date}\n${point.modalBands.length > 1 ? 'Tied most common bands' : 'Most common accuracy band'}: ${winners}`
    + `\n${point.games} analyzed games\n`
    + DAILY_ACCURACY_LABELS.map((label, index) => `${label}: ${point.counts[index]} games`).join('\n')
    + (point.averageGameSalience == null ? '' : `\n${point.averageGameSalience.toPrecision(4)} average game salience`)
}
