// Average over the same eligible analyzed games as the API's daily sum.
// Normalize against the full daily series, never against slider-filtered points.
export function relativeDailySalience(points) {
  const averaged = points.map((point) => ({
    ...point,
    averageGameSalience: Number.isFinite(point.dailySalience) && point.dailySalience >= 0
      && Number.isSafeInteger(point.games) && point.games > 0
      ? point.dailySalience / point.games : null,
  }))
  let minimum = Infinity
  let maximum = -Infinity
  for (const { averageGameSalience } of averaged) {
    if (averageGameSalience === null) continue
    minimum = Math.min(minimum, averageGameSalience)
    maximum = Math.max(maximum, averageGameSalience)
  }
  const available = Number.isFinite(minimum)
  const enabled = available && maximum > minimum
  return {
    minimum: available ? minimum : null,
    maximum: available ? maximum : null,
    enabled,
    points: averaged.map((point) => ({
      ...point,
      relativeSalience: enabled && point.averageGameSalience !== null
        ? Math.max(0, Math.min(100, 100 * (point.averageGameSalience - minimum) / (maximum - minimum)))
        : null,
    })),
  }
}

export function filterDailySalience(points, range, enabled) {
  // Missing salience is not zero. Keep unknown days in the unfiltered view only.
  if (!enabled || (range[0] === 0 && range[1] === 100)) return points
  return points.filter(({ relativeSalience }) => Number.isFinite(relativeSalience)
    && relativeSalience >= range[0] - 1e-9 && relativeSalience <= range[1] + 1e-9)
}

export function moveSalienceBound(range, index, value) {
  const next = [...range]
  next[index] = Math.max(index === 0 ? 0 : range[0],
    Math.min(index === 0 ? range[1] : 100, Math.round(value)))
  return next
}
