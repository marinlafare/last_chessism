// Calendar-aligned bounds keep even a year with a single day easy to select.
// Dates are already player-local calendar days; UTC here avoids browser timezone shifts.
export function buildYearScale(points, left, right) {
  const years = [...new Set(points
    .filter(point => Number.isFinite(point.timestamp))
    .map(point => new Date(point.timestamp).getUTCFullYear()))].sort((a, b) => a - b)
  if (!years.length) return { bands: [], start: null, end: null }
  const start = Date.UTC(years[0], 0, 1)
  const end = Date.UTC(years.at(-1) + 1, 0, 1) - 1
  const x = timestamp => left + (timestamp - start) / (end - start) * (right - left)
  return {
    start,
    end,
    bands: years.map(year => ({
      year: String(year),
      left: x(Date.UTC(year, 0, 1)),
      right: x(Math.min(end, Date.UTC(year + 1, 0, 1))),
    })),
  }
}

export function yearAtX(bands, x) {
  if (!Number.isFinite(x)) return null
  return bands.find((band, index) => x >= band.left
    && (x < band.right || (index === bands.length - 1 && x === band.right)))?.year ?? null
}
