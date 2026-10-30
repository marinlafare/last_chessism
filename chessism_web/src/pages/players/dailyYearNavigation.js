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

export const MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

export function datePeriodLabel(period) {
  return period === 'all' ? 'All years' : period.length === 7
    ? `${MONTH_LABELS[Number(period.slice(5)) - 1]} ${period.slice(0, 4)}` : period
}

export function dateInPeriod(date, period) {
  return period === 'all' || String(date).startsWith(`${period}-`)
}

export function buildDateScale(points, left, right, period = 'all') {
  if (period === 'all') {
    const scale = buildYearScale(points, left, right)
    return { ...scale, bands: scale.bands.map(({ year, ...band }) => ({ ...band, key: year, label: year, kind: 'year' })) }
  }
  const year = Number(period.slice(0, 4)), month = period.length === 7 ? Number(period.slice(5)) - 1 : null
  const start = Date.UTC(year, month ?? 0, 1)
  const end = month === null ? Date.UTC(year + 1, 0, 1) - 1 : Date.UTC(year, month + 1, 1) - 1
  const x = timestamp => left + (timestamp - start) / (end - start) * (right - left)
  const months = [...new Set(points.filter(p => Number.isFinite(p.timestamp)
    && p.timestamp >= start && p.timestamp <= end).map(p => new Date(p.timestamp).getUTCMonth()))].sort((a, b) => a - b)
  return { start, end, bands: month !== null ? [] : months.map(m => ({
    key: `${year}-${String(m + 1).padStart(2, '0')}`, label: MONTH_LABELS[m], kind: 'month',
    left: x(Date.UTC(year, m, 1)), right: x(Math.min(end, Date.UTC(year, m + 1, 1))),
  })) }
}

export function periodAtX(bands, x) {
  return yearAtX(bands.map(band => ({ ...band, year: band.key })), x)
}

export function periodTicks(start, end, left, right, period) {
  if (start === null || end === null || period === 'all') return []
  const year = Number(period.slice(0, 4)), month = period.length === 7 ? Number(period.slice(5)) - 1 : null
  const x = timestamp => left + (timestamp - start) / (end - start) * (right - left)
  if (month === null) return MONTH_LABELS.map((label, m) => ({
    label, x: x((Date.UTC(year, m, 1) + Date.UTC(year, m + 1, 1)) / 2),
  }))
  const lastDay = new Date(end).getUTCDate()
  return Array.from({ length: 7 }, (_, i) => {
    const day = 1 + Math.round((lastDay - 1) * i / 6)
    return { label: String(day), x: x(Date.UTC(year, month, day)) }
  })
}
