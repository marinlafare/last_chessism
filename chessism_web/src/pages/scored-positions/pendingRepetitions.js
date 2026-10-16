export const APPEARANCE_Y_LIMIT = 1000
export const APPEARANCE_X_MAX = 10
export const APPEARANCE_PAGE_SIZE = 10

export function appearanceWindow(bars, page = 0, mode = 'uniqueness') {
  const fromTail = mode === 'commonness'
  const eligible = fromTail ? bars.filter(bar => bar.appearances > 1) : bars
  const lastPage = Math.max(0, Math.ceil(eligible.length / APPEARANCE_PAGE_SIZE) - 1)
  const currentPage = Math.max(0, Math.min(Number.isFinite(page) ? Math.floor(page) : 0, lastPage))
  const start = fromTail ? Math.max(0, eligible.length - (currentPage + 1) * APPEARANCE_PAGE_SIZE)
    : currentPage * APPEARANCE_PAGE_SIZE
  const end = fromTail ? eligible.length - currentPage * APPEARANCE_PAGE_SIZE : start + APPEARANCE_PAGE_SIZE
  return { bars: eligible.slice(start, end), currentPage, lastPage }
}

// A categorical (compressed) X-axis: absent counts never become empty bars.
// Real appearance values are retained, including very large gaps in the tail.
export function allAppearanceHistogram(rows = []) {
  const counts = new Map()
  let withoutAppearances = 0
  for (const row of rows) {
    const x = Number(row.appearances), count = Number(row.total_fens)
    const unscored = Number(row.unscored_fens)
    if (!Number.isSafeInteger(x) || x < 0 || !Number.isSafeInteger(count) || count <= 0) continue
    if (x === 0) { withoutAppearances += count; continue }
    const previous = counts.get(x) || { appearances: x, count: 0, unscored: 0 }
    previous.count += count
    previous.unscored += Number.isSafeInteger(unscored) && unscored >= 0 && unscored <= count ? unscored : 0
    counts.set(x, previous)
  }
  const bars = [...counts.values()].sort((a, b) => a.appearances - b.appearances)
  return { bars, withoutAppearances, total: bars.reduce((sum, bar) => sum + bar.count, 0) }
}

export function appearanceScale(bars, logarithmic = true) {
  const maxCount = bars.reduce((max, bar) => Math.max(max, bar.count), 1)
  const upper = 10 ** Math.ceil(Math.log10(maxCount))
  // Relabel the existing full-distribution range; keep bar heights and the
  // denominator identical across pages. This is not a per-FEN uniqueness score.
  return { upper, ticks: [0, .2, .4, .6, .8, 1],
    fraction: count => logarithmic ? Math.log10(1 + count) / Math.log10(1 + upper) : count / upper }
}

export function appearanceHistogram(rows = []) {
  const counts = new Map()
  let total = 0, withoutAppearances = 0, aboveRange = 0
  for (const row of rows) {
    // The API's legacy "repetitions" key contains fen.n_games, which already
    // counts appearances, not repeats after a first appearance. Never add 1.
    const x = Number(row.repetitions), count = Number(row.fen_count)
    if (!Number.isSafeInteger(x) || x < 0 || !Number.isSafeInteger(count) || count < 0) continue
    if (x === 0) { withoutAppearances += count; continue }
    total += count
    if (x > APPEARANCE_X_MAX) { aboveRange += count; continue }
    counts.set(x, (counts.get(x) || 0) + count)
  }
  const bars = Array.from({ length: APPEARANCE_X_MAX }, (_, i) => {
    const appearances = i + 1, count = counts.get(appearances) || 0
    return { appearances, count, height: Math.min(count, APPEARANCE_Y_LIMIT), clipped: count > APPEARANCE_Y_LIMIT }
  })
  return { total, withoutAppearances, aboveRange, bars }
}
