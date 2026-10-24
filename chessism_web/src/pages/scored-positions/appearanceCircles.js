// Input is the validated, ascending histogram from allAppearanceHistogram.
export function circleDistribution(bars) {
  return {
    highlightValue: bars.find(bar => bar.appearances === 1)?.count || 0,
    nonUniqueValue: bars.reduce((sum, bar) => sum + (bar.appearances > 1 ? bar.count : 0), 0),
    items: bars.filter(bar => bar.appearances >= 2 && bar.appearances <= 10)
      .map(bar => ({ key: bar.appearances, appearances: bar.appearances, value: bar.count })),
  }
}

export { pieSegments } from '../../utils/pieSegments.js'

// Include zero-appearance FENs too: this pie represents the whole FEN table.
export function coverageDistribution(rows = []) {
  let total = 0, unscored = 0
  for (const row of rows) {
    const count = Number(row.total_fens), missing = Number(row.unscored_fens)
    if (!Number.isSafeInteger(count) || count < 0 || !Number.isSafeInteger(missing) || missing < 0 || missing > count) continue
    total += count
    unscored += missing
  }
  return [{ key: 'scored', value: total - unscored }, { key: 'unscored', value: unscored }]
}
