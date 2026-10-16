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
