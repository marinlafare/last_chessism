// Input is the validated, ascending histogram from allAppearanceHistogram.
export function circleDistribution(bars) {
  return {
    highlightValue: bars.find(bar => bar.appearances === 1)?.count || 0,
    nonUniqueValue: bars.reduce((sum, bar) => sum + (bar.appearances > 1 ? bar.count : 0), 0),
    items: bars.filter(bar => bar.appearances >= 2 && bar.appearances <= 10)
      .map(bar => ({ key: bar.appearances, appearances: bar.appearances, value: bar.count })),
  }
}

export function pieSegments(items) {
  const positive = items.filter(item => Number.isFinite(item.value) && item.value > 0)
  const total = positive.reduce((sum, item) => sum + item.value, 0)
  let cumulative = 0
  const point = fraction => {
    const angle = fraction * 2 * Math.PI - Math.PI / 2
    return `${100 + 94 * Math.cos(angle)} ${100 + 94 * Math.sin(angle)}`
  }
  return positive.map((item, index) => {
    const start = cumulative / total
    cumulative += item.value
    const end = index === positive.length - 1 ? 1 : cumulative / total
    return { ...item, fraction: item.value / total, full: positive.length === 1,
      path: `M 100 100 L ${point(start)} A 94 94 0 ${end - start > .5 ? 1 : 0} 1 ${point(end)} Z` }
  })
}
