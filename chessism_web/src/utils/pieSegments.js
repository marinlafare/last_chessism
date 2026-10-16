// Shared SVG geometry: centered at (100, 100), radius 94.
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
