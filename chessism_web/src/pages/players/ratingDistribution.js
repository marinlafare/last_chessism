// Rendering is proportional to rating intervals, never to individual games.
export function ratingDistributionPlot(data) {
  const x = data?.x || [], y = data?.y || []
  const allBins = x.length === y.length ? x.map((rating, index) => ({ rating, percent: y[index] })) : []
  const total = allBins.reduce((sum, bin) => sum + bin.percent, 0)
  let first = 0, last = allBins.length - 1
  if (total > 0) {
    // Remove only whole bins within each 0.5% tail, retaining boundary bins
    // so at least 99% of games remain. Use the returned mass to tolerate
    // rounding of API percentages without changing any bar's value.
    const tail = total * 0.005
    let lowerTail = 0, upperTail = 0
    while (first < last && lowerTail + allBins[first].percent <= tail) {
      lowerTail += allBins[first++].percent
    }
    while (last > first && upperTail + allBins[last].percent <= tail) {
      upperTail += allBins[last--].percent
    }
    // Keep 20 rating points of context on both sides, within the full range.
    const lower = allBins[first].rating - 20, upper = allBins[last].rating + 20
    while (first > 0 && allBins[first - 1].rating >= lower) first--
    while (last < allBins.length - 1 && allBins[last + 1].rating <= upper) last++
  }
  const isTrimmed = first > 0 || last < allBins.length - 1
  const bins = isTrimmed ? allBins.slice(first, last + 1) : allBins
  const visiblePercent = total > 0 ? 100 * bins.reduce((sum, bin) => sum + bin.percent, 0) / total : 0
  const maximum = bins.reduce((value, bin) => Math.max(value, bin.percent), 0)
  const magnitude = maximum > 0 ? 10 ** Math.floor(Math.log10(maximum)) : 1
  const ceiling = maximum > 0 ? Math.min(100, Math.ceil(maximum / magnitude) * magnitude) : 1
  const ticks = bins.length ? [...new Set(Array.from({ length: Math.min(5, bins.length) }, (_, i) => (
    Math.round(i * (bins.length - 1) / Math.max(1, Math.min(5, bins.length) - 1))
  )))] : []
  return { bins, ceiling, ticks, binWidth: data?.bin_width || 10, isTrimmed, visiblePercent }
}
