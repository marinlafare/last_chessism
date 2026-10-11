export function commonnessScale(bars) {
  const maximum = bars.reduce((max, bar) => Math.max(max, bar.appearances), 1)
  const roughStep = Math.max(1, maximum / 4)
  const magnitude = 10 ** Math.floor(Math.log10(roughStep))
  const step = [1, 2, 5, 10].find(n => n * magnitude >= roughStep) * magnitude
  const upper = Math.ceil(maximum / step) * step
  return { upper, ticks: Array.from({ length: upper / step + 1 }, (_, i) => i * step) }
}
