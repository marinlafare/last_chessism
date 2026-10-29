// Presentation only: filling a meter never declares a backend job complete.
export function progressMeter(value, max = 100) {
  const known = Number.isFinite(value) && Number.isFinite(max) && max > 0
  const current = known ? Math.max(0, Math.min(max, value)) : undefined
  return { value: current, max: known ? max : undefined,
    percent: known ? current / max * 100 : null }
}
