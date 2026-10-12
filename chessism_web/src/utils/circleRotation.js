// Shared by homepage and Scored Positions pies.
export function rotateCircle(angle, delta) {
  if (!Number.isFinite(delta)) return angle
  return ((angle + delta) % 360 + 360) % 360
}

export function wheelRotation(deltaY, deltaMode = 0, side = 200) {
  const pixels = deltaY * (deltaMode === 1 ? 16 : deltaMode === 2 ? side : 1)
  return Number.isFinite(pixels) ? Math.max(-600, Math.min(600, pixels)) * .15 : 0
}

export function circlePercentage(fraction) {
  if (fraction > 0 && fraction < .0001) return '<0.01%'
  return new Intl.NumberFormat('en-US', { style: 'percent', maximumFractionDigits: 2 }).format(fraction)
}
