import { progressMeter } from './progressMeter.js'

// No shared CSS/style identity: each caller retains its own track, fill and
// Edit styles target. Native <progress> remains native for existing selectors.
export default function ProgressMeter({ value, max = 100, native = false,
  fillClassName, fillTag: Fill = 'div', indeterminateWidth = 0, ...props }) {
  const meter = progressMeter(value, max)
  if (native) return <progress {...props} value={meter.value} max={meter.max || 1} />
  return <div {...props} role="progressbar" aria-valuemin={0}
    aria-valuemax={meter.max || 100} aria-valuenow={meter.value}>
    <Fill className={fillClassName} aria-hidden="true"
      style={{ width: `${meter.percent ?? indeterminateWidth}%` }} />
  </div>
}
