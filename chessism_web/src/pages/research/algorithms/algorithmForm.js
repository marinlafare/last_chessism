export const activeRun = (run) => ['queued', 'running'].includes(run.status)
export const number = (value) => value == null || !Number.isFinite(value) ? 'N/A' : value.toLocaleString(undefined, { maximumFractionDigits: 3 })
export const bytes = (value) => `${number(value / (1024 * 1024))} MiB`
export function duration(seconds) {
  if (seconds == null || !Number.isFinite(seconds)) return '—'
  const total = Math.max(0, Math.floor(seconds))
  return `${Math.floor(total / 3600)}:${String(Math.floor(total / 60) % 60).padStart(2, '0')}:${String(total % 60).padStart(2, '0')}`
}
