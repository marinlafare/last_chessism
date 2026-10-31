import { formatNumber } from '../../utils/formatters.js'

export function formatPlayerCount(value) {
  const number = Number(value ?? 0)
  return Number.isFinite(number) && number >= 1e6 ? `${(number / 1e6).toFixed(1)} M` : formatNumber(number)
}
