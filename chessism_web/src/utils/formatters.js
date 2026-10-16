export const formatNumber = (value, fractionDigits = null) => {
  const numeric = Number(value ?? 0)
  if (!Number.isFinite(numeric)) return '0'

  const options = Number.isInteger(fractionDigits)
    ? { minimumFractionDigits: fractionDigits, maximumFractionDigits: fractionDigits }
    : undefined

  return numeric.toLocaleString('en-US', options)
}
