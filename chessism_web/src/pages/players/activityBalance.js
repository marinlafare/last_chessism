// Balance controls color intensity; game volume separately controls opacity.
// The base palette remains user-owned.
export function balanceOf(row = {}) {
  const games = Number(row.games || 0)
  return games > 0 ? (Number(row.wins || 0) - Number(row.losses || 0)) / games : null
}

export function balanceScale(rows) {
  const balances = rows.map(balanceOf).filter(value => value !== null)
  const positive = Math.max(0, ...balances), negative = Math.min(0, ...balances)
  return row => {
    const balance = balanceOf(row)
    if (balance === null) return { kind: 'empty', balance: null, intensity: 0 }
    if (balance === 0) return { kind: 'balanced', balance, intensity: 1 }
    const step = 1 / Number(row.games), maximum = balance > 0 ? positive : -negative
    const fraction = maximum > step ? Math.max(0, Math.min(1, (Math.abs(balance) - step) / (maximum - step))) : 0
    return { kind: balance > 0 ? 'positive' : 'negative', balance, intensity: 0.25 + 0.75 * fraction }
  }
}

export function balanceDescription(row) {
  const balance = balanceOf(row)
  return balance === null ? 'No games' : `Win–loss balance: ${balance > 0 ? '+' : ''}${(balance * 100).toFixed(1)}%`
}

export function volumeOpacityScale(rows = []) {
  const maximum = Math.max(0, ...rows.map(row => Number(row.games || 0)))
  return (row = {}) => maximum > 0 ? Math.min(1, Math.max(0, Number(row.games || 0) / maximum)) : 0
}
