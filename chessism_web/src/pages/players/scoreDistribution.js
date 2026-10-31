export const SCORE_ENDINGS = [
  ['checkmates_for', 'Checkmates given'],
  ['checkmates_against', 'Checkmates received'],
  ['stalemates_by_player', 'Stalemates · player'],
  ['stalemates_by_opponent', 'Stalemates · opponent'],
  ['other_draws', 'Other draws'],
]

export function scoreInterval(bin) {
  if (bin.lower === null) return `Score < ${bin.upper} cp`
  if (bin.upper === null) return `Score > +${bin.lower} cp`
  return `${bin.lower} ${bin.lower_inclusive ? '≤' : '<'} score ${bin.upper_inclusive ? '≤' : '<'} ${bin.upper} cp`
}

export function combineScoreModes(data, modes) {
  const result = { counts: (data?.bins || []).map(() => 0), endings: {}, cp: 0, unscored: 0, mates: 0, tablebases: 0, mateDistances: [] }
  const distances = new Map()
  for (const mode of modes) {
    const row = data?.by_mode?.[mode]
    if (!row) continue
    row.counts.forEach((value, index) => { result.counts[index] += Number(value) })
    for (const [key, value] of Object.entries(row.endings)) result.endings[key] = (result.endings[key] || 0) + Number(value)
    result.cp += row.cp_positions
    result.unscored += row.unscored_positions
    result.mates += row.mate_positions
    result.tablebases += row.tablebase_positions
    for (const mate of row.mates || []) {
      const entry = distances.get(mate.moves) || { moves: mate.moves, for: 0, against: 0 }
      entry[mate.side] += Number(mate.positions)
      distances.set(mate.moves, entry)
    }
  }
  result.mateDistances = [...distances.values()].sort((a, b) => (a.moves ?? Infinity) - (b.moves ?? Infinity))
  return result
}
