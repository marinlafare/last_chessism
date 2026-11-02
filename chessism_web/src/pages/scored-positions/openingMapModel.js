export const ROOT = 'chess:initial'
export const PALETTE = { a: '#e49b51', b: '#688be7', both: '#be74d7', other: '#73898d', root: '#e6e2d4',
  community0: '#53b995', community1: '#6797de', community2: '#d5b359', community3: '#b27bd2',
  community4: '#dc7f85', community5: '#69bdca', community6: '#91b65c', community7: '#c7956c' }
export function slotIndex(snapshot, scope, color) {
  return snapshot.slots.indexOf(scope === 'all' || color === 'all' ? scope : `${scope}_${color}`)
}
export function comparisonColor(a, b, palette) { return palette[a && b ? 'both' : a ? 'a' : b ? 'b' : 'other'] }
const comparisonKey = (a,b) => a && b ? 'both' : a ? 'a' : b ? 'b' : 'other'

export function mapView(snapshot, layout, options, palette = PALETTE) {
  const { scope, color, minimum, community, colorBy } = options
  const slot = slotIndex(snapshot, scope, color), aSlot = slotIndex(snapshot, 'a', color), bSlot = slotIndex(snapshot, 'b', color)
  const visible = snapshot.nodes.filter(node => node.key === ROOT ? snapshot.coverage.games[slot] > 0 : node.games[slot] >= minimum)
  const byKey = new Map(visible.map(node => [node.key, node])), groups = new Map()
  for (const node of visible) {
    const pos = layout.positions[node.key]
    if (!pos) continue
    const id = `group:${pos.group}`
    if (!groups.has(id)) groups.set(id, { ...layout.groups[pos.group], members: [], appearances: 0, a: 0, b: 0 })
    const group = groups.get(id)
    group.members.push(node.key); group.appearances += node.appearances[slot]
    group.a += node.games[aSlot]; group.b += node.games[bSlot]
  }
  let nodes, edges
  if (community === null) {
    nodes = [...groups.values()].map(group => ({ key: group.id, x: group.x, y: group.y,
      label: group.members.length === 1 && group.members[0] === ROOT ? 'Start' : `Group ${group.index + 1}`,
      description: `${group.members.length.toLocaleString()} positions · ${group.appearances.toLocaleString()} position appearances`,
      size: 3 + 1.5 * Math.log2(1 + group.members.length),
      color: colorBy === 'players' ? comparisonColor(group.a, group.b, palette) : palette[`community${group.index % 8}`],
      colorKey: colorBy === 'players' ? comparisonKey(group.a,group.b) : `community${group.index % 8}`,
      members: group.members, weight: group.appearances }))
    const connections = new Map()
    for (const edge of snapshot.edges) {
      if (!edge.appearances[slot] || !byKey.has(edge.source) || !byKey.has(edge.target)) continue
      const source = `group:${layout.positions[edge.source].group}`, target = `group:${layout.positions[edge.target].group}`
      if (source === target) continue
      const key = `${source}>${target}`
      if (!connections.has(key)) connections.set(key, { key, source, target, weight: 0 })
      connections.get(key).weight += edge.appearances[slot]
    }
    edges = [...connections.values()]
  } else {
    // At most 500 nodes per detail view. Frequency threshold and paging controls
    // remain explicit, while one-hop expansion is available in the board panel.
    const members = (groups.get(community)?.members || []).sort((a,b) => byKey.get(b).games[slot] - byKey.get(a).games[slot] || a.localeCompare(b))
    nodes = members.slice(0, 500).map(key => {
      const row = byKey.get(key), pos = layout.positions[key], root = key === ROOT
      return { key, ...pos, label: root ? 'Start' : `FEN ${row.fen_id}`,
        description: root ? `${snapshot.coverage.games[slot].toLocaleString()} game starts (not counted as played positions)`
          : `${row.games[slot].toLocaleString()} games · ${row.appearances[slot].toLocaleString()} appearances`,
        size: root ? 9 : 3 + Math.log2(1 + row.games[slot]), weight: row.games[slot],
        colorKey: root ? 'root' : colorBy === 'players' ? comparisonKey(row.games[aSlot],row.games[bSlot]) : `community${pos.group % 8}`,
        color: root ? palette.root : colorBy === 'players' ? comparisonColor(row.games[aSlot], row.games[bSlot], palette) : palette[`community${pos.group % 8}`] }
    })
    const keys = new Set(nodes.map(node => node.key))
    edges = snapshot.edges.filter(e => keys.has(e.source) && keys.has(e.target) && e.appearances[slot] > 0)
      .map(e => ({ ...e, weight: e.appearances[slot] }))
  }
  const frame = community === null ? layout.groups : (layout.groups.find(g => g.id === community)?.keys || []).map(k => layout.positions[k])
  const xs = frame.map(p => p.x), ys = frame.map(p => p.y)
  const x0 = Math.min(0,...xs), x1 = Math.max(1,...xs), y0 = Math.min(0,...ys), y1 = Math.max(1,...ys)
  return { nodes, edges, totalPositions: visible.length, frameKey: community || 'overview',
    bounds: { x: [x0,x1], y: [y0,y1] },
    detailLimited: community !== null && (groups.get(community)?.members.length || 0) > 500,
    groups: [...groups.values()], slot }
}
