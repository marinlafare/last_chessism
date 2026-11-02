import { UndirectedGraph } from 'graphology'
import louvain from 'graphology-communities-louvain'
import forceAtlas2 from 'graphology-layout-forceatlas2'

export const LAYOUT_VERSION = 1
export const ROOT = 'chess:initial'

// Fixed seed and canonical ordering keep snapshots reproducible. The same
// coordinates/community membership are reused across all player/color filters.
export function prepareLayout(snapshot, progress = () => {}) {
  let seed = 1947
  const rng = () => { seed = (Math.imul(1664525, seed) + 1013904223) >>> 0; return seed / 4294967296 }
  const graph = new UndirectedGraph({ allowSelfLoops: false })
  const nodes = [...snapshot.nodes].sort((a,b) => a.key.localeCompare(b.key))
  for (const node of nodes) graph.addNode(node.key, { x: rng() * 100, y: rng() * 100, size: 1 })
  for (const edge of [...snapshot.edges].sort((a,b) => a.key.localeCompare(b.key))) {
    if (edge.source === edge.target || !graph.hasNode(edge.source) || !graph.hasNode(edge.target)) continue
    const weight = Math.log1p(edge.appearances[0])
    if (graph.hasEdge(edge.source, edge.target)) graph.updateEdgeAttribute(edge.source, edge.target, 'weight', n => n + weight)
    else graph.addEdge(edge.source, edge.target, { weight })
  }
  progress('Finding connected communities…')
  // Every game shares the starting node. Do not let that hub define communities.
  const communityGraph = graph.copy()
  if (communityGraph.hasNode(ROOT)) communityGraph.dropNode(ROOT)
  const mapping = communityGraph.size ? louvain(communityGraph, { rng, resolution: 1 })
    : Object.fromEntries(communityGraph.nodes().map((key,i) => [key,i]))
  mapping[ROOT] = 'start'
  const members = new Map()
  for (const node of nodes) {
    const group = mapping[node.key]
    if (!members.has(group)) members.set(group, [])
    members.get(group).push(node.key)
  }
  const groups = [...members.values()].sort((a,b) => a[0].localeCompare(b[0]))
  const membership = {}
  groups.forEach((keys, i) => keys.forEach(key => { membership[key] = i }))
  for (const node of nodes) {
    const angle = 2 * Math.PI * membership[node.key] / Math.max(1, groups.length)
    graph.mergeNodeAttributes(node.key, { x: 100 * Math.cos(angle) + rng() * 10, y: 100 * Math.sin(angle) + rng() * 10 })
  }
  progress('Arranging the opening map…')
  if (graph.size) forceAtlas2.assign(graph, { iterations: 120, settings: {
    ...forceAtlas2.inferSettings(graph), barnesHutOptimize: true, gravity: 1,
    scalingRatio: 10, slowDown: 5, edgeWeightInfluence: 1,
  } })
  const positions = Object.fromEntries(graph.nodes().map(key => {
    const {x,y} = graph.getNodeAttributes(key)
    return [key, { x: Number.isFinite(x) ? x : rng(), y: Number.isFinite(y) ? y : rng(), group: membership[key] }]
  }))
  return { version: LAYOUT_VERSION, revision: snapshot.revision, positions,
    groups: groups.map((keys, i) => ({ id: `group:${i}`, index: i, keys,
      x: keys.reduce((n,k) => n + positions[k].x, 0) / keys.length,
      y: keys.reduce((n,k) => n + positions[k].y, 0) / keys.length })) }
}
