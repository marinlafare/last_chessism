// UCI scores are relative to the side to move. The UI uses White's perspective.
export function parseAnalysisInfo(text, fen) {
  if (!text.startsWith('info ') || text.startsWith('info string ')) return null
  const score = text.match(/\bscore (cp|mate) (-?\d+)\b/)
  const depth = text.match(/\bdepth (\d+)\b/)
  if (!score || !depth || /\b(?:lowerbound|upperbound)\b/.test(text)) return null
  const perspective = fen.split(/\s+/)[1] === 'b' ? -1 : 1
  const value = Number(score[2])
  const mate = score[1] === 'mate'
  const numeric = mate ? (value > 0 ? 10000 - value : -10000 - value) : value
  return {
    score: numeric * perspective,
    scoreType: score[1],
    mate: mate ? value * perspective : null,
    depth: Number(depth[1]),
    multipv: Number(text.match(/\bmultipv (\d+)\b/)?.[1] || 1),
    nodes: Number(text.match(/\bnodes (\d+)\b/)?.[1] || 0),
    pv: text.match(/\bpv (.+)$/)?.[1].trim().split(/\s+/) || [],
  }
}

export function analysisResult(fen, lines) {
  return { fen, analysis: [...lines.values()].sort((a, b) => a.multipv - b.multipv) }
}
