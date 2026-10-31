// Display the review's existing assessment; never infer a rating from a score.
const badges = {
  best: { symbol: '!', label: 'Best' },
  good: { symbol: '✓', label: 'Good' },
  inaccuracy: { symbol: '?!', label: 'Inaccuracy' },
  mistake: { symbol: '?', label: 'Mistake' },
  blunder: { symbol: '??', label: 'Blunder' },
}

export function reviewMoveBadge(move, orientation = 'white') {
  const book = Boolean(move?.book)
  const badge = book ? { label: 'Book', title: ['Book move', ...(move.book.openings || []).map(o => `${o.name} (${o.eco})`)].join(' · ') }
    : badges[move?.classification]
  // UCI supplies the destination even for captures, castling and promotions.
  // Missing/invalid move data must not attach a badge to an arbitrary piece.
  if (!badge || !/^[a-h][1-8][a-h][1-8][qrbn]?$/.test(move?.uci || '')) return null
  const square = move.uci.slice(2, 4)
  const file = square.charCodeAt(0) - 97, rank = Number(square[1]) - 1
  return { ...badge, classification: book ? 'book' : move.classification, square,
    column: orientation === 'black' ? 7 - file : file,
    row: orientation === 'black' ? rank : 7 - rank }
}
