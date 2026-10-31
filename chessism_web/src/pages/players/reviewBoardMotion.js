// Presentation only: match adjacent positions without altering any chess data.
const initial = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR'
export function reviewPositionPieces(fen) {
  const result = new Map()
  const ranks = (!fen || fen === 'start' ? initial : fen.split(' ')[0]).split('/')
  ranks.forEach((rank, index) => {
    let file = 0
    for (const symbol of rank) {
      if (/^[1-8]$/.test(symbol)) file += Number(symbol)
      else {
        result.set(`${String.fromCharCode(97 + file++)}${8 - index}`, `${symbol === symbol.toUpperCase() ? 'w' : 'b'}${symbol.toUpperCase()}`)
      }
    }
  })
  return result
}

const distance = (a, b) => (a.charCodeAt(0) - b.charCodeAt(0)) ** 2 + (Number(a[1]) - Number(b[1])) ** 2
function promotion(from, source, to, target) {
  if (source[0] !== target[0]) return false
  const last = source[0] === 'w' ? '8' : '1', penultimate = source[0] === 'w' ? '7' : '2'
  return Math.abs(from.charCodeAt(0) - to.charCodeAt(0)) <= 1 && (
    (source[1] === 'P' && 'QRBN'.includes(target[1]) && from[1] === penultimate && to[1] === last)
    || (target[1] === 'P' && 'QRBN'.includes(source[1]) && to[1] === penultimate && from[1] === last))
}

export function planReviewMotion(before, after) {
  const missing = new Map([...before].filter(([square, piece]) => after.get(square) !== piece))
  const added = [...after].filter(([square, piece]) => before.get(square) !== piece)
  const moves = [], appearing = []
  for (const [to, piece] of added) {
    const candidates = [...missing].filter(([from, previous]) => previous === piece || promotion(from, previous, to, piece))
    candidates.sort((a, b) => Number(b[1] === piece) - Number(a[1] === piece) || distance(a[0], to) - distance(b[0], to))
    const from = candidates[0]?.[0]
    if (from) { moves.push({ from, to }); missing.delete(from) }
    else appearing.push(to)
  }
  return { moves, appearing, disappearing: [...missing.keys()] }
}

const piecesOnly = nodes => new Map([...nodes].map(([square, entry]) => [square, entry.piece]))
const samePosition = (nodes, target) => nodes.size === target.size && [...target].every(([square, piece]) => nodes.get(square)?.piece === piece)
function collect(root) {
  return new Map([...root.querySelectorAll('.game-review-piece-skin')].map(node => {
    const square = node.closest('[data-square]')?.dataset.square
    return [square, { node, piece: node.querySelector('[data-piece]')?.dataset.piece }]
  }).filter(([square, entry]) => square && entry.piece))
}

// The board library snaps on interrupted animations. Let it commit positions
// immediately; animate our existing skins with the browser's compositor instead.
// No React updates or layout measurements occur on individual animation frames.
export function createReviewBoardMotion(root, overlay) {
  let config, pending, completion, synchronizing = false, destroyed = false
  const running = new Map(), ghosts = new Set()
  const settled = () => {
    if (destroyed || synchronizing || pending || running.size) return
    const callback = completion
    completion = null
    callback?.()
  }
  const cancel = node => {
    const entry = running.get(node)
    if (entry) { entry.animation.cancel(); entry.finish() }
  }
  const clear = () => {
    for (const node of [...running.keys()]) cancel(node)
    for (const node of ghosts) node.remove()
    ghosts.clear()
  }
  function animate(node, frames, duration, fade = false, remove = false) {
    cancel(node)
    if (!node.animate || duration <= 0) {
      if (remove) { ghosts.delete(node); node.remove() }
      settled()
      return
    }
    const previous = { willChange: node.style.willChange, zIndex: node.style.zIndex }
    node.style.willChange = fade ? 'opacity' : 'transform'
    node.style.zIndex = remove ? '5' : '6'
    const animation = node.animate(frames, {
      duration, easing: fade ? 'ease-out' : 'cubic-bezier(0.65, 0, 0.35, 1)',
    })
    const finish = () => {
      if (running.get(node) !== entry) return
      running.delete(node)
      Object.assign(node.style, previous)
      if (remove) { ghosts.delete(node); node.remove() }
      settled()
    }
    const entry = { animation, finish }
    running.set(node, entry)
    animation.onfinish = finish
    animation.oncancel = finish
  }
  function synchronize() {
    if (destroyed || !pending) return
    const after = collect(root)
    if (!samePosition(after, pending.target)) return
    const { before, duration, enabled, onSettled } = pending
    pending = null
    completion = onSettled
    if (!enabled || !before.size) { settled(); return }
    synchronizing = true
    const plan = planReviewMotion(piecesOnly(before), piecesOnly(after))
    // React may reuse a captured piece's skin for its captor. Cancel OLD
    // effects first, so removing the capture cannot cancel the new move.
    for (const [square, entry] of before) if (after.get(square)?.piece !== entry.piece) cancel(entry.node)
    // Read all destinations before writing animation styles (one layout pass).
    for (const entry of after.values()) {
      entry.rect = entry.node.getBoundingClientRect()
      entry.opacity = getComputedStyle(entry.node).opacity
    }
    const bounds = overlay.getBoundingClientRect()
    for (const { from, to } of plan.moves) {
      const source = before.get(from), destination = after.get(to)
      const dx = source.rect.left - destination.rect.left, dy = source.rect.top - destination.rect.top
      if (Math.abs(dx) + Math.abs(dy) < 0.5) continue
      animate(destination.node, [{ transform: `translate3d(${dx}px, ${dy}px, 0)` }, { transform: 'translate3d(0, 0, 0)' }], duration)
    }
    for (const square of plan.appearing) {
      const { node, opacity } = after.get(square)
      animate(node, [{ opacity: 0 }, { opacity }], Math.min(120, duration), true)
    }
    for (const square of plan.disappearing) {
      const { clone, rect, opacity } = before.get(square)
      if (!clone) continue
      Object.assign(clone.style, {
        position: 'absolute', left: `${rect.left - bounds.left}px`, top: `${rect.top - bounds.top}px`,
        width: `${rect.width}px`, height: `${rect.height}px`, margin: '0', transform: 'none', boxSizing: 'border-box',
      })
      clone.setAttribute('aria-hidden', 'true')
      overlay.append(clone)
      ghosts.add(clone)
      animate(clone, [{ opacity }, { opacity: 0 }], Math.min(120, duration), true, true)
    }
    // Detached source nodes must not retain animation callbacks.
    for (const node of [...running.keys()]) if (!root.contains(node) && !ghosts.has(node)) cancel(node)
    synchronizing = false
    settled()
  }
  const observer = new MutationObserver(synchronize)
  observer.observe(root, { childList: true, subtree: true })
  return {
    update(next) {
      // Superseded navigation must never play its delayed sound.
      completion = null
      const before = collect(root), target = reviewPositionPieces(next.position)
      const enabled = Boolean(config && config.id === next.id && config.orientation === next.orientation
        && config.width === next.width && next.duration > 0 && Math.abs(next.ply - config.ply) === 1)
      // Snapshot the *visible* positions before the board library commits. This
      // rebases rapid forward/back steps from mid-flight, never from old squares.
      for (const [square, entry] of before) {
        entry.rect = entry.node.getBoundingClientRect()
        entry.opacity = getComputedStyle(entry.node).opacity
        if (enabled && target.get(square) !== entry.piece) entry.clone = entry.node.cloneNode(true)
      }
      if (!enabled) clear()
      else for (const node of [...ghosts]) cancel(node)
      config = next
      pending = { before, target, enabled, duration: next.duration, onSettled: next.onSettled }
      synchronize()
    },
    destroy() { destroyed = true; observer.disconnect(); clear(); pending = null },
  }
}
