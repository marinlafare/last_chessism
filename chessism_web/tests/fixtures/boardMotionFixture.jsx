import React, { useState } from 'react'
import { createRoot } from 'react-dom/client'
import GameReviewBoard from '/src/pages/players/GameReviewBoard.jsx'
import '/src/pages/players/player-game-explorer.css'

history.replaceState(null, '', '/')
const test = window.boardMotionTest = { errors: [], requests: [], animations: [] }
window.addEventListener('error', event => test.errors.push(event.message))
window.addEventListener('unhandledrejection', event => test.errors.push(String(event.reason)))
window.fetch = (...args) => { test.requests.push(String(args[0])); throw Error('No network in this fixture') }
const nativeAnimate = Element.prototype.animate
Element.prototype.animate = function(frames, options) {
  const animation = nativeAnimate.call(this, frames, options)
  test.animations.push({ animation, node: this, frames, options })
  if (test.pause) animation.pause()
  return animation
}
function Fixture() {
  const [state, setState] = useState({ id:'motion', position:'start', orientation:'white', ply:0, animationDuration:200, width:560 })
  test.set = value => setState(previous => ({ ...previous, ...value }))
  const [mounted, setMounted] = useState(true)
  test.mount = setMounted
  return <div style={{ '--game-review-board-size':`${state.width}px`, margin:24 }}>
    {mounted ? <GameReviewBoard {...state} /> : null}
  </div>
}
createRoot(document.getElementById('root')).render(<Fixture />)
