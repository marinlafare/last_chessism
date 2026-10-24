import React, { useState } from 'react'
import { createRoot } from 'react-dom/client'
import MainCharactersCarousel from '/src/pages/home/MainCharactersCarousel.jsx'
import AppearanceProvider, { AppearanceControls } from '/src/appearance/AppearanceProvider.jsx'
import '/src/index.css'
import '/src/pages/home/home.css'

history.replaceState(null, '', '/')
const state = window.rankingTest = { requests: [], errors: [], selected: [], failBullet: false, empty: false, count: 14 }
window.addEventListener('error', event => state.errors.push(event.message))
window.addEventListener('unhandledrejection', event => state.errors.push(String(event.reason)))
window.fetch = async (input, options = {}) => {
  const url = new URL(input, location.href), mode = url.searchParams.get('time_control')
  const scope = url.searchParams.get('title_status')
  if (options.method && options.method !== 'GET') throw Error('No writes allowed')
  if (url.pathname.endsWith('/appearance')) return new Response(JSON.stringify({ revision: 0, active_preset_id: null,
    presets: [], overrides: [], registry: window.rankingRegistry }), { headers: { 'Content-Type': 'application/json' } })
  if (!url.pathname.endsWith('/players/main_characters/top')) throw Error('No live API access allowed: ' + url.pathname)
  state.requests.push({ mode, scope })
  const players = Array.from({ length: state.empty ? 0 : scope === 'titled' ? state.count : 2 }, (_, index) => ({
    player_name: `${scope}${index}`, full_name: index === 0 ? 'Long Player Name For Wrapping' : `${scope}${index}`,
    avatar: index === 0 ? '/chessism.jpg' : index === 1 ? '/tests/fixtures/ranking-chart.html' : '',
    // The first player's taller Rapid bar exposes all three layers to real
    // pointer tests; later players also exercise fully occluded back bars.
    rating: (mode === 'rapid' && index === 0 ? 3600 : { blitz: 3200, rapid: 2600, bullet: 3400 }[mode]) - index * 30,
  })).filter((_, index) => mode !== 'rapid' || index !== 2).reverse()
  await new Promise(resolve => setTimeout(resolve, mode === 'blitz' ? 50 : 250))
  if (options.signal?.aborted) throw new DOMException('Aborted', 'AbortError')
  return new Response(JSON.stringify(state.failBullet && mode === 'bullet' ? { detail: 'Unavailable' } : { players }),
    { status: state.failBullet && mode === 'bullet' ? 503 : 200, headers: { 'Content-Type': 'application/json' } })
}
function Fixture() {
  const [titled, setTitled] = useState(true)
  const [selected, setSelected] = useState('')
  return <AppearanceProvider account={{ role: 'admin' }} pageKey="/">
    <AppearanceControls />
    <main style={{ maxWidth: 700, padding: 16, margin: '0 auto' }}>
      <MainCharactersCarousel titledOnly={titled} selectedPlayer={selected} onTitleFilterChange={setTitled}
        onSelectPlayer={name => { state.selected.push(name); setSelected(name) }} />
    </main>
  </AppearanceProvider>
}
const root = createRoot(document.getElementById('root'))
let generation = 0
state.reset = () => {
  state.requests = []; state.errors = []; state.selected = []; state.failBullet = false; state.empty = false; state.count = 14
  root.render(<Fixture key={++generation} />)
}
state.reset()
