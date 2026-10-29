import React, { useState } from 'react'
import { createRoot } from 'react-dom/client'
import PlayerGameReviewModal from '/src/pages/players/PlayerGameReviewModal.jsx'
import BoardSoundPreferences from '/src/components/chess/BoardSoundPreferences.jsx'
import AppearanceProvider, { AppearanceControls } from '/src/appearance/AppearanceProvider.jsx'
import '/src/index.css'
import '/src/pages/players/player-game-explorer.css'

history.replaceState(null, '', '/')
const state = window.gameReviewTest = { requests: [], unexpected: [], errors: [], sounds: 0 }
const styleKey = 'game-review-styles-fixture'
window.addEventListener('error', event => state.errors.push(event.message))
window.addEventListener('unhandledrejection', event => state.errors.push(String(event.reason)))
window.Audio = class {
  load() {} pause() {} removeAttribute() {}
  play() { state.sounds++; return Promise.resolve() }
}
const initial = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1'
const moves = [
  { ply: 1, move_number: 1, move_color: 'white', san: 'e4', classification: 'best', move_accuracy: 99,
    engine_line_before: ['e4', 'e5'],
    fen_after: 'rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq - 0 1',
    clock: { white_time_left: 175, black_time_left: 178 },
    evaluation_after: { white_cp: 100, white_win_percent: 60, kind: 'cp' } },
  { ply: 2, move_number: 1, move_color: 'black', san: 'e5', classification: 'inaccuracy', move_accuracy: 80,
    engine_line_before: ['e5', 'Nf3'],
    fen_after: 'rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq - 0 2',
    clock: { white_time_left: 175, black_time_left: 178 },
    evaluation_after: { white_cp: 100, white_win_percent: 60, kind: 'cp' } },
  { ply: 3, move_number: 2, move_color: 'white', san: 'Nf3', classification: 'mistake',
    fen_after: 'rnbqkbnr/pppp1ppp/8/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R b KQkq - 1 2' },
  { ply: 4, move_number: 2, move_color: 'black', san: 'Nc6', classification: 'blunder',
    fen_after: 'r1bqkbnr/pppp1ppp/2n5/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R w KQkq - 2 3' },
  { ply: 5, move_number: 3, move_color: 'white', san: 'Bc4', classification: 'good',
    fen_after: 'r1bqkbnr/pppp1ppp/2n5/4p3/2B1P3/5N2/PPPP1PPP/RNBQK2R b KQkq - 3 3' },
  { ply: 6, move_number: 3, move_color: 'black', san: 'Nf6', classification: null,
    fen_after: 'r1bqkb1r/pppp1ppp/2n2n2/4p3/2B1P3/5N2/PPPP1PPP/RNBQK2R w KQkq - 4 4' },
]
window.fetch = async (input, options = {}) => {
  const pathname = new URL(input, location.href).pathname
  state.requests.push(pathname)
  if (window.gameReviewRegistry && /\/appearance(?:\/save)?$/.test(pathname)) {
    let saved = { revision: 1, active_preset_id: 'official', default_preset_id: 'official', presets: [{ id: 'official', name: 'Default' }],
      overrides: [{ scope: 'home', target: 'title', property: 'color', value: '#abc123' },
        { scope: 'home', target: 'value', property: 'color', value: '#123abc' },
        { scope: 'home', target: 'panel', property: 'background', value: '#102030' }],
      ...JSON.parse(localStorage.getItem(styleKey) || 'null') }
    if (options.method === 'POST') {
      const payload = JSON.parse(options.body)
      if (payload.preset_id !== 'official' || payload.revision !== saved.revision) throw Error('Unexpected style save')
      saved = { ...saved, revision: saved.revision + 1, overrides: payload.overrides }
      localStorage.setItem(styleKey, JSON.stringify(saved))
    }
    return new Response(JSON.stringify({ ...saved, registry: window.gameReviewRegistry }), { headers: { 'Content-Type': 'application/json' } })
  }
  if (/\/games\/[12]\/score$/.test(pathname)) return new Response(JSON.stringify({
    game: { initial_fen: initial, time_control: '180+2', mode: 'blitz', played_at_utc: '2026-10-23T12:00:00Z',
      white: { name: 'WhitePlayer', rating: 2400 }, black: { name: 'BlackPlayer', rating: 2300 },
      result: { white: 1, black: 0, white_reason: 'win', black_reason: 'resigned' } },
    player_summaries: [{ player_name: 'WhitePlayer', accuracy: 92, blunder_count: 1 },
      { player_name: 'BlackPlayer', accuracy: 58, blunder_count: 3 }], moves,
    ...(state.coverage ? { blunder_coverage: state.coverage, blunder_annotations_pending: !!state.pending } : {}),
  }), { headers: { 'Content-Type': 'application/json' } })
  state.unexpected.push(pathname)
  throw Error('No live access allowed: ' + pathname)
}
function Fixture() {
  const [selection, setSelection] = useState({ gameId: 1, playerName: 'WhitePlayer' })
  state.select = setSelection
  return <BoardSoundPreferences accountId="game-review-fixture">
    <style>{window.gameReviewStyles || ''}</style>
    {window.gameReviewRegistry ? <><AppearanceControls />
      <h2 data-style-scope="home" data-style-id="title" id="outside-title">Unrelated title</h2>
      <strong data-style-scope="home" data-style-id="value" id="outside-value">Unrelated value</strong>
      <section data-style-scope="home" data-style-id="panel" id="outside-panel">Unrelated panel</section>
    </> : null}
    <PlayerGameReviewModal {...selection} onClose={() => setSelection(current => ({ ...current, gameId: null }))} />
  </BoardSoundPreferences>
}
createRoot(document.getElementById('root')).render(window.gameReviewRegistry
  ? <AppearanceProvider account={{ role: 'admin' }} pageKey="/"><Fixture /></AppearanceProvider> : <Fixture />)
