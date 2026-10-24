const lichessSet = (id, name, description) => ({
  id, name, description, provider: 'Lichess · Enigmahack',
  source: 'https://github.com/lichess-org/lila/tree/79bf4470f703c40c53973c6092b5363006ed3df2/public/sound/' + id,
  files: Object.fromEntries(Object.entries({
    move: 'Move', capture: 'Capture', castle: 'Move', check: 'Check', mate: 'Check',
  }).map(([event, file]) => [event, `sounds/chess/lichess/${id}/${file}.mp3`])),
})

const impactSet = (id, name, description) => ({
  id, name, description, provider: 'Kenney · CC0',
  source: 'https://kenney.nl/assets/impact-sounds',
  files: Object.fromEntries(['move', 'capture', 'castle', 'check', 'mate']
    .map(event => [event, `sounds/chess/kenney/${id}/${event}.mp3`])),
})

export const BOARD_SOUND_SETS = [
  lichessSet('piano', 'Piano', 'Soft piano notes.'),
  lichessSet('nes', 'NES', 'Retro 8-bit sounds.'),
  lichessSet('sfx', 'SFX', 'Distinct sound effects.'),
  lichessSet('futuristic', 'Futuristic', 'Electronic, space-like sounds.'),
  impactSet('glass', 'Glass', 'Clear glass taps.'),
  impactSet('metal', 'Metal', 'Metallic taps.'),
  impactSet('soft', 'Soft', 'Soft, muted impacts.'),
  {
    id: 'wooden', name: 'Wooden', description: 'The previous Chessism sound set.', provider: 'Kenney · CC0',
    source: 'https://kenney.nl/assets/impact-sounds',
    files: Object.fromEntries(['move', 'capture', 'castle', 'check', 'mate'].map(event => [event, `sounds/chess/${event}.mp3`])),
  },
]

export const DEFAULT_BOARD_SOUND_SET = 'piano'
export function getBoardSoundSet(id) {
  return BOARD_SOUND_SETS.find(set => set.id === id) || BOARD_SOUND_SETS[0]
}
