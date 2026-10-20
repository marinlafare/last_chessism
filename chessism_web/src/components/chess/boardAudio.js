import { DEFAULT_BOARD_SOUND_SET, getBoardSoundSet } from './boardSoundSets'

export function boardMoveSound(move, backwards = false) {
  if (backwards) return 'move'
  const san = String(move?.san || '').replaceAll('0', 'O')
  if (san.includes('#')) return 'mate'
  if (san.includes('+')) return 'check'
  if (/^O-O/.test(san)) return 'castle'
  if (san.includes('x')) return 'capture'
  return 'move'
}

// A separate player from completion notifications: board mute never affects
// backups or other background tasks. Preloading is silent; play is only called
// from a user gesture, not from a render/state effect.
export function createBoardAudio(soundSetId = DEFAULT_BOARD_SOUND_SET, { preload = true } = {}) {
  const soundSet = getBoardSoundSet(soundSetId)
  const clips = new Map()
  let disposed = false
  let active = null
  if (typeof Audio !== 'undefined') {
    for (const file of new Set(Object.values(soundSet.files))) {
      const clip = new Audio()
      clip.preload = preload ? 'auto' : 'none'
      clip.src = `${import.meta.env.BASE_URL}${file}`
      if (preload) clip.load()
      clips.set(file, clip)
    }
  }

  const stop = () => {
    if (!active) return
    active.pause()
    // Seeking before metadata arrives is not supported by every browser.
    try { active.currentTime = 0 } catch { /* Next play retries the seek. */ }
    active = null
  }

  return {
    stop,
    play(name, quiet = false) {
      if (disposed) return Promise.resolve(false)
      stop()
      const clip = clips.get(soundSet.files[name])
      if (!clip) return Promise.resolve(false)
      active = clip
      clip.volume = quiet ? 0.35 : 0.55
      try { clip.currentTime = 0 } catch { /* Metadata may still be loading. */ }
      try {
        // Missing/blocked audio must not interrupt game navigation. A newer
        // move pauses this clip rather than queuing an old sound for later.
        return Promise.resolve(clip.play()).then(() => !disposed && active === clip && !clip.paused, () => false)
      } catch { return Promise.resolve(false) }
    },
    dispose() {
      disposed = true
      stop()
      for (const clip of clips.values()) {
        clip.removeAttribute('src')
        clip.load()
      }
      clips.clear()
    },
  }
}
