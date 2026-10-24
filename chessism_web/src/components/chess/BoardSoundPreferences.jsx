import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'
import { DEFAULT_BOARD_SOUND_SET, getBoardSoundSet } from './boardSoundSets'

const Context = createContext(null)

function normalizePreferences(value) {
  return {
    soundSet: getBoardSoundSet(value?.soundSet || DEFAULT_BOARD_SOUND_SET).id,
    muted: typeof value?.muted === 'boolean' ? value.muted : false,
  }
}

function readPreferences(key) {
  try {
    const saved = window.localStorage.getItem(key)
    if (saved) return normalizePreferences(JSON.parse(saved))
    // Preserve the old board-mute preference when adopting account settings.
    return normalizePreferences({ muted: window.localStorage.getItem('chessism.boardSounds.muted') === 'true' })
  } catch { return normalizePreferences(null) }
}

export default function BoardSoundPreferences({ accountId, children }) {
  const storageKey = `chessism.boardSounds.user.${accountId}.v1`
  const [preferences, setPreferences] = useState(() => readPreferences(storageKey))
  const [saveError, setSaveError] = useState('')
  const current = useRef(preferences)

  const updatePreferences = useCallback(changes => {
    const next = normalizePreferences({ ...current.current, ...changes })
    current.current = next
    setPreferences(next)
    try {
      window.localStorage.setItem(storageKey, JSON.stringify(next))
      setSaveError('')
    } catch {
      setSaveError('Browser storage is unavailable. These settings will last for this session only.')
    }
  }, [storageKey])

  useEffect(() => {
    const onStorage = event => {
      if (event.key !== storageKey && event.key !== null) return
      const next = readPreferences(storageKey)
      current.current = next
      setPreferences(next)
      setSaveError('')
    }
    window.addEventListener('storage', onStorage)
    return () => window.removeEventListener('storage', onStorage)
  }, [storageKey])

  const toggleMuted = useCallback(() => updatePreferences({ muted: !current.current.muted }), [updatePreferences])
  const value = useMemo(() => ({ ...preferences, saveError, updatePreferences, toggleMuted }),
    [preferences, saveError, updatePreferences, toggleMuted])
  return <Context.Provider value={value}>{children}</Context.Provider>
}

export function useBoardSoundPreferences() {
  const value = useContext(Context)
  if (!value) throw new Error('Board sound preferences require the signed-in settings provider.')
  return value
}
