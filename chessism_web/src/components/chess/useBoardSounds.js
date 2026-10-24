import { useCallback, useEffect, useRef } from 'react'
import { boardMoveSound, createBoardAudio } from './boardAudio'
import { useBoardSoundPreferences } from './BoardSoundPreferences'

export default function useBoardSounds(gameId) {
  const { muted, soundSet, toggleMuted: togglePreference } = useBoardSoundPreferences()
  const audioRef = useRef(null)

  useEffect(() => {
    if (!gameId) return undefined
    const audio = createBoardAudio(soundSet)
    audioRef.current = audio
    const onVisibility = () => { if (document.hidden) audio.stop() }
    document.addEventListener('visibilitychange', onVisibility)
    return () => {
      audioRef.current = null
      document.removeEventListener('visibilitychange', onVisibility)
      audio.dispose()
    }
  }, [gameId, soundSet])

  useEffect(() => { if (muted) audioRef.current?.stop() }, [muted])

  const toggleMuted = useCallback(() => {
    if (!muted) audioRef.current?.stop()
    togglePreference()
  }, [muted, togglePreference])

  const playMove = useCallback((move, backwards = false) => {
    if (!muted && !document.hidden) {
      audioRef.current?.play(boardMoveSound(move, backwards), backwards)
    }
  }, [muted])

  return { muted, toggleMuted, playMove }
}
