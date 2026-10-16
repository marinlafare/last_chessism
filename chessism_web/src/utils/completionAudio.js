const getAudioContext = () => {
  if (typeof window === 'undefined') return null
  const AudioContextClass = window.AudioContext || window.webkitAudioContext
  return AudioContextClass ? new AudioContextClass() : null
}

const resolveAudioContext = (audioContextRef) => {
  if (typeof window === 'undefined') return null
  const context = audioContextRef?.current || window.__chessismCompletionAudioContext || getAudioContext()
  if (!context) return null
  window.__chessismCompletionAudioContext = context
  if (audioContextRef) audioContextRef.current = context
  return context
}

const unlockCompletionAudio = (audioContextRef) => {
  resolveAudioContext(audioContextRef)?.resume?.()
}

const playCompletionSound = (audioContextRef) => {
  const context = resolveAudioContext(audioContextRef)
  if (!context) return
  context.resume?.()

  const now = context.currentTime
  const notes = [
    { frequency: 246.94, start: 0, duration: 0.16 },
    { frequency: 277.18, start: 0.17, duration: 0.16 },
    { frequency: 293.66, start: 0.34, duration: 0.22 }
  ]

  notes.forEach((note) => {
    const oscillator = context.createOscillator()
    const gain = context.createGain()
    oscillator.type = 'triangle'
    oscillator.frequency.setValueAtTime(note.frequency, now + note.start)
    gain.gain.setValueAtTime(0.0001, now + note.start)
    gain.gain.exponentialRampToValueAtTime(0.12, now + note.start + 0.02)
    gain.gain.exponentialRampToValueAtTime(0.0001, now + note.start + note.duration)
    oscillator.connect(gain)
    gain.connect(context.destination)
    oscillator.start(now + note.start)
    oscillator.stop(now + note.start + note.duration + 0.03)
  })
}

export { playCompletionSound, unlockCompletionAudio }
