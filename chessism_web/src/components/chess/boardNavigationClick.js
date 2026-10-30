// A tiny, quiet tick for interrupted/rapid navigation; one voice, no sound queue.
export function createBoardNavigationClick({
  AudioContext = globalThis.AudioContext || globalThis.webkitAudioContext,
} = {}) {
  let context, active, disposed = false, generation = 0
  const stop = () => {
    generation++
    if (!active) return
    try { active.stop() } catch { /* Already finished. */ }
    active = null
  }
  return {
    stop,
    async play() {
      if (disposed || !AudioContext) return
      const current = ++generation
      try {
        context ||= new AudioContext()
        if (context.state === 'suspended') await context.resume()
        if (disposed || current !== generation || context.state !== 'running') return
        if (active) { try { active.stop() } catch { /* Already finished. */ } }
        const oscillator = context.createOscillator(), gain = context.createGain()
        const start = context.currentTime
        oscillator.type = 'triangle'
        oscillator.frequency.setValueAtTime(1200, start)
        gain.gain.setValueAtTime(0, start)
        gain.gain.linearRampToValueAtTime(0.06, start + 0.001)
        gain.gain.exponentialRampToValueAtTime(0.0001, start + 0.018)
        oscillator.connect(gain)
        gain.connect(context.destination)
        oscillator.onended = () => {
          oscillator.disconnect()
          gain.disconnect()
          if (active === oscillator) active = null
        }
        active = oscillator
        oscillator.start(start)
        oscillator.stop(start + 0.02)
      } catch { /* Unsupported/blocked audio must never block navigation. */ }
    },
    dispose() {
      disposed = true
      stop()
      context?.close().catch(() => {})
    },
  }
}
