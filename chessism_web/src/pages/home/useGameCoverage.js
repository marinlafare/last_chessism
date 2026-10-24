import { useEffect, useState } from 'react'
import { fetchPlayerPositionStats } from '../players/playerApi.js'

const cache = new Map()
const CACHE_MS = 60000
const CACHE_LIMIT = 128

// Reuse the hero's fully-scored-game counts, only for the visible chart page.
// Missing/failed counts stay unknown; they must never look like zero coverage.
export default function useGameCoverage(players) {
  const namesKey = JSON.stringify(players.map(player => player.player_name))
  const [coverage, setCoverage] = useState({})

  useEffect(() => {
    const names = JSON.parse(namesKey)
    let disposed = false
    let controller = null
    let timer
    let deadline
    setCoverage(Object.fromEntries(names.map(name => [name, cache.get(name) || {}])))

    const load = async () => {
      if (disposed || controller) return
      window.clearTimeout(timer)
      const needed = names.filter(name => Date.now() - (cache.get(name)?.refreshedAt || 0) >= CACHE_MS)
      if (document.hidden || !needed.length) {
        if (names.length) timer = window.setTimeout(load, CACHE_MS)
        return
      }
      controller = new AbortController()
      const request = controller
      deadline = window.setTimeout(() => request.abort(), 20000)
      await Promise.all(needed.map(async name => {
        try {
          const payload = await fetchPlayerPositionStats(name, { signal: request.signal })
          if (disposed || request.signal.aborted) return
          if (!Number.isSafeInteger(payload.analyzed_games) || payload.analyzed_games < 0) {
            throw new Error('Invalid scored-game count')
          }
          const entry = { scoredGames: payload.analyzed_games, refreshedAt: Date.now() }
          cache.delete(name)
          cache.set(name, entry)
          if (cache.size > CACHE_LIMIT) cache.delete(cache.keys().next().value)
          setCoverage(current => ({ ...current, [name]: entry }))
        } catch {
          if (!disposed) setCoverage(current => ({
            ...current, [name]: { ...current[name], unavailable: true },
          }))
        }
      }))
      window.clearTimeout(deadline)
      controller = null
      if (!disposed) timer = window.setTimeout(load, CACHE_MS)
    }
    const onVisibilityChange = () => { if (!document.hidden) load() }
    load()
    document.addEventListener('visibilitychange', onVisibilityChange)
    return () => {
      disposed = true
      window.clearTimeout(timer)
      window.clearTimeout(deadline)
      controller?.abort()
      document.removeEventListener('visibilitychange', onVisibilityChange)
    }
  }, [namesKey])

  return coverage
}
