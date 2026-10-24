import { useEffect, useRef, useState } from 'react'
import { fetchTopMainCharacters } from './homeApi.js'
import { PLAYER_FETCH_LIMIT } from './ratingChartData.js'

// Only the chart needs the extra modes. Reuse the precomputed rankings API,
// cache each successful response, and never fetch on arrow navigation.
export default function useRankingModes(enabled, titledOnly) {
  const cache = useRef(new Map())
  const [state, setState] = useState({ scope: null, modes: {}, loading: false, failed: [] })
  const [attempt, setAttempt] = useState(0)
  const scope = titledOnly ? 'titled' : 'untitled'
  useEffect(() => {
    if (!enabled) return undefined
    const controller = new AbortController()
    const modes = cache.current.get(scope) || {}
    const pending = ['rapid', 'bullet'].filter(mode => !Object.hasOwn(modes, mode))
    setState({ scope, modes, loading: pending.length > 0, failed: [] })
    if (!pending.length) return undefined
    Promise.allSettled(pending.map(mode => fetchTopMainCharacters(mode, PLAYER_FETCH_LIMIT,
      { signal: controller.signal, titleStatus: scope }))).then(results => {
      if (controller.signal.aborted) return
      const next = { ...modes }, failed = []
      results.forEach((result, index) => {
        const mode = pending[index]
        if (result.status === 'fulfilled' && Array.isArray(result.value?.players)) next[mode] = result.value.players
        else failed.push(mode)
      })
      cache.current.set(scope, next)
      setState({ scope, modes: next, loading: false, failed })
    })
    return () => controller.abort()
  }, [enabled, scope, attempt])
  return { ...(state.scope === scope ? state : { modes: {}, loading: enabled, failed: [] }),
    retry: () => setAttempt(value => value + 1) }
}
