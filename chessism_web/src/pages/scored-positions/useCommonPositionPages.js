import { useEffect, useMemo, useState } from 'react'
import { fetchCommonPositionPage } from './scoredPositionsApi'
import { createCommonPositionLoader } from './commonPositionLoader'

export default function useCommonPositionPages(firstPage, active) {
  const snapshot = firstPage?.snapshot
  const [navigation, setNavigation] = useState({ snapshot, history: [null], index: 0 })
  const [, redraw] = useState(0)
  const loader = useMemo(() => createCommonPositionLoader(fetchCommonPositionPage), [snapshot])
  // Reset synchronously on source changes so an old cursor cannot start a
  // request against the new snapshot in the render before effects run.
  const current = navigation.snapshot === snapshot ? navigation : { snapshot, history: [null], index: 0 }
  const { history, index } = current
  const cursor = history[index]
  const data = index === 0 ? firstPage : loader.get(cursor)
  const nextCursor = data?.status === 'complete' && !data.stale ? data.next_cursor : null
  useEffect(() => {
    const unsubscribe = loader.subscribe(() => redraw(n => n + 1))
    return () => { unsubscribe(); loader.stop() }
  }, [loader])
  useEffect(() => {
    loader.retain(active ? [cursor, nextCursor] : [])
  }, [loader, cursor, nextCursor, active])
  return { data, index,
    first: () => setNavigation({ ...current, index: 0 }),
    previous: () => setNavigation({ ...current, index: Math.max(0, index - 1) }),
    next: () => {
      if (!data?.next_cursor) return
      setNavigation({ snapshot, history: [...history.slice(0, index + 1), data.next_cursor], index: index + 1 })
    }, retry: () => loader.retry(cursor),
  }
}
