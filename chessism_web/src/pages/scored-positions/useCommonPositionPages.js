import { useEffect, useRef, useState } from 'react'
import { fetchCommonPositionPage } from './scoredPositionsApi'

export default function useCommonPositionPages(firstPage, active) {
  const [history, setHistory] = useState([null])
  const [index, setIndex] = useState(0)
  const [response, setResponse] = useState(null)
  const [retryCount, setRetryCount] = useState(0)
  const pages = useRef(new Map())
  const cursor = history[index]
  useEffect(() => {
    pages.current.clear(); setHistory([null]); setIndex(0); setResponse(null)
  }, [firstPage?.snapshot])
  useEffect(() => {
    if (!cursor || !active) return undefined
    const controller = new AbortController()
    let timer
    if (pages.current.has(cursor)) { setResponse({ cursor, data: pages.current.get(cursor) }); return undefined }
    setResponse({ cursor, data: { status: 'pending', rows: [] } })
    const read = async (retry = false) => {
      try {
        const data = await fetchCommonPositionPage(cursor, { signal: controller.signal, retry })
        if (controller.signal.aborted) return
        setResponse({ cursor, data })
        if (data.status === 'complete') pages.current.set(cursor, data)
        if (data.status === 'pending') timer = setTimeout(read, 2000)
      } catch (error) {
        if (!controller.signal.aborted) setResponse({ cursor, data: { status: 'error', detail: error.message || 'Positions unavailable.' } })
      }
    }
    read(retryCount > 0)
    return () => { controller.abort(); clearTimeout(timer) }
  }, [cursor, active, retryCount])
  const data = index === 0 ? firstPage : response?.cursor === cursor ? response.data : null
  return { data, index,
    first: () => setIndex(0), previous: () => setIndex(i => Math.max(0, i - 1)),
    next: () => {
      if (!data?.next_cursor) return
      setHistory(current => [...current.slice(0, index + 1), data.next_cursor])
      setIndex(i => i + 1)
    }, retry: () => { pages.current.delete(cursor); setRetryCount(n => n + 1) },
  }
}
