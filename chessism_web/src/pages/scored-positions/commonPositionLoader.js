// Share a single request/poll loop between prefetch and navigation. The caller
// retains only the visible page and ONE lookahead: never crawl the whole corpus.
export function createCommonPositionLoader(fetchPage) {
  const pages = new Map(), requests = new Map(), listeners = new Set()
  let wanted = new Set()
  const notify = () => listeners.forEach(listener => listener())
  const cancel = cursor => {
    const request = requests.get(cursor)
    if (!request) return
    request.controller.abort(); clearTimeout(request.timer); requests.delete(cursor)
    if (pages.get(cursor)?.status === 'pending') pages.delete(cursor)
  }
  const load = (cursor, retry = false) => {
    if (requests.has(cursor) || (!retry && pages.has(cursor))) return
    const request = { controller: new AbortController(), polls: 0, timer: null }
    requests.set(cursor, request)
    pages.set(cursor, { status: 'pending', rows: [] }); notify()
    const read = async (retryRequest = false) => {
      try {
        const data = await fetchPage(cursor, { signal: request.controller.signal, retry: retryRequest })
        if (request.controller.signal.aborted) return
        pages.set(cursor, data)
        if (data.status === 'pending') {
          // Fast short-job detection, bounded polling if the worker is busy.
          request.timer = setTimeout(read, Math.min(2000, 500 * 2 ** Math.min(request.polls++, 2)))
        } else {
          requests.delete(cursor)
          // Keep browser memory bounded even after browsing thousands of FENs.
          if (pages.size > 64) {
            const oldest = [...pages.keys()].find(key => !wanted.has(key) && !requests.has(key))
            if (oldest) pages.delete(oldest)
          }
        }
        notify()
      } catch (error) {
        if (request.controller.signal.aborted) return
        requests.delete(cursor)
        pages.set(cursor, { status: 'error', detail: error.message || 'Positions unavailable.' }); notify()
      }
    }
    read(retry)
  }
  return {
    get: cursor => pages.get(cursor),
    subscribe: listener => { listeners.add(listener); return () => listeners.delete(listener) },
    retain: cursors => {
      wanted = new Set(cursors.filter(Boolean))
      for (const cursor of requests.keys()) if (!wanted.has(cursor)) cancel(cursor)
      for (const cursor of wanted) load(cursor)
    },
    retry: cursor => { if (cursor) { cancel(cursor); pages.delete(cursor); load(cursor, true) } },
    stop: () => { for (const cursor of requests.keys()) cancel(cursor) },
  }
}
