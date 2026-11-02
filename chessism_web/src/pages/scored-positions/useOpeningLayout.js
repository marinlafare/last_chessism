import { useEffect, useState } from 'react'
const VERSION = 1, memory = new Map()

// Coordinates only; no game lists or account data. Best-effort persistent cache
// survives reopening/reloading the page. Private browsing can use memory only.
async function database() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open('chessism-opening-layouts', 1)
    request.onupgradeneeded = () => request.result.createObjectStore('layouts')
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error)
  })
}
async function saved(key, value) {
  let db
  try {
    db = await database()
    return await new Promise((resolve, reject) => {
      const tx = db.transaction('layouts', value ? 'readwrite' : 'readonly'), store = tx.objectStore('layouts')
      const request = value ? store.put({ value, at: Date.now() }, key) : store.get(key)
      tx.oncomplete = () => resolve(value || request.result?.value)
      tx.onerror = () => reject(tx.error)
      if (value) {
        const all = store.openCursor(), entries = []
        all.onsuccess = () => {
          const cursor = all.result
          if (cursor) { entries.push({ key: cursor.key, at: cursor.value.at }); cursor.continue() }
          else entries.sort((a,b) => b.at - a.at).slice(3).forEach(row => store.delete(row.key))
        }
      }
    })
  } catch { return undefined } finally { db?.close() }
}

export function useOpeningLayout(snapshot) {
  const key = snapshot ? `${VERSION}:${snapshot.revision}` : '', [state, setState] = useState(null)
  const [retry, setRetry] = useState(0)
  useEffect(() => {
    if (!snapshot) return undefined
    let alive = true, worker, timer
    const ready = layout => { if (alive) setState({ key, layout }) }
    setState({ key, message: 'Preparing opening map…' })
    const run = async () => {
      const cached = memory.get(key) || await saved(key)
      if (!alive) return
      if (cached?.version === VERSION && cached.revision === snapshot.revision) { ready(cached); return }
      worker = new Worker(new URL('./openingMap.worker.js', import.meta.url), { type: 'module' })
      const fail = error => { clearTimeout(timer); worker.terminate(); if (alive) setState({ key, error }) }
      timer = setTimeout(() => fail('The map layout took too long. Try again.'), 45000)
      worker.onerror = () => fail('Unable to prepare the map. Try again.')
      worker.onmessage = ({ data }) => {
        if (!alive) return
        if (data.error) { fail(data.error); return }
        if (data.layout) {
          clearTimeout(timer); worker.terminate(); memory.set(key, data.layout)
          while (memory.size > 3) memory.delete(memory.keys().next().value)
          void saved(key, data.layout); ready(data.layout)
        } else setState({ key, message: data.message })
      }
      worker.postMessage(snapshot)
    }
    run().catch(() => { if (alive) setState({ key, error: 'Unable to prepare the map.' }) })
    return () => { alive = false; clearTimeout(timer); worker?.terminate() }
  }, [key, retry])
  return { ...(state?.key === key ? state : {}), retry: () => setRetry(n => n + 1) }
}
