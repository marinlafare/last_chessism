import { useCallback, useEffect, useRef, useState } from 'react'
import { fetchMatrixDefinitions } from './matrixApi'
import { mergeDefinitions } from './matrixForm'

export default function useMatrixDefinitions() {
  const [state, setState] = useState({ definitions: [], hasMore: false, legacyCount: 0, loading: true, error: '' })
  const rows = useRef([])
  const request = useRef(null)
  const mounted = useRef(false)

  const cancel = useCallback(() => {
    request.current?.abort()
    request.current = null
  }, [])

  const load = useCallback(async (append = false) => {
    if (!mounted.current) return
    // Duplicate load-more clicks cannot request the same offset twice.
    if (append && request.current) return
    cancel()
    const controller = new AbortController()
    request.current = controller
    setState((current) => ({ ...current, loading: true, error: '' }))
    try {
      const payload = await fetchMatrixDefinitions({ offset: append ? rows.current.length : 0, signal: controller.signal })
      if (controller.signal.aborted) return
      rows.current = mergeDefinitions(append ? rows.current : [], payload.definitions || [])
      setState({ definitions: rows.current, hasMore: Boolean(payload.has_more), legacyCount: payload.legacy_snapshot_count || 0, loading: false, error: '' })
    } catch (error) {
      if (!controller.signal.aborted) setState((current) => ({ ...current, loading: false, error: error.message || 'Unable to load definitions.' }))
    } finally {
      if (request.current === controller) request.current = null
    }
  }, [cancel])

  useEffect(() => {
    mounted.current = true
    load()
    return () => { mounted.current = false; cancel() }
  }, [load, cancel])

  // Successful writes already return authoritative metadata. Avoid a second
  // request, and prevent an older list response from undoing the local change.
  const updateRows = (next) => {
    cancel()
    rows.current = next
    setState((current) => ({ ...current, definitions: next, loading: false, error: '' }))
  }

  return {
    ...state, load,
    insert: (definition) => updateRows(mergeDefinitions([definition], rows.current)),
    forget: (id) => updateRows(rows.current.filter((definition) => definition.id !== id)),
  }
}
