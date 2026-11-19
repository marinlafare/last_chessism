import { useEffect, useRef, useState } from 'react'
import { inspectPlayerGameScope, previewPlayerGameAnalysis } from './positionsApi'

const INITIAL = {
  playerName: '', selectionMode: 'latest', dateFrom: '', dateTo: '', gameLimit: 200,
  useAll: true, rangeOrder: 'latest', batchSize: 500, coolOff: 120,
  scope: null, preview: null, loadingScope: false, loadingPreview: false, error: '',
}
const SCOPE_FIELDS = ['playerName', 'selectionMode', 'dateFrom', 'dateTo']
const PLAN_FIELDS = [...SCOPE_FIELDS, 'gameLimit', 'useAll', 'rangeOrder']
const scopePayload = form => ({
  player_name: form.playerName.trim().toLowerCase(), selection_mode: form.selectionMode,
  date_from: form.selectionMode === 'range' ? form.dateFrom || null : null,
  date_to: form.selectionMode === 'range' ? form.dateTo || null : null,
})

// A revision, not player-name equality, owns each asynchronous plan. Going back
// to Alice cannot revive a request from an earlier visit to Alice.
export function usePlayerGameSelection() {
  const [form, render] = useState(INITIAL)
  const current = useRef(INITIAL)
  const revision = useRef(0)
  const request = useRef(null)
  const alive = useRef(true)
  const publish = value => { current.current = value; render(value) }
  const invalidate = () => { revision.current++; request.current?.abort(); request.current = null }
  useEffect(() => {
    alive.current = true
    return () => { alive.current = false; invalidate() }
  }, [])

  const setForm = update => {
    const previous = current.current
    let next = typeof update === 'function' ? update(previous) : update
    const scopeChanged = SCOPE_FIELDS.some(key => next[key] !== previous[key])
    if (PLAN_FIELDS.some(key => next[key] !== previous[key]) || (previous.preview && !next.preview)) {
      invalidate()
      next = { ...next, preview: null, loadingPreview: false, loadingScope: false, error: '',
        scope: scopeChanged ? null : next.scope }
    }
    publish(next)
  }
  const read = async (kind, fetcher, payload) => {
    invalidate()
    const version = revision.current
    const controller = new AbortController()
    request.current = controller
    publish({ ...current.current, preview: null, error: '', loadingScope: kind === 'scope', loadingPreview: kind === 'preview',
      ...(kind === 'scope' ? { scope: null } : {}) })
    try {
      const value = await fetcher(payload, { signal: controller.signal })
      if (!alive.current || version !== revision.current) return
      publish({ ...current.current, [kind]: value, loadingScope: false, loadingPreview: false })
    } catch (error) {
      if (!alive.current || version !== revision.current) return
      publish({ ...current.current, loadingScope: false, loadingPreview: false, error: error.message || 'Unable to read the game selection.' })
    }
  }
  const inspect = event => {
    event.preventDefault()
    const payload = scopePayload(current.current)
    const error = !payload.player_name ? 'Player name is required.'
      : payload.selection_mode === 'range' && (!payload.date_from || !payload.date_to) ? 'Choose both dates for the range.' : ''
    if (error) { setForm(form => ({ ...form, error })); return }
    return read('scope', inspectPlayerGameScope, payload)
  }
  const preview = () => {
    const form = current.current
    if (!form.scope) { setForm(value => ({ ...value, error: 'Inspect the game selection first.' })); return }
    const useAll = form.selectionMode === 'range' && form.useAll
    return read('preview', previewPlayerGameAnalysis, {
      ...scopePayload(form), game_limit: useAll ? null : Math.max(1, Number(form.gameLimit) || 1),
      use_all: useAll, range_order: form.rangeOrder,
    })
  }
  const takePlan = () => {
    const form = current.current
    if (!alive.current || !form.preview?.plan_id || form.loadingPreview || form.loadingScope) return null
    // Consume once at confirmation, before awaiting enqueue. New selections are
    // independent of that request and must not be cleared by its response.
    const plan = { preview: form.preview, batchSize: Number(form.batchSize), coolOff: Math.max(0, Number(form.coolOff) || 0) }
    setForm(value => ({ ...value, preview: null }))
    return plan
  }
  return { playerGameAnalysis: form, setPlayerGameAnalysis: setForm,
    handleInspectPlayerGameScope: inspect, handlePreviewPlayerGameAnalysis: preview, takePlan }
}
