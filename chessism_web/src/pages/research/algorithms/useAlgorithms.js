import { useCallback, useEffect, useRef, useState } from 'react'
import * as api from './algorithmApi'
import { newAlgorithmForm, updateAlgorithmForm } from './algorithmForm'

export default function useAlgorithms() {
  const [catalog, setCatalog] = useState(null)
  const [algorithmCatalog, setAlgorithmCatalog] = useState(null)
  const [inputs, setInputs] = useState([])
  const [inputMore, setInputMore] = useState(false)
  const [definitions, setDefinitions] = useState([])
  const [definitionMore, setDefinitionMore] = useState(false)
  const [runs, setRuns] = useState([])
  const [runOffset, setRunOffset] = useState(0)
  const [runMore, setRunMore] = useState(false)
  const [form, setForm] = useState(null)
  const [estimate, setEstimate] = useState(null)
  const [preview, setPreview] = useState(null)
  const [result, setResult] = useState(null)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [working, setWorking] = useState('loading')
  const mounted = useRef(false)
  const busy = useRef(false)
  const resultRequest = useRef(null)

  useEffect(() => {
    mounted.current = true
    const controller = new AbortController()
    const options = { signal: controller.signal }
    Promise.all([api.fetchAlgorithmCatalog(options), api.fetchInputCatalog(options), api.fetchInputs(0, options), api.fetchDefinitions(0, options)])
      .then(([algorithms, columns, matrices, saved]) => {
        if (controller.signal.aborted) return
        setAlgorithmCatalog(algorithms)
        setCatalog(columns)
        setInputs(matrices.definitions)
        setInputMore(matrices.has_more)
        setDefinitions(saved.definitions)
        setDefinitionMore(saved.has_more)
        if (matrices.definitions.length) setForm(newAlgorithmForm(matrices.definitions[0], columns))
      }).catch((failure) => { if (!controller.signal.aborted) setError(failure.message) })
      .finally(() => { if (!controller.signal.aborted) setWorking('') })
    return () => { mounted.current = false; controller.abort(); resultRequest.current?.abort() }
  }, [])

  const [revision, setRevision] = useState(0)
  useEffect(() => {
    const controller = new AbortController()
    let timer
    const load = async () => {
      try {
        const payload = await api.fetchRuns(runOffset, { signal: controller.signal })
        if (!controller.signal.aborted) { setRuns(payload.runs); setRunMore(payload.has_more) }
      } catch (failure) { if (!controller.signal.aborted) setError(failure.message) }
      if (!controller.signal.aborted) timer = setTimeout(load, 5000)
    }
    load()
    return () => { controller.abort(); clearTimeout(timer) }
  }, [runOffset, revision])

  const act = useCallback(async (label, task) => {
    if (busy.current) return
    busy.current = true
    setWorking(label); setError(''); setNotice('')
    try { await task() } catch (failure) { if (mounted.current) setError(failure.message) }
    finally { busy.current = false; if (mounted.current) setWorking('') }
  }, [])

  const update = (changes) => { setForm((current) => updateAlgorithmForm(current, changes)); setEstimate(null) }
  const selectInput = (id) => { setForm(newAlgorithmForm(inputs.find((item) => item.id === id), catalog)); setEstimate(null) }
  const save = () => act('saving', async () => {
    const saved = await api.saveAlgorithm(form)
    if (!mounted.current) return
    setDefinitions((current) => [saved, ...current])
    setNotice('Instructions saved. Press Run on the saved algorithm to start a calculation.')
  })
  const preflight = () => act('checking', async () => {
    const payload = await api.preflightAlgorithm(form)
    if (mounted.current) setEstimate(payload)
  })
  const run = (definition) => act('queueing', async () => {
    await api.runAlgorithm(definition.id)
    if (!mounted.current) return
    setRunOffset(0); setRevision((current) => current + 1)
    setNotice('Run queued. You can close this page; the dedicated worker continues in the background.')
  })
  const cancel = (item) => act('cancelling', async () => {
    const reply = await api.cancelAlgorithm(item.id)
    if (mounted.current) { setNotice(reply.message); setRevision((current) => current + 1) }
  })
  const remove = (definition) => {
    if (!window.confirm(`Delete “${definition.name}” instructions? Existing runs and results will remain.`)) return
    act('deleting', async () => {
      await api.deleteAlgorithm(definition.id)
      if (mounted.current) setDefinitions((current) => current.filter((item) => item.id !== definition.id))
    })
  }
  const inspect = async (id) => {
    resultRequest.current?.abort()
    const controller = new AbortController()
    resultRequest.current = controller
    setResult(null)
    try {
      const payload = await api.fetchRun(id, { signal: controller.signal })
      if (!controller.signal.aborted) setResult(payload)
    } catch (failure) { if (!controller.signal.aborted) setError(failure.message) }
  }
  const moreInputs = () => act('loading inputs', async () => {
    const payload = await api.fetchInputs(inputs.length)
    if (mounted.current) { setInputs((current) => [...current, ...payload.definitions]); setInputMore(payload.has_more) }
  })
  const moreDefinitions = () => act('loading definitions', async () => {
    const payload = await api.fetchDefinitions(definitions.length)
    if (mounted.current) { setDefinitions((current) => [...current, ...payload.definitions]); setDefinitionMore(payload.has_more) }
  })
  return { catalog, algorithmCatalog, inputs, inputMore, definitions, definitionMore, runs, runOffset, runMore, form, estimate, preview, result,
    error, notice, working, update, selectInput, save, preflight, run, cancel, remove, inspect, moreInputs, moreDefinitions,
    setPreview, setResult, setRunOffset, refresh: () => setRevision((current) => current + 1) }
}
