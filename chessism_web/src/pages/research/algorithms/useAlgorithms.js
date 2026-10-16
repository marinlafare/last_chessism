import { useCallback, useEffect, useRef, useState } from 'react'
import * as api from './algorithmApi'
import { blankBuilder, draftSchemas, durationTemplate, openBuilder, relationshipsTemplate } from './builderForm'

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
  const [frozenInput, setFrozenInput] = useState(null)
  const [validation, setValidation] = useState(null)
  const [watchedRun, setWatchedRun] = useState(null)
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
        if (matrices.definitions.length) setForm(blankBuilder(matrices.definitions[0], columns))
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

  const matrix = frozenInput && frozenInput.id === form?.matrix_definition_id
    ? frozenInput : inputs.find((item) => item.id === form?.matrix_definition_id)
  const schemas = validation?.schemas || (form ? draftSchemas(form, matrix, catalog) : {})
  const availableInputs = frozenInput ? [frozenInput, ...inputs.filter((item) => item.id !== frozenInput.id)] : inputs
  const update = (changes) => {
    setForm((current) => ({ ...current, ...changes }))
    setEstimate(null); setValidation(null)
  }
  const selectInput = (id) => {
    setFrozenInput(null)
    setForm((current) => ({ ...blankBuilder(inputs.find((item) => item.id === id), catalog), parent_definition_id: current?.parent_definition_id || null }))
    setEstimate(null); setValidation(null)
  }
  const reset = () => {
    setFrozenInput(null); setForm(blankBuilder(inputs[0], catalog))
    setEstimate(null); setValidation(null); setNotice('Start with an empty algorithm or choose a template.')
  }
  const template = (kind) => {
    try {
      update(kind === 'relationships' ? relationshipsTemplate(form, schemas.input || {}) : durationTemplate(form))
      setNotice('Template loaded. Every step and output is editable; nothing has been saved or run.')
    } catch (failure) { setError(failure.message) }
  }
  const openDefinition = (definition) => {
    try {
      const opened = openBuilder(definition, catalog)
      setForm(opened.form); setFrozenInput(opened.matrix); setEstimate(null); setValidation(null)
      setNotice('Editing a new revision. Saving preserves the previous definition and all existing runs.')
      window.scrollTo({ top: 0, behavior: 'smooth' })
    } catch (failure) { setError(failure.message) }
  }
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
  const validate = () => act('validating', async () => {
    const payload = await api.validateAlgorithm(form)
    if (mounted.current) setValidation(payload)
  })
  const sample = () => act('queueing sample', async () => {
    const payload = await api.sampleAlgorithm(form)
    if (!mounted.current) return
    setWatchedRun(payload.id); setRunOffset(0); setRevision((current) => current + 1)
    setNotice('Sample queued (up to 500 source rows). Results and intermediate previews will open when it finishes.')
  })
  const run = (definition) => act('queueing', async () => {
    const payload = await api.runAlgorithm(definition.id)
    if (!mounted.current) return
    setRunOffset(0); setRevision((current) => current + 1)
    setWatchedRun(payload.id)
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
  const inspect = useCallback(async (id) => {
    resultRequest.current?.abort()
    const controller = new AbortController()
    resultRequest.current = controller
    setResult(null)
    try {
      const payload = await api.fetchRun(id, { signal: controller.signal })
      if (!controller.signal.aborted) setResult(payload)
    } catch (failure) { if (!controller.signal.aborted) setError(failure.message) }
  }, [])
  useEffect(() => {
    const watched = runs.find((item) => item.id === watchedRun)
    if (watched && ['complete', 'failed', 'cancelled'].includes(watched.status)) {
      setWatchedRun(null)
      setNotice(watched.status === 'complete' ? 'Calculation complete. Results are shown below.' : 'Run finished; see its details below.')
      inspect(watched.id)
    }
  }, [runs, watchedRun, inspect])
  const moreInputs = () => act('loading inputs', async () => {
    const payload = await api.fetchInputs(inputs.length)
    if (mounted.current) { setInputs((current) => [...current, ...payload.definitions]); setInputMore(payload.has_more) }
  })
  const moreDefinitions = () => act('loading definitions', async () => {
    const payload = await api.fetchDefinitions(definitions.length)
    if (mounted.current) { setDefinitions((current) => [...current, ...payload.definitions]); setDefinitionMore(payload.has_more) }
  })
  return { catalog, algorithmCatalog, inputs: availableInputs, inputMore, definitions, definitionMore, runs, runOffset, runMore, form, estimate, preview, result,
    matrix, schemas, validation, validate, sample, reset, template, openDefinition,
    error, notice, working, update, selectInput, save, preflight, run, cancel, remove, inspect, moreInputs, moreDefinitions,
    setPreview, setResult, setRunOffset, refresh: () => setRevision((current) => current + 1) }
}
