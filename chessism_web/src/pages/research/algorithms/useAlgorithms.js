import { useCallback, useEffect, useRef, useState } from 'react'
import * as api from './algorithmApi'
import { blankBuilder, draftSchemas, durationTemplate, inputColumns, openBuilder, relationshipsTemplate } from './builderForm'

export default function useAlgorithms() {
  const [catalog, setCatalog] = useState(null)
  const [algorithmCatalog, setAlgorithmCatalog] = useState(null)
  const [inputs, setInputs] = useState([])
  const [inputMore, setInputMore] = useState(false)
  const [inputOffset, setInputOffset] = useState(0)
  const [resolvedInput, setResolvedInput] = useState(null)
  const [inputRevision, setInputRevision] = useState(0)
  const [definitions, setDefinitions] = useState([])
  const [definitionMore, setDefinitionMore] = useState(false)
  const [runs, setRuns] = useState([])
  const [latestRuns, setLatestRuns] = useState([])
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
    Promise.all([api.fetchAlgorithmCatalog(options), api.fetchInputCatalog(options), api.fetchInputs(0, options), api.fetchDefinitions(0, options), api.fetchDefaultInputs(options)])
      .then(async ([algorithms, columns, matrices, saved, defaults]) => {
        if (controller.signal.aborted) return
        setAlgorithmCatalog(algorithms)
        setCatalog(columns)
        const available = [...defaults.definitions, ...matrices.definitions]
        setInputs(available)
        setInputMore(matrices.has_more)
        setInputOffset(matrices.definitions.length)
        setDefinitions(saved.definitions)
        setDefinitionMore(saved.has_more)
        const inputId = new URLSearchParams(window.location.search).get('input')
        const requestedInput = available.find((item) => item.id === inputId)
        if (requestedInput) setForm(blankBuilder(requestedInput, columns))
        // Old collection deep links still open only a preview, never select or run.
        const previewId = new URLSearchParams(window.location.search).get('preview')
        if (previewId) {
          const definition = available.find((item) => item.id === previewId) || await api.fetchInput(previewId, options)
          if (!controller.signal.aborted) setPreview(definition)
        }
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
        const options = { signal: controller.signal }
        const [latest, older] = await Promise.all([api.fetchRuns(0, options),
          runOffset ? api.fetchRuns(runOffset, options) : Promise.resolve(null)])
        const payload = older || latest
        if (!controller.signal.aborted) {
          setRuns(payload.runs); setRunMore(payload.has_more)
          setLatestRuns(latest.runs)
        }
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

  const sourceMatrix = frozenInput && frozenInput.id === form?.matrix_definition_id
    ? frozenInput : inputs.find((item) => item.id === form?.matrix_definition_id)
  const matrix = resolvedInput?.id === sourceMatrix?.id && resolvedInput ? resolvedInput : sourceMatrix
  const schemas = validation?.schemas || (form ? draftSchemas(form, matrix, catalog) : {})
  const availableInputs = frozenInput ? [frozenInput, ...inputs.filter((item) => item.id !== frozenInput.id)] : inputs
  const update = (changes) => {
    setForm((current) => ({ ...current, ...changes }))
    setEstimate(null); setValidation(null)
  }
  const selectInput = (id) => {
    if (busy.current || !catalog) return
    const selected = inputs.find((item) => item.id === id)
    if (!selected) return
    setFrozenInput(null)
    setResolvedInput(null); setInputRevision((current) => current + 1)
    setForm((current) => ({ ...blankBuilder(selected, catalog), parent_definition_id: current?.parent_definition_id || null }))
    setEstimate(null); setValidation(null)
  }
  const reset = () => {
    setFrozenInput(null); setResolvedInput(null); setForm(null)
    setInputRevision((current) => current + 1)
    setEstimate(null); setValidation(null); setNotice('Select an input formula to begin. Nothing has been run.')
  }
  const inputRequest = (parameters) => ({
    matrix_definition_id: sourceMatrix?.builtin_key ? null : sourceMatrix?.id,
    builtin_matrix_key: sourceMatrix?.builtin_key || null,
    matrix_parameters: parameters,
  })
  const requestForm = () => ({ ...form,
    matrix_definition_id: sourceMatrix?.builtin_key ? null : form.matrix_definition_id,
    builtin_matrix_key: sourceMatrix?.builtin_key || null,
    matrix_parameters: resolvedInput?.runtime_parameters || null,
  })
  const dirtyInput = () => { setResolvedInput(null); setEstimate(null); setValidation(null) }
  const applyInput = (parameters) => act('configuring input', async () => {
    const reply = await api.configureInput(inputRequest(parameters))
    if (!mounted.current) return
    const bound = { ...sourceMatrix, config: reply.config, runtime_parameters: parameters }
    const available = new Set(inputColumns(bound, catalog).map((column) => column.key))
    setResolvedInput(bound)
    setForm((current) => {
      const columns = current.columns.filter((key) => available.has(key))
      const changed = columns.length !== current.columns.length
      return { ...current, columns: columns.length ? columns : [...available].slice(0, 12),
        max_rows: Math.min(current.max_rows, reply.config.filters.max_rows || 100000),
        ...(changed ? { steps: [], outputs: [] } : {}) }
    })
    setEstimate(null); setValidation(null)
    setNotice('Input selection applied to this algorithm only. No rows were fetched or saved. If selected columns disappeared, dependent draft steps were cleared.')
  })
  const previewInput = () => {
    if (!matrix) return
    setPreview({ ...matrix, id: matrix.runtime_parameters ? matrix.id : null, storage_kind: 'definition',
      feature_count: matrix.config.feature_columns.length, label_count: matrix.config.label_columns.length })
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
      setResolvedInput(null); setInputRevision((current) => current + 1)
      setForm(opened.form); setFrozenInput(opened.matrix); setEstimate(null); setValidation(null)
      setNotice('Editing a new revision. Saving preserves the previous definition and all existing runs.')
      window.scrollTo({ top: 0, behavior: 'smooth' })
    } catch (failure) { setError(failure.message) }
  }
  const save = () => act('saving', async () => {
    const saved = await api.saveAlgorithm(requestForm())
    if (!mounted.current) return
    setDefinitions((current) => [saved, ...current])
    setNotice('Instructions saved. Press Run on the saved algorithm to start a calculation.')
  })
  const preflight = () => act('checking', async () => {
    const payload = await api.preflightAlgorithm(requestForm())
    if (mounted.current) setEstimate(payload)
  })
  const validate = () => act('validating', async () => {
    const payload = await api.validateAlgorithm(requestForm())
    if (mounted.current) setValidation(payload)
  })
  const sample = () => act('queueing sample', async () => {
    const payload = await api.sampleAlgorithm(requestForm())
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
  const runCuda = (player, options) => act('queueing CUDA validation', async () => {
    const payload = await api.runCudaValidation(player, options)
    if (!mounted.current) return
    setRunOffset(0); setRevision((current) => current + 1); setWatchedRun(payload.id)
    setNotice('One-player GPU validation queued. The report will open when it finishes; live summaries stay unchanged.')
  })
  const benchmarkCanonical = () => act('queueing pipeline timing', async () => {
    const payload = await api.benchmarkCanonical()
    if (!mounted.current) return
    setRunOffset(0); setRevision(current => current + 1); setWatchedRun(payload.id)
    setNotice('100,000-game timing run queued. Published summaries remain unchanged; follow it in Active runs.')
  })
  const cancel = (item) => act('cancelling', async () => {
    const reply = await api.cancelAlgorithm(item.id)
    if (mounted.current) { setNotice(reply.message); setRevision((current) => current + 1) }
  })
  const trainBlunders = games => act('queueing rating-aware model', async () => {
    const payload = await api.trainBlunderModel(games)
    if (!mounted.current) return
    setRunOffset(0); setRevision(current => current + 1); setWatchedRun(payload.id)
    setNotice('Rating-aware CUDA candidate queued. Its report will open when ready; live summaries stay unchanged.')
  })
  const trainBlundersV2 = (games, sampling, refinement) => act('queueing rating-aware v2', async () => {
    const payload = await api.trainBlunderV2(games, sampling, refinement)
    if (!mounted.current) return
    setRunOffset(0); setRevision(current => current + 1); setWatchedRun(payload.id)
    setNotice('Smooth rating-aware v2 queued. Validation gates will be reported; nothing is automatically promoted.')
  })
  const validateBlunders = player => act('queueing fixed-coefficient validation', async () => {
    const payload = await api.validateFixedBlunders(player)
    if (!mounted.current) return
    setRunOffset(0); setRevision(current => current + 1); setWatchedRun(payload.id)
    setNotice('Fixed-coefficient GPU validation queued. Live blunder counts and coefficients stay unchanged.')
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
    const watched = latestRuns.find((item) => item.id === watchedRun)
    if (watched && ['complete', 'failed', 'cancelled'].includes(watched.status)) {
      setWatchedRun(null)
      setNotice(watched.status === 'complete' ? 'Calculation complete. Results are shown below.' : 'Run finished; see its details below.')
      inspect(watched.id)
    }
  }, [latestRuns, watchedRun, inspect])
  const moreInputs = () => act('loading inputs', async () => {
    const payload = await api.fetchInputs(inputOffset)
    if (mounted.current) {
      setInputs((current) => [...new Map([...current, ...payload.definitions].map((item) => [item.id, item])).values()])
      setInputOffset((current) => current + payload.definitions.length); setInputMore(payload.has_more)
    }
  })
  const removeInput = (definition) => {
    if (definition.builtin_key || !window.confirm(`Delete the formula “${definition.name}”? Existing algorithms and backups will be kept.`)) return
    act('deleting formula', async () => {
      await api.deleteInput(definition.id)
      if (!mounted.current) return
      setInputs((current) => current.filter((item) => item.id !== definition.id))
      setInputOffset((current) => Math.max(0, current - 1))
      if (!frozenInput && form?.matrix_definition_id === definition.id) reset()
    })
  }
  const moreDefinitions = () => act('loading definitions', async () => {
    const payload = await api.fetchDefinitions(definitions.length)
    if (mounted.current) { setDefinitions((current) => [...current, ...payload.definitions]); setDefinitionMore(payload.has_more) }
  })
  return { catalog, algorithmCatalog, inputs: availableInputs, inputMore, definitions, definitionMore, runs,
    activeRuns: latestRuns.filter(run => ['queued', 'running'].includes(run.status)), runOffset, runMore, form, estimate, preview, result,
    matrix, sourceMatrix, formulaInputs: inputs, inputRevision, schemas, validation, validate, sample, reset, template, openDefinition,
    applyInput, dirtyInput, previewInput, removeInput, runCuda, trainBlunders, trainBlundersV2, validateBlunders, benchmarkCanonical,
    error, notice, working, update, selectInput, save, preflight, run, cancel, remove, inspect, moreInputs, moreDefinitions,
    setPreview, setResult, setRunOffset, refresh: () => setRevision((current) => current + 1) }
}
