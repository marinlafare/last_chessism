import { useEffect, useRef, useState } from 'react'
import { deleteMatrixDefinition, estimateMatrix, fetchMatrixCatalog, saveMatrixDefinition } from './matrixApi'
import { createMatrixForm, defaultFeatures, formFromDefinition, matrixFormPayload, toggleFormColumn, toggleFormMode } from './matrixForm'
import useMatrixDefinitions from './useMatrixDefinitions'

export default function useMatrixConstructor() {
  const [catalog, setCatalog] = useState(null)
  const [form, setForm] = useState(createMatrixForm)
  const [estimate, setEstimate] = useState(null)
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [working, setWorking] = useState('')
  const [viewedArtifact, setViewedArtifact] = useState(null)
  const estimateRequest = useRef(null)
  const mutation = useRef(false)
  const mounted = useRef(false)
  const saved = useMatrixDefinitions()
  const rowTypes = catalog?.row_types || []
  const rowType = rowTypes.find((item) => item.key === form.rowTypeKey)

  useEffect(() => {
    mounted.current = true
    const controller = new AbortController()
    fetchMatrixCatalog({ signal: controller.signal }).then((payload) => {
      if (controller.signal.aborted) return
      setCatalog(payload)
      setForm((current) => ({ ...current, features: defaultFeatures(payload.row_types?.find((item) => item.key === current.rowTypeKey)) }))
    }).catch((failure) => {
      if (!controller.signal.aborted) setError(failure.message || 'Unable to load the matrix catalog.')
    })
    return () => {
      mounted.current = false
      controller.abort()
      estimateRequest.current?.abort()
    }
  }, [])

  const changeForm = (update) => {
    estimateRequest.current?.abort()
    estimateRequest.current = null
    setWorking((current) => current === 'estimate' ? '' : current)
    setEstimate(null)
    setNotice('')
    setError('')
    setForm(update)
  }

  const estimateCurrentScope = async () => {
    if (mutation.current) return
    estimateRequest.current?.abort()
    const controller = new AbortController()
    estimateRequest.current = controller
    setWorking('estimate')
    setError('')
    try {
      const next = await estimateMatrix(matrixFormPayload(form), { signal: controller.signal })
      if (!controller.signal.aborted) setEstimate(next)
    } catch (failure) {
      if (!controller.signal.aborted) setError(failure.message || 'Unable to estimate this matrix.')
    } finally {
      if (mounted.current && estimateRequest.current === controller) {
        estimateRequest.current = null
        setWorking('')
      }
    }
  }

  const save = async () => {
    if (mutation.current || estimateRequest.current) return
    mutation.current = true
    setWorking('save')
    setError('')
    try {
      const definition = await saveMatrixDefinition(matrixFormPayload(form))
      if (!mounted.current) return
      saved.insert(definition)
      setNotice(`Saved “${definition.name}”: instructions only. No matrix files or background job were created.`)
    } catch (failure) {
      if (mounted.current) setError(failure.message || 'Unable to save this definition.')
    } finally {
      mutation.current = false
      if (mounted.current) setWorking('')
    }
  }

  const remove = async (definition) => {
    if (mutation.current || estimateRequest.current) return
    if (!window.confirm(`Delete the instructions for “${definition.name}”? Existing snapshots and backups will be kept.`)) return
    mutation.current = true
    setWorking('delete')
    setError('')
    try {
      await deleteMatrixDefinition(definition.id)
      if (mounted.current) saved.forget(definition.id)
    } catch (failure) {
      if (mounted.current) setError(failure.message || 'Unable to delete this definition.')
    } finally {
      mutation.current = false
      if (mounted.current) setWorking('')
    }
  }

  const useDefinition = (definition) => {
    changeForm(formFromDefinition(definition))
    setNotice('Definition loaded into the form. Save creates a separate definition; the original is unchanged.')
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }

  return {
    catalog, form, rowType, rowTypes, estimate, notice, error: error || saved.error,
    working, saved, viewedArtifact, setViewedArtifact, save, remove, useDefinition, estimateCurrentScope,
    changeName: (name) => changeForm((current) => ({ ...current, name })),
    changeRowType: (key) => {
      const next = rowTypes.find((item) => item.key === key)
      changeForm((current) => ({ ...current, rowTypeKey: key, features: defaultFeatures(next), labels: [], name: `${next?.label || 'Research'} matrix` }))
    },
    toggleMode: (mode) => changeForm((current) => toggleFormMode(current, mode)),
    toggleColumn: (key, role) => changeForm((current) => toggleFormColumn(current, key, role)),
    updateFilter: (key, value) => changeForm((current) => ({ ...current, filters: { ...current.filters, [key]: value } })),
    showDraftPreview: () => setViewedArtifact({ id: null, storage_kind: 'definition', name: form.name,
      config: matrixFormPayload(form), feature_count: form.features.length, label_count: form.labels.length }),
  }
}
