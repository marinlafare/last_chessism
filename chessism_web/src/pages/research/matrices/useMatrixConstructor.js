import { useEffect, useRef, useState } from 'react'
import { fetchMatrixCatalog, fetchMatrixDefinition, saveMatrixDefinition } from './matrixApi'
import { createMatrixForm, defaultFeatures, formFromDefinition, matrixFormPayload, toggleFormColumn } from './matrixForm'

export default function useMatrixConstructor() {
  const [catalog, setCatalog] = useState(null)
  const [form, setForm] = useState(createMatrixForm)
  const [notice, setNotice] = useState('')
  const [error, setError] = useState('')
  const [working, setWorking] = useState('')
  const [savedDefinition, setSavedDefinition] = useState(null)
  const mutation = useRef(false)
  const mounted = useRef(false)
  const rowTypes = (catalog?.row_types || []).filter((item) => item.key !== 'move_scores')
  const rowType = rowTypes.find((item) => item.key === form.rowTypeKey)

  useEffect(() => {
    mounted.current = true
    const controller = new AbortController()
    const definitionId = new URLSearchParams(window.location.search).get('definition')
    Promise.all([
      fetchMatrixCatalog({ signal: controller.signal }),
      definitionId ? fetchMatrixDefinition(definitionId, { signal: controller.signal }) : Promise.resolve(null),
    ]).then(([payload, definition]) => {
      if (controller.signal.aborted) return
      setCatalog(payload)
      const isDefaultScores = definition?.row_type === 'move_scores'
      setForm((current) => {
        if (definition && !isDefaultScores) return formFromDefinition(definition)
        const custom = current.rowTypeKey === 'move_scores' ? createMatrixForm() : current
        return { ...custom, features: defaultFeatures(payload.row_types?.find((item) => item.key === custom.rowTypeKey)) }
      })
      if (isDefaultScores) setNotice('Game move scores is in Algorithm creation → Input formulas → Default. This constructor is for custom formulas; your saved copy is unchanged.')
      else if (definition) setNotice('Columns loaded as a new reusable formula. Its player and scope will be chosen later; the original formula is unchanged.')
    }).catch((failure) => {
      if (!controller.signal.aborted) setError(failure.message || 'Unable to load the matrix catalog.')
    })
    return () => { mounted.current = false; controller.abort() }
  }, [])

  const changeForm = (update) => {
    setNotice('')
    setSavedDefinition(null)
    setError('')
    setForm(update)
  }

  const save = async () => {
    if (mutation.current) return
    if (!rowType) { setError('Choose a custom row type. Default formulas are in Algorithm creation.'); return }
    mutation.current = true
    setWorking('save')
    setError('')
    try {
      const definition = await saveMatrixDefinition(matrixFormPayload(form))
      if (!mounted.current) return
      setSavedDefinition(definition)
      setNotice('Saved “' + definition.name + '”. Find it in Algorithm creation → Input formulas. No dataset was created.')
    } catch (failure) {
      if (mounted.current) setError(failure.message || 'Unable to save this formula.')
    } finally {
      mutation.current = false
      if (mounted.current) setWorking('')
    }
  }

  return {
    catalog, form, rowType, rowTypes, notice, error, working, savedDefinition, save,
    changeName: (name) => changeForm((current) => ({ ...current, name })),
    changeRowType: (key) => {
      const next = rowTypes.find((item) => item.key === key)
      if (!next) return
      changeForm((current) => ({ ...current, rowTypeKey: key,
        features: defaultFeatures(next),
        labels: [], name: (next?.label || 'Research') + ' matrix' }))
    },
    toggleColumn: (key, role) => changeForm((current) => toggleFormColumn(current, key, role)),
  }
}
