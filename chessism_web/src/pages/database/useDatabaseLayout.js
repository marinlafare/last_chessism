import { useCallback, useEffect, useState } from 'react'
import { normalizeLayout } from './workspaceGeometry'
import { readSavedLayout, writeSavedLayout } from './layoutPersistence'

export function useDatabaseLayout(tables, accountId) {
  // Always open on the system hierarchy, including browsers with an old layout.
  const [layout, updateLayout] = useState(() => normalizeLayout(null, tables))
  const [initial] = useState(() => {
    try { return { saved: readSavedLayout(localStorage, accountId), error: '' } }
    catch { return { saved: null, error: 'Saved layout could not be read. The default workspace is still available.' } }
  })
  const [savedLayout, setSavedLayout] = useState(initial.saved)
  const [storageError, setStorageError] = useState(initial.error)
  const [notice, setNotice] = useState('')
  const setLayout = useCallback(value => { setNotice(''); updateLayout(value) }, [])
  useEffect(() => updateLayout(value => normalizeLayout(value, tables)), [tables])

  const saveLayout = () => {
    try {
      const saved = writeSavedLayout(localStorage, accountId, normalizeLayout(layout, tables))
      setSavedLayout(saved); setStorageError(''); setNotice('Personal layout saved in this browser.')
    } catch {
      setStorageError('Layout could not be saved in this browser. Your current arrangement is still available on this page.')
    }
  }
  const loadLayout = () => {
    try {
      // Read again so explicitly loading also sees a layout saved in another tab.
      const saved = readSavedLayout(localStorage, accountId)
      if (!saved) { setSavedLayout(null); setNotice('No saved layout is available.'); return false }
      updateLayout({ ...normalizeLayout(saved, tables), autoFrame: false })
      setSavedLayout(saved); setStorageError(''); setNotice('Personal layout loaded. Save layout to keep further changes.')
      return true
    } catch {
      setStorageError('Saved layout could not be loaded. Your current arrangement has not changed.')
      return false
    }
  }
  // No auto-save or unmount writes: dragging, panning and expanding are temporary.
  return { layout, setLayout, storageError, notice, hasSavedLayout: Boolean(savedLayout), saveLayout, loadLayout }
}
