import { useEffect, useState } from 'react'
import { pageStyleTarget } from '../../../appearance/styleCore.js'
import { fetchDefaultMatrices } from './matrixApi'
import MatrixDefinitionList from './MatrixDefinitionList'
import MatrixDataFrameModal from './MatrixDataFrameModal'

export default function DefaultMatrices() {
  const [definitions, setDefinitions] = useState([])
  const [error, setError] = useState('')
  const [preview, setPreview] = useState(null)
  useEffect(() => {
    const controller = new AbortController()
    fetchDefaultMatrices({ signal: controller.signal }).then((data) => {
      if (!controller.signal.aborted) setDefinitions(data.definitions || [])
    }).catch((failure) => { if (!controller.signal.aborted) setError(failure.message) })
    return () => controller.abort()
  }, [])
  return <>
    <section {...pageStyleTarget('defaults.panel')} className="matrix-panel" aria-label="Default matrices">
      <h2 {...pageStyleTarget('title')}>Default matrices</h2>
      <p {...pageStyleTarget('text')}>Reusable input definitions, not calculated datasets. Select one to choose its data in Algorithm creation.</p>
      {error ? <p role="alert">{error}</p> : definitions.length ? <MatrixDefinitionList definitions={definitions}
        onPreview={setPreview} onSelect={(definition) => {
          window.location.assign(`/research/algorithms?input=${encodeURIComponent(definition.id)}`)
        }} /> : <p role="status">Loading default matrices…</p>}
    </section>
    {preview && <MatrixDataFrameModal artifact={preview} onClose={() => setPreview(null)} />}
  </>
}
