import { useEffect, useState } from 'react'
import { deleteMatrixArtifact, fetchMatrixArtifacts } from './matrixApi'
import MatrixPreviewButton from './MatrixPreviewButton'
import { formatNumber } from '../../../utils/formatters'

export default function LegacyMatrixSnapshots({ count, onPreview, onChanged }) {
  const [open, setOpen] = useState(false)
  const [snapshots, setSnapshots] = useState([])
  const [error, setError] = useState('')
  const [refresh, setRefresh] = useState(0)
  const [loading, setLoading] = useState(false)
  useEffect(() => {
    if (!open) return undefined
    const controller = new AbortController()
    setLoading(true)
    fetchMatrixArtifacts({ signal: controller.signal }).then((payload) => {
      if (!controller.signal.aborted) { setSnapshots(payload.artifacts || []); setError(''); setLoading(false) }
    }).catch((failure) => { if (!controller.signal.aborted) { setError(failure.message); setLoading(false) } })
    return () => controller.abort()
  }, [open, refresh])
  const remove = async (snapshot) => {
    if (!window.confirm(`Delete the legacy working files for “${snapshot.name}”? Its definition and existing backup copies will be kept.`)) return
    try { await deleteMatrixArtifact(snapshot.id); setRefresh((value) => value + 1); onChanged() }
    catch (failure) { setError(failure.message) }
  }
  if (!count && !snapshots.length) return null
  return (
    <section className="matrix-panel">
      <div className="section-head"><div><p className="eyebrow">LEGACY</p><h2>Preserved snapshots ({count})</h2></div><button className="btn btn-secondary btn-inline" type="button" onClick={() => setOpen((value) => !value)}>{open ? 'hide' : 'show snapshots'}</button></div>
      <p className="matrix-empty">These old files are unchanged and included separately in manual database backups. New definitions do not create snapshot files.</p>
      {open ? <>
        <button className="matrix-inspect-button" type="button" onClick={() => setRefresh((value) => value + 1)}>refresh snapshots</button>
        {error ? <p className="matrix-error" role="alert">{error}</p> : null}
        {loading ? <p className="matrix-empty" role="status">Loading legacy snapshots…</p> : null}
        <div className="matrix-artifact-list">{snapshots.map((snapshot) => <article className="matrix-artifact" key={snapshot.id}>
          <div className="matrix-artifact-head"><strong>{snapshot.name}</strong><div className="matrix-artifact-tools">{snapshot.status === 'complete' ? <MatrixPreviewButton name={snapshot.name} onClick={() => onPreview(snapshot)} /> : null}<span className={`matrix-state ${snapshot.status}`}>{snapshot.status}</span></div></div>
          <div className="matrix-artifact-result"><span>{formatNumber(snapshot.row_count)} rows · {snapshot.feature_count} features · {snapshot.label_count} labels</span><code>{snapshot.artifact_path}</code></div>
          {snapshot.progress?.detail || snapshot.error ? <p className="matrix-empty">{snapshot.progress?.detail || snapshot.error}</p> : null}
          {!['queued', 'running'].includes(snapshot.status) ? <button className="matrix-delete" type="button" onClick={() => remove(snapshot)}>delete legacy working snapshot</button> : null}
        </article>)}</div>
      </> : null}
    </section>
  )
}
