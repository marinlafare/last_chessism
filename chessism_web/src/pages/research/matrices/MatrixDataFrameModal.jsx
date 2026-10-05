import { useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { fetchLiveMatrixPreview, fetchMatrixPreview } from './matrixApi'
import './matrixDataFrame.css'

const displayValue = (value) => {
  if (value === null) return 'N/A'
  if (typeof value === 'number' && !Number.isInteger(value)) return Number(value.toPrecision(7)).toString()
  return String(value)
}

export default function MatrixDataFrameModal({ artifact, onClose }) {
  const isLive = artifact.storage_kind === 'definition'
  const dialogRef = useRef(null)
  const [offset, setOffset] = useState(0)
  const [limit, setLimit] = useState(50)
  const [role, setRole] = useState('all')
  const [sliceIndex, setSliceIndex] = useState(0)
  const [fullscreen, setFullscreen] = useState(false)
  const [retry, setRetry] = useState(0)
  const [request, setRequest] = useState({ data: null, loading: true, error: '' })

  useEffect(() => {
    const dialog = dialogRef.current
    const previousFocus = document.activeElement
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    dialog.showModal()
    return () => {
      dialog.close()
      document.body.style.overflow = previousOverflow
      if (previousFocus?.isConnected) previousFocus.focus()
    }
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    setRequest((current) => ({ ...current, loading: true, error: '' }))
    const options = { offset, limit, role, sliceIndex, signal: controller.signal }
    const pending = isLive ? fetchLiveMatrixPreview(artifact, options) : fetchMatrixPreview(artifact.id, options)
    pending
      .then((data) => {
        if (!controller.signal.aborted) setRequest({ data, loading: false, error: '' })
      })
      .catch((error) => {
        if (!controller.signal.aborted) {
          setRequest({ data: null, loading: false, error: error.message || 'Unable to read this matrix.' })
        }
      })
    return () => controller.abort()
  }, [artifact.id, artifact.config, isLive, offset, limit, role, sliceIndex, retry])

  const data = request.data
  const total = data?.total_rows ?? artifact.row_count ?? 0
  const pages = Math.max(1, Math.ceil(total / limit))
  const currentPage = Math.floor(offset / limit) + 1
  const lastOffset = (pages - 1) * limit
  const hasCategories = data?.columns?.some((column) => column.encoding === 'dictionary')
  const changeRole = (value) => { setRole(value); setOffset(0); setSliceIndex(0) }
  const closeFromBackdrop = (event) => {
    if (event.target !== event.currentTarget) return
    const rect = event.currentTarget.getBoundingClientRect()
    if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) onClose()
  }

  return createPortal(
    <dialog
      ref={dialogRef}
      className={`matrix-dataframe ${fullscreen ? 'is-fullscreen' : ''}`}
      aria-labelledby="matrix-dataframe-title"
      onCancel={(event) => { event.preventDefault(); onClose() }}
      onClick={closeFromBackdrop}
    >
      <header className="matrix-dataframe-header">
        <div><p className="eyebrow">{isLive ? 'LIVE PREVIEW—NOT A SAVED SNAPSHOT' : 'LEGACY SNAPSHOT · DATAFRAME VIEW'}</p><h2 id="matrix-dataframe-title">{artifact.name}</h2></div>
        <div className="matrix-dataframe-actions">
          {isLive ? <button type="button" disabled={request.loading} onClick={() => setRetry((value) => value + 1)}>refresh sample</button> : null}
          <button type="button" onClick={() => setFullscreen((current) => !current)}>{fullscreen ? 'restore size' : 'full screen'}</button>
          <button type="button" onClick={onClose} aria-label="Close matrix viewer" autoFocus>×</button>
        </div>
      </header>
      <div className="matrix-dataframe-controls">
        <label>Columns<select value={role} onChange={(event) => changeRole(event.target.value)}>
          <option value="all">All</option><option value="features">Features</option>
          <option value="labels" disabled={!artifact.label_count}>Labels</option>
        </select></label>
        <label>{isLive ? 'Preview rows' : 'Rows per page'}<select value={limit} onChange={(event) => { setLimit(Number(event.target.value)); setOffset(0) }}>
          {[25, 50, 100].map((size) => <option key={size} value={size}>{size}</option>)}
        </select></label>
        {data?.dimensions === 3 ? (
          <label>Slice (0–{data.slice_count - 1})<input type="number" min="0" max={data.slice_count - 1} value={sliceIndex} onChange={(event) => {
            const next = Number(event.target.value)
            if (Number.isInteger(next) && next >= 0 && next < data.slice_count) { setSliceIndex(next); setOffset(0) }
          }} /></label>
        ) : null}
        <span className="matrix-dataframe-shape">{data ? `${data.dimensions}D ${isLive ? 'sample' : ''} · ${data.shape.join(' × ')}` : isLive ? 'Live sample' : `${total.toLocaleString()} rows`} · read only</span>
      </div>
      <p className="matrix-dataframe-note">
        {isLive ? 'Live source values; they may change as the database changes. No matrix files are saved. Categories show source labels, not permanent codes.' : 'Saved snapshot values, not recalculated from the live games.'} N/A marks missing values; hover a value for full precision.
        {hasCategories ? ' Categorical columns show their stored dictionary codes.' : ''}
        {data?.dimensions === 3 ? ' Axes: slice × row × column.' : ''}
      </p>
      <div className="matrix-dataframe-scroll" aria-busy={request.loading} tabIndex="0" aria-label="Matrix rows, scroll to see more columns">
        {request.loading ? <p role="status">Loading matrix rows…</p> : null}
        {request.error ? <div role="alert"><p>{request.error}</p><button type="button" onClick={() => setRetry((current) => current + 1)}>retry</button></div> : null}
        {!request.loading && !request.error && data ? (
          <table>
            <caption className="matrix-dataframe-caption">{artifact.name} · {isLive ? 'live sample' : 'saved matrix data'}</caption>
            <thead><tr><th scope="col">index</th>{data.columns.map((column) => (
              <th scope="col" key={`${column.role}-${column.key}`}>
                {column.key}<small>{column.role === 'features' ? 'feature' : 'label'} · {column.encoding === 'source_category' ? 'category (encoded on run)' : `${isLive ? 'on run: ' : ''}${column.storage_dtype}`}{column.encoding === 'dictionary' ? ' · code' : ''}</small>
              </th>
            ))}</tr></thead>
            <tbody>{data.rows.map((row, index) => (
              <tr key={data.offset + index}>
                <th scope="row">{data.offset + index}</th>
                {row.map((value, column) => <td className={value === null ? 'is-missing' : ''} key={column} title={value === null ? 'Missing value' : String(value)}>{displayValue(value)}</td>)}
              </tr>
            ))}</tbody>
          </table>
        ) : null}
        {!request.loading && data && !data.rows.length ? <p>No matching rows in the current scope.</p> : null}
      </div>
      <footer className="matrix-dataframe-footer">
        <span aria-live="polite">{!request.loading && data ? isLive ? `${data.rows.length} preview rows${data.more_matching_rows ? ' · more rows available' : ''} · definition limit: ${data.row_limit.toLocaleString()}` : `Rows ${data.rows.length ? data.offset + 1 : 0}–${data.offset + data.rows.length} of ${total.toLocaleString()}` : request.loading ? 'Loading…' : 'Preview unavailable'}</span>
        {!isLive ? <nav aria-label="Matrix pagination">
          <button type="button" disabled={request.loading || !offset} onClick={() => setOffset(0)} aria-label="First page">«</button>
          <button type="button" disabled={request.loading || !offset} onClick={() => setOffset(Math.max(0, offset - limit))} aria-label="Previous page">‹</button>
          <span>Page {currentPage} / {pages.toLocaleString()}</span>
          <button type="button" disabled={request.loading || !data?.has_more} onClick={() => setOffset(offset + limit)} aria-label="Next page">›</button>
          <button type="button" disabled={request.loading || !data?.has_more} onClick={() => setOffset(lastOffset)} aria-label="Last page">»</button>
        </nav> : null}
      </footer>
    </dialog>,
    document.body
  )
}
