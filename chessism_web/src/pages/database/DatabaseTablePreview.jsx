import { useEffect, useId, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { pageStyleTarget } from '../../appearance/styleCore'
import { fetchDatabasePreview } from './databaseApi'
import './databasePreview.css'

export default function DatabaseTablePreview({ table, onClose }) {
  const dialog = useRef(null), titleId = useId()
  const [revision, setRevision] = useState(0)
  const [request, setRequest] = useState({ loading: true, data: null, error: '' })

  useEffect(() => {
    const node = dialog.current, previousFocus = document.activeElement
    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    node.showModal()
    return () => {
      node.close()
      document.body.style.overflow = previousOverflow
      if (previousFocus?.isConnected) previousFocus.focus({ preventScroll: true })
    }
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    setRequest({ loading: true, data: null, error: '' })
    fetchDatabasePreview(table.id, { signal: controller.signal }).then(data => {
      if (!controller.signal.aborted) setRequest({ loading: false, data, error: '' })
    }).catch(error => {
      if (!controller.signal.aborted) setRequest({ loading: false, data: null, error: error.message || 'Preview unavailable.' })
    })
    return () => controller.abort()
  }, [table.id, revision])

  const { data, loading, error } = request
  const closeOutside = event => {
    if (event.target !== event.currentTarget) return
    const rect = event.currentTarget.getBoundingClientRect()
    if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) onClose()
  }

  return createPortal(<dialog {...pageStyleTarget('popup')} className="database-preview" ref={dialog}
    aria-labelledby={titleId} onClick={closeOutside} onCancel={event => { event.preventDefault(); onClose() }}>
    <header className="database-preview-header">
      <div><h2 {...pageStyleTarget('heading')} id={titleId}>{table.id}</h2><span>Read-only · DataFrame preview</span></div>
      <div className="database-preview-actions">
        <button {...pageStyleTarget('button')} type="button" disabled={loading} onClick={() => setRevision(value => value + 1)}>New sample</button>
        <button {...pageStyleTarget('button')} type="button" aria-label="Close table preview" autoFocus onClick={onClose}>×</button>
      </div>
    </header>
    {loading && <p className="database-preview-message" role="status">Sampling up to 50 rows…</p>}
    {error && <p className="database-preview-message database-error" role="alert">{error}</p>}
    {data && <>
      <div key={revision} className="database-preview-scroll" tabIndex={0} role="region" aria-label="Table rows; scroll to see more">
        <table className="dataframe">
          <caption className="database-preview-sr-only">Sampled rows from {table.id}</caption>
          <thead><tr><th scope="col" aria-label="Row index" />{data.columns.map(column => <th scope="col" key={column.name}
            title={`${column.data_type}${column.redacted ? ' · protected values hidden' : ''}`}>{column.name}</th>)}</tr></thead>
          <tbody>{data.rows.map((row, index) => <tr key={index}>
            <th scope="row">{index}</th>{row.map((value, col) => <td key={col} className={value == null ? 'database-preview-null' : ''}>
              <span title={value == null ? 'NULL' : String(value)}>{value == null ? 'NULL' : String(value)}</span>
            </td>)}
          </tr>)}</tbody>
        </table>
        {!data.row_count && <p className="database-preview-message">This table has no visible rows.</p>}
      </div>
      <footer {...pageStyleTarget('text')} className="database-preview-footer">
        <span>{data.row_count} rows × {data.columns.length} columns · Scroll for more rows and columns.</span>
        <span>{data.sample_method === 'random_blocks' ? 'Random block sample.' : `Shuffled sample from up to ${data.candidate_limit.toLocaleString()} candidate rows.`}
          {data.columns.some(column => column.redacted) && ' Credential fields are hidden.'}
          {data.truncated_cells > 0 && ` Long values shortened to ${data.cell_limit} characters.`}
          {data.omitted_columns > 0 && ` ${data.omitted_columns} additional columns omitted.`}</span>
      </footer>
    </>}
  </dialog>, document.body)
}
