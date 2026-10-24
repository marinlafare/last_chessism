import { useEffect, useState } from 'react'
import CommonPositionPreview from './CommonPositionPreview'
import CommonnessBars from './CommonnessBars'
import useCommonPositionPages from './useCommonPositionPages'
import { styleTarget } from '../../appearance/styleCore'

export default function CommonnessChart({ data: firstPage, error, active }) {
  const page = useCommonPositionPages(firstPage, active)
  const data = page.data
  const [selected, setSelected] = useState(null)
  useEffect(() => { setSelected(null) }, [data?.refreshed_at, active, page.index])
  const rows = data?.rows || []
  const pending = !data || data.status === 'pending' || (!data.refreshed_at && !data.status)
  const failure = data?.status === 'error' || data?.status === 'changed'
  const select = (bar, event) => setSelected({ bar, rect: event.currentTarget.getBoundingClientRect() })
  return <div className="commonness-panel">
    <div className="pending-repetition-heading">
      <h2 className="panel-title">Communess</h2>
    </div>
    {(error || data?.refresh_error) && <div className="status-banner warn" role="alert">{error || data.refresh_error}</div>}
    <CommonPositionPreview selection={selected} />
    <div className="commonness-navigation" role="group" aria-label="Browse common positions">
    <button {...styleTarget('button')} className="commonness-page-arrow" type="button" aria-label="Previous ten positions"
      disabled={page.index === 0} onClick={page.previous}>‹</button>
    {failure ? <div className="commonness-message" role="alert">{data.detail}
      {data.status === 'error' && <button type="button" onClick={page.retry}>Retry</button>}</div>
      : pending ? <p className="commonness-message" role="status">{error || data?.refresh_error ? 'Summary unavailable.'
        : data ? 'Preparing positions in the background…' : 'Loading positions…'}</p>
      : !rows.length ? <p className="commonness-message">{page.index ? 'No more repeated positions.' : 'No repeated FENs with recorded appearances.'}</p>
        : <CommonnessBars rows={rows} selected={selected} onSelect={select} onClear={() => setSelected(null)} />}
    <button {...styleTarget('button')} className="commonness-page-arrow" type="button" aria-label="Next ten positions"
      disabled={pending || failure || !data?.next_cursor} onClick={page.next}>›</button>
    </div>
    <nav {...styleTarget('commonness.paging')} className="appearance-pages commonness-pages" aria-label="Common position pages">
      {page.index > 0 && <button {...styleTarget('button')} type="button" onClick={page.first}>First 10</button>}
      <span aria-live="polite">{rows.length ? `${page.index * 10 + 1}–${page.index * 10 + rows.length}` : pending ? 'Loading…' : 'No positions'}</span>
    </nav>
  </div>
}
