import { useEffect, useState } from 'react'
import { formatNumber } from '../../utils/formatters'
import AppearanceChartDetails from './AppearanceChartDetails'
import CommonPositionPreview from './CommonPositionPreview'
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
      <AppearanceChartDetails>
        <p>Positions ordered by appearances, ten at a time, including scored and unscored FENs. Ties remain separate buttons.</p>
        <p>Labels show the most frequent recorded arrival: move number, side and SAN. Different move orders can reach the same FEN. Counts include all arrivals, not only the representative move.</p>
        <p>Hover, tap or focus for the actual board and a verified example sequence. Later groups are prepared and cached only when requested.</p>
        {data?.refreshed_at && <p>Snapshot: <time dateTime={data.refreshed_at}>{new Date(data.refreshed_at).toLocaleString()}</time>
          {data.stale ? ' · Changes pending; summary refreshes in the background.' : ''}</p>}
      </AppearanceChartDetails>
    </div>
    {(error || data?.refresh_error) && <div className="status-banner warn" role="alert">{error || data.refresh_error}</div>}
    <CommonPositionPreview selection={selected} />
    {failure ? <div className="commonness-message" role="alert">{data.detail}
      {data.status === 'error' && <button type="button" onClick={page.retry}>Retry</button>}</div>
      : pending ? <p className="commonness-message" role="status">{error || data?.refresh_error ? 'Summary unavailable.'
        : data ? 'Preparing positions in the background…' : 'Loading positions…'}</p>
      : !rows.length ? <p className="commonness-message">{page.index ? 'No more repeated positions.' : 'No repeated FENs with recorded appearances.'}</p>
        : <div className="commonness-buttons" aria-label="Most-common positions" onMouseLeave={() => setSelected(null)}>
          {rows.map(bar => {
            const label = `${bar.label}: ${formatNumber(bar.appearances)} times`
            return <div key={bar.fen} className="commonness-entry">
              <span {...styleTarget('position.rank')} className="commonness-rank" aria-hidden="true">#{bar.rank}</span>
              <button {...styleTarget('position.button')} type="button" aria-label={`Rank ${bar.rank}, ${label}`}
                aria-describedby={selected?.bar.fen === bar.fen ? 'common-position-preview' : undefined}
                data-common-rank={bar.rank} data-value={bar.appearances} data-fen={bar.fen}
                onMouseEnter={event => select(bar, event)} onMouseLeave={() => setSelected(null)}
                onFocus={event => select(bar, event)} onBlur={() => setSelected(null)} onClick={event => select(bar, event)}
                onKeyDown={event => {
                  if (event.key === 'Escape') setSelected(null)
                }}>
                <span {...styleTarget('position.label')} className="commonness-move-label">{bar.label}</span>
                <span {...styleTarget('position.count')} className="commonness-count">{formatNumber(bar.appearances)}</span>
              </button>
            </div>
          })}
        </div>}
    <nav className="appearance-pages commonness-pages" aria-label="Common position pages">
      <button type="button" disabled={page.index === 0} onClick={page.first}>First</button>
      <button type="button" disabled={page.index === 0} onClick={page.previous}>Previous</button>
      <span aria-live="polite">{page.index * 10 + 1}–{page.index * 10 + (rows.length || 10)}</span>
      <button type="button" disabled={pending || failure || !data?.next_cursor} onClick={page.next}>Next 10</button>
    </nav>
  </div>
}
