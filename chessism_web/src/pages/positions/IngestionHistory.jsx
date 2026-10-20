import { useEffect, useId, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { formatNumber } from '../../utils/formatters.js'
import { fetchIngestionHistoryDay, fetchIngestionHistoryDays, fetchIngestionHistoryRun } from './ingestionHistoryApi'

function duration(milliseconds) {
  if (milliseconds == null) return '—'
  const seconds = Math.max(0, Number(milliseconds) / 1000)
  if (seconds < 60) return `${seconds.toFixed(1)}s`
  const whole = Math.floor(seconds)
  const hours = Math.floor(whole / 3600)
  const minutes = Math.floor((whole % 3600) / 60)
  return `${hours ? `${hours}h ` : ''}${minutes}m ${whole % 60}s`
}

function IssueIcon({ messages }) {
  const description = messages.filter(Boolean).join('\n')
  if (!description) return null
  return (
    <span className="fen-history-issue" tabIndex={0} title={description}
      role="img" aria-label={description}>
      <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="2" aria-hidden="true">
        <path d="m5 5 10 10M15 5 5 15" />
      </svg>
    </span>
  )
}

function RunCounts({ counts = {} }) {
  const importedFallback = counts.games_downloaded == null && counts.games_imported != null
  const metrics = [
    {
      label: importedFallback ? 'Games imported' : 'Games downloaded',
      value: importedFallback ? counts.games_imported : counts.games_downloaded,
      description: importedFallback
        ? 'This older run recorded imported games, not the total fetched from archives. Includes archived nonstandard games.'
        : 'Games received from the downloaded archives, including games already stored or later excluded.',
    },
    {
      label: 'Positions from games',
      value: counts.position_occurrences,
      description: 'Position occurrences extracted in this run. Repeated positions are counted each time they appear.',
    },
    {
      label: 'Endgame positions marked',
      value: counts.tablebase_positions_marked,
      description: 'Positions given exact tablebase results in this run. Excludes unavailable results and finalization steps.',
    },
  ]
  return (
    <dl className="fen-history-counts" aria-label="Ingestion counts">
      {metrics.map(metric => (
        <div className="fen-history-line" key={metric.label}
          title={metric.value == null ? 'This count was not recorded for the run.' : metric.description}>
          <dt {...pageStyleTarget('text')}>{metric.label}</dt>
          <dd {...pageStyleTarget('value')} className="fen-history-time">
            {metric.value == null ? 'Not recorded' : formatNumber(metric.value, 0)}
          </dd>
        </div>
      ))}
    </dl>
  )
}

function HistoryRun({ run, index }) {
  const id = useId()
  const [open, setOpen] = useState(false)
  const [detail, setDetail] = useState(null)
  const [error, setError] = useState('')
  const [attempt, setAttempt] = useState(0)
  useEffect(() => {
    if (!open || detail) return
    const controller = new AbortController()
    setError('')
    fetchIngestionHistoryRun(run.run_id, { signal: controller.signal })
      .then(payload => { if (!controller.signal.aborted) setDetail(payload.run) })
      .catch(reason => { if (!controller.signal.aborted) setError(reason.message) })
    return () => controller.abort()
  }, [open, run.run_id, detail, attempt])
  const runIssue = run.last_error || (
    ['failed', 'error', 'warning', 'cancelled', 'canceled', 'partial'].includes(run.status)
      ? `Ingestion ${run.status}.` : ''
  )
  const startedAt = new Date(run.started_at)
  const timeLabel = startedAt.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })
  const kind = run.trigger === 'player_create' ? 'creation'
    : run.trigger === 'player_update' ? 'update' : 'automatic'
  const kindLabel = kind === 'creation' ? 'Player creation · first download'
    : kind === 'update' ? 'Player update' : 'Automatic ingestion'
  return (
    <div {...pageStyleTarget('card')} className="fen-history-run"
      style={{ '--fen-history-delay': `${Math.min(index, 9) * 45}ms`, '--fen-history-direction': index % 2 ? -1 : 1 }}>
      <button {...pageStyleTarget('button')} type="button" className="fen-history-toggle fen-history-player-toggle"
        aria-expanded={open} aria-controls={id} onClick={() => setOpen(value => !value)}>
        <span className={`fen-history-kind is-${kind}`} role="img" aria-label={kindLabel} title={kindLabel} />
        <span className="fen-history-player-name">{run.player_name || 'Automatic ingestion'}</span>
        <time dateTime={run.started_at} className="fen-history-date">
          <span>{timeLabel}</span>
        </time>
      </button>
      {open && (
        <div id={id} className="fen-history-summary">
          {!detail && !error && <p role="status">Loading timings…</p>}
          {error && <div className="status-banner warn" role="alert">
            {error}{' '}
            <button {...pageStyleTarget('button')} type="button" onClick={() => setAttempt(value => value + 1)}>Retry</button>
          </div>}
          {detail && (
            <>
              <RunCounts counts={detail.counts} />
              {detail.stages.map(stage => (
                <div className="fen-history-line" key={stage.stage}>
                  <span {...pageStyleTarget('text')}>{stage.label}</span>
                  <IssueIcon messages={stage.issues || []} />
                  <span {...pageStyleTarget('value')} className="fen-history-time"
                    title={stage.timing_basis === 'longest_worker_active_time'
                      ? 'Active processing time of the longest-running worker; parallel and overlapping steps do not add up to the total.'
                      : 'Elapsed time for this phase.'}>{duration(stage.elapsed_ms)}</span>
                </div>
              ))}
              <div className="fen-history-line fen-history-total">
                <span {...pageStyleTarget('text')}>Total</span>
                <IssueIcon messages={[runIssue]} />
                <span {...pageStyleTarget('value')} className="fen-history-time"
                  title="End-to-end elapsed time, including queue waits.">{duration(detail.elapsed_ms)}</span>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}

function HistoryDays({ data, page, selectedDay, onDay, onPage, loading }) {
  if (!data?.days.length) return null
  const paginated = page > 0 || data.has_more
  return (
    <nav className="fen-history-days" aria-label="Ingestion history dates">
      {paginated && (
        <button {...pageStyleTarget('button')} type="button" className="fen-history-days-arrow"
          aria-label="Newer ingestion dates" title="20 newer active days"
          disabled={loading || page === 0} onClick={() => onPage(page - 1)}>
          <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden="true">
            <path d="m12 5-5 5 5 5" />
          </svg>
        </button>
      )}
      <div className="fen-history-days-scroll">
        <div className="fen-history-days-track" style={{ '--fen-history-day-count': data.days.length }}>
          {data.days.map(item => {
            const date = new Date(`${item.day}T12:00:00`)
            const label = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
            return (
              <button {...pageStyleTarget('button')} key={item.day} type="button" className="fen-history-day"
                aria-pressed={selectedDay === item.day} aria-label={`${item.day}: ${item.run_count} ingestions`}
                title={`${item.day} · ${item.run_count} ingestion${item.run_count === 1 ? '' : 's'}`}
                disabled={loading} onClick={() => onDay(item.day)}>
                <time dateTime={item.day}><span>{label}</span><small>{date.getFullYear()}</small></time>
              </button>
            )
          })}
        </div>
      </div>
      {paginated && (
        <button {...pageStyleTarget('button')} type="button" className="fen-history-days-arrow"
          aria-label="Older ingestion dates" title="20 older active days"
          disabled={loading || !data.has_more} onClick={() => onPage(page + 1)}>
          <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.8" aria-hidden="true">
            <path d="m8 5 5 5-5 5" />
          </svg>
        </button>
      )}
    </nav>
  )
}

export default function IngestionHistory({ revision }) {
  const id = useId()
  const [open, setOpen] = useState(false)
  const [dayPage, setDayPage] = useState(0)
  const [days, setDays] = useState(null)
  const [selectedDay, setSelectedDay] = useState(null)
  const [timezone] = useState(() => Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC')
  const [data, setData] = useState(null)
  const [error, setError] = useState('')
  const [daysError, setDaysError] = useState('')
  const [daysLoading, setDaysLoading] = useState(false)
  const [loading, setLoading] = useState(false)
  const [attempt, setAttempt] = useState(0)
  useEffect(() => {
    if (!open) return
    const controller = new AbortController()
    setDaysLoading(true)
    setDaysError('')
    fetchIngestionHistoryDays(dayPage, timezone, { signal: controller.signal })
      .then(payload => {
        if (controller.signal.aborted) return
        setDays(payload)
        setSelectedDay(current => payload.days.some(item => item.day === current)
          ? current : payload.days[0]?.day || null)
      })
      .catch(reason => { if (!controller.signal.aborted) setDaysError(reason.message) })
      .finally(() => { if (!controller.signal.aborted) setDaysLoading(false) })
    return () => controller.abort()
  }, [open, dayPage, timezone, revision, attempt])
  useEffect(() => {
    if (!open || !selectedDay) return
    const controller = new AbortController()
    setLoading(true)
    setError('')
    fetchIngestionHistoryDay(selectedDay, timezone, { signal: controller.signal })
      .then(payload => { if (!controller.signal.aborted) setData(payload) })
      .catch(reason => { if (!controller.signal.aborted) setError(reason.message) })
      .finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [open, selectedDay, timezone, revision, attempt])
  return (
    <div className="fen-ingestion-history">
      <button {...pageStyleTarget('button')} type="button" className="fen-history-toggle fen-history-main-toggle"
        aria-expanded={open} aria-controls={id} onClick={() => setOpen(value => !value)}>
        <span>History</span>
      </button>
      {open && (
        <div id={id} className="fen-history-list" aria-busy={loading || daysLoading}>
          {!data && (loading || daysLoading) && <p role="status">Loading history…</p>}
          {(error || daysError) && <div className="status-banner warn" role="alert">
            {daysError || error}{' '}
            <button {...pageStyleTarget('button')} type="button" onClick={() => setAttempt(value => value + 1)}>Retry</button>
          </div>}
          {!daysLoading && !daysError && days?.days.length === 0 && <p>No completed ingestions recorded.</p>}
          {/* Keep the displayed day mounted until its replacement is ready. */}
          {data && (
            <>
              {data.runs.length === 0 && <p>No completed ingestions recorded.</p>}
              <div className="fen-history-entries" key={data.day}>
                {data.runs.map((run, index) => <HistoryRun key={run.run_id} run={run} index={index} />)}
              </div>
            </>
          )}
          <HistoryDays data={days} page={dayPage} selectedDay={data?.day || selectedDay}
            onDay={setSelectedDay} onPage={setDayPage} loading={daysLoading} />
        </div>
      )}
    </div>
  )
}
