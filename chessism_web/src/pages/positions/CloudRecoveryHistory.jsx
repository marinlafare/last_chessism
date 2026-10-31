import { pageStyleTarget } from '../../appearance/styleCore.js'

const label = { retry: 'Automatic retry', recovered: 'Recovered automatically', exhausted: 'Retry limit reached',
  paused: 'Paused', resumed: 'Resumed manually' }
const duration = value => {
  const seconds = Math.round(value)
  return seconds >= 60 ? `${Math.floor(seconds / 60)}m ${seconds % 60}s` : `${seconds}s`
}

export default function CloudRecoveryHistory({ recovery, stylePrefix }) {
  const target = (id, fallback) => pageStyleTarget(stylePrefix ? `${stylePrefix}.${id}` : fallback)
  if (!recovery) return null
  return <details {...target('panel', 'card')} className="cloud-recovery-history">
    <summary {...target('toggle', 'disclosure')}>Recovery history · {recovery.total} events</summary>
    <p {...target('summary', 'text')}>Recorded manual pauses: {recovery.counts.paused || 0}
      {' · '}Time waiting for manual recovery: {duration(recovery.paused_seconds || 0)}</p>
    <div className="cloud-recovery-history-scroll" tabIndex={0} role="region" aria-label="Recovery events">
      <table>
        <thead><tr>{['Time', 'Event', 'Details'].map(text => <th {...target('column', 'text')} key={text}>{text}</th>)}</tr></thead>
        <tbody>{recovery.events.map(event => <tr key={event.id}>
          <td {...target('time', 'text')}><time dateTime={event.created_at}>{new Date(event.created_at).toLocaleString()}</time></td>
          <td {...target('event', 'text')}>{label[event.kind] || event.kind}</td>
          <td {...target('details', 'text')}>{event.operation}{event.error_type ? ` · ${event.error_type}` : ''}
            {event.attempt != null ? ` · attempt ${event.attempt}` : ''}
            {event.delay_seconds != null ? ` · retry in ${duration(event.delay_seconds)}` : ''}
            {event.elapsed_seconds != null ? ` · elapsed ${duration(event.elapsed_seconds)}` : ''}
            {event.message ? ` — ${event.message}` : ''}</td>
        </tr>)}</tbody>
      </table>
    </div>
    {recovery.total > recovery.events.length && <p {...target('more', 'text')}>Showing the latest {recovery.events.length} events. Older events remain saved.</p>}
    <small {...target('note', 'text')}>{recovery.note}</small>
  </details>
}
