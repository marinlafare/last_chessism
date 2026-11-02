import { useState } from 'react'
import { fetchGraphStatus, retryGraphSync } from './systemStatusApi'
import { usePoll } from '../../hooks/usePoll'
import { pageStyleTarget } from '../../appearance/styleCore.js'

const number = value => Number(value || 0).toLocaleString()

export default function GraphSyncPanel() {
  const { data, error, refresh } = usePoll(fetchGraphStatus, 5000, { pauseWhenHidden: true })
  const [retrying, setRetrying] = useState(false)
  const [retryError, setRetryError] = useState('')
  const stale = data?.enabled && (!data.heartbeat || Date.now() - Date.parse(data.heartbeat) > 60000)
  async function retry() {
    setRetrying(true); setRetryError('')
    try { await retryGraphSync(); refresh() }
    catch (failure) { setRetryError(failure.message) }
    finally { setRetrying(false) }
  }
  return <section {...pageStyleTarget('graph.panel')} className="system-times" aria-label="Neo4j synchronization">
    <h2 {...pageStyleTarget('graph.title')}>Neo4j synchronization</h2>
    <p {...pageStyleTarget('graph.status')} role="status">{error ? 'Could not load graph status.' : !data ? 'Loading…' : stale ? 'Waiting for synchronization worker' : data.phase.replaceAll('_', ' ')}</p>
    {data?.installed && <>
      <p {...pageStyleTarget('graph.counts')}>{number(data.games)} games · {number(data.positions)} positions · {number(data.pending_items)} pending items</p>
      <p {...pageStyleTarget('graph.detail')}>{data.detail}</p>
      {data.backfills?.map(run => <div key={run.id} {...pageStyleTarget('graph.backfill')}>
        <span>{run.player_name}: {number(run.processed_games)} / {number(run.total_games)} games</span>
        <progress {...pageStyleTarget('graph.progress')} aria-label={`${run.player_name} graph import`} value={run.processed_games} max={run.total_games} style={{ width: '100%' }} />
      </div>)}
      <small {...pageStyleTarget('graph.scope')}>Counts cover imported games, not the entire PostgreSQL database.</small>
      {data.failed_events > 0 && <button {...pageStyleTarget('graph.retry')} onClick={retry} disabled={retrying}>{retrying ? 'Retrying…' : 'Retry synchronization'}</button>}
      {retryError && <p role="alert">{retryError}</p>}
    </>}
  </section>
}
