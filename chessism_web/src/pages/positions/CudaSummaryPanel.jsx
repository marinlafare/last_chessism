import { useRef, useState } from 'react'
import { usePoll } from '../../hooks/usePoll'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import ProgressMeter from '../../components/progress/ProgressMeter'
import { fetchCudaSummaries, runCudaSummaries, retryCudaSummaries } from './positionsApi'
import './cuda-summaries.css'

const names = { blunders: 'Blunders', accuracy: 'Accuracy', score_distribution: 'Score distribution',
  behavioral: 'Behavioral analysis', unexpected_endings: 'Unexpected endings', salience: 'Salience · first 15 moves', position_frequency: 'Repeated positions' }
const number = value => Number(value || 0).toLocaleString()
const players = value => number(value) + (Number(value) === 1 ? ' player' : ' players')
function Fill(props) { return <span {...pageStyleTarget('cuda.progress.fill')} {...props} /> }

export default function CudaSummaryPanel() {
  const { data, error, loading, refresh } = usePoll(fetchCudaSummaries, 5000, { requestTimeoutMs: 10000 })
  const [failure, setFailure] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [history, setHistory] = useState(false)
  const busy = useRef(false)
  const submit = async (id) => {
    if (busy.current) return
    busy.current = true; setSubmitting(true); setFailure('')
    try { await (id ? retryCudaSummaries(id) : runCudaSummaries()); refresh() }
    catch (e) { setFailure(e.message || 'Unable to request CUDA summaries.') }
    finally { busy.current = false; setSubmitting(false) }
  }
  if (error?.status === 403) return null
  const run = data?.active || data?.latest
  const pending = data?.pending_games || {}
  const metadata = run?.timings?.metadata
  const salience = run?.timings?.salience
  const frequency = run?.timings?.position_frequency
  const frequencyActive = frequency && !frequency.complete && /position frequencies|repeated positions/i.test(run?.phase || '')
  const salienceActive = run?.metrics?.includes('salience') && !salience?.complete &&
    (salience || !Number(run?.total_games))
  const metadataOnly = run?.metrics?.includes('behavioral') && !Number(run?.total_games)
  const available = (data?.available_metrics || []).reduce((sum, metric) => sum + Number(pending[metric] || 0), 0)
    + Number(data?.pending_score_players || 0) + Number(data?.pending_accuracy_players || 0)
    + Number(data?.pending_behavioral_players || 0) + Number(data?.pending_unexpected_endings_players || 0)
    + Number(data?.pending_salience_players || 0) + Number(data?.pending_position_frequency_players || 0)
  return <section {...pageStyleTarget('cuda.panel')} className="cuda-summary-panel" aria-label="CUDA summaries">
    <div className="cuda-summary-heading">
      <button {...pageStyleTarget('cuda.run')} type="button"
        disabled={loading || !data?.manual || submitting || Boolean(data?.active) || !available || data?.preparing_games > 0}
        onClick={() => submit()}>Run CUDA summaries</button>
      <span {...pageStyleTarget('cuda.pending')} role="status">
        {loading ? 'Loading pending work…' : data?.manual
          ? players(data.pending_players) + ' with pending summaries'
          : 'Manual scheduling is not enabled yet.'}
      </span>
    </div>
    {data?.manual && <>
      <div className="cuda-summary-metrics">{Object.entries(names).filter(([metric]) =>
        !['behavioral','unexpected_endings','salience','position_frequency'].includes(metric) || data.available_metrics.includes(metric)).map(([metric, name]) =>
        <span {...pageStyleTarget('cuda.metric.' + metric)} key={metric}>
          {name}: {['behavioral','unexpected_endings','salience','position_frequency'].includes(metric) ? players(data['pending_' + metric + '_players']) + ' pending' : metric === 'accuracy' && data.pending_accuracy_players > 0
            ? players(data.pending_accuracy_players) + ' pending' : number(pending[metric]) + ' games'}
          {data.deferred_metrics.includes(metric) ? ' · GPU migration pending' : ' · ready for manual runs'}
        </span>)}</div>
      <p {...pageStyleTarget('cuda.note')}>CUDA calculations start only when requested. {data.available_metrics.includes('behavioral')
        ? 'Scores, blunders and accuracy share position inputs; behavioral analysis and Unexpected endings use an incremental metadata stage.' : data.available_metrics.includes('accuracy')
        ? 'Blunders, score distribution, and accuracy share one input read. Daily accuracy bands are saved with the results.' : data.available_metrics.includes('score_distribution')
        ? 'Blunders and score distribution share one input read. Accuracy migration is next.'
        : 'This first stage runs CUDA blunders; accuracy and score distribution will be connected next.'} {data.available_metrics.includes('salience') && 'Salience uses the first 15 full moves with equal weights and refreshes the hero daily cache.'} Existing results are retained until publication.</p>
      {data.pending_score_players > 0 && <p {...pageStyleTarget('cuda.pending')}>{number(data.pending_score_players)} players have changed score-distribution partitions.</p>}
      {data.preparing_games > 0 && <p {...pageStyleTarget('cuda.preparing')} role="status">{number(data.preparing_games)} games still being prepared.</p>}
      {run && <div {...pageStyleTarget('cuda.progress.panel')} className="cuda-summary-progress">
        <strong {...pageStyleTarget('cuda.progress.phase')} role="status">{run.phase}</strong>
        <ProgressMeter {...pageStyleTarget('cuda.progress.track')} className="cuda-summary-track"
          fillTag={Fill} aria-label="CUDA summary progress"
          aria-valuetext={metadataOnly || salienceActive || frequencyActive ? run.phase : undefined}
          value={frequencyActive ? (frequency.prepared ? frequency.shards + frequency.published : undefined) : salienceActive ? (salience?.prepared && salience.shards < salience.shard_total ? salience.shards : undefined) : metadataOnly ? (!metadata?.ready && run.status !== 'complete' ? undefined : metadata?.complete ? 1 : Number(metadata?.computed || 0)) : Number(run.processed_games) + Number(run.superseded_games)}
          max={frequencyActive ? frequency.shard_total + frequency.players.length : salienceActive ? salience?.shard_total || 32 : metadataOnly ? (metadata?.complete ? 1 : Math.max(1, metadata?.chunks?.length || 0)) : Number(run.total_games) || 1} />
        {!metadataOnly && !salienceActive && !frequencyActive && Number(run.total_games) > 0 && <span {...pageStyleTarget('cuda.progress.count')}>{number(run.processed_games)} / {number(run.total_games)} games processed
          {run.superseded_games > 0 ? ' · ' + number(run.superseded_games) + ' changed games left pending for a new run' : ''}
        </span>}
        {run.error && <p {...pageStyleTarget('cuda.error')} role="alert">{run.error}</p>}
        {frequency && <span {...pageStyleTarget('cuda.progress.position_frequency')}>
          Repeated positions: {number(frequency.rows)} occurrences prepared · {number(frequency.batches)} GPU batches · {number(frequency.published)} / {number(frequency.players.length)} players published
        </span>}
        {salience && <span {...pageStyleTarget('cuda.progress.salience')}>
          Salience: {number(salience.games)} games · {number(salience.rows)} early positions prepared · {number(salience.batches)} GPU batches
        </span>}
        {run.timings?.metadata && <span {...pageStyleTarget('cuda.progress.count')}>
          Metadata: {number(run.timings.metadata.rows)} player-games prepared · {number(run.timings.metadata.computed)} / {number(run.timings.metadata.chunks?.length)} input chunks calculated
        </span>}
        {run.status === 'failed' && <button {...pageStyleTarget('cuda.retry')} type="button"
          disabled={submitting || Boolean(data.active)} onClick={() => submit(run.id)}>Retry run</button>}
      </div>}
      <button {...pageStyleTarget('cuda.history.toggle')} type="button" aria-expanded={history}
        onClick={() => setHistory(open => !open)}>History</button>
      {history && <div {...pageStyleTarget('cuda.history.panel')} className="cuda-summary-history">
        {!data.history.length && <p {...pageStyleTarget('cuda.history.entry')}>No previous runs.</p>}
        {data.history.map(item => <article {...pageStyleTarget('cuda.history.entry')} key={item.id}>
          <span>{new Date(item.created_at).toLocaleString()} · {item.status} · {number(item.processed_games)} games processed</span>
          <span>Input: {Number(item.timings?.source_seconds || 0).toFixed(1)} s · CUDA: {Number(item.timings?.cuda_seconds || 0).toFixed(1)} s · Save: {Number(item.timings?.publication_seconds || 0).toFixed(1)} s
            {item.metrics?.includes('score_distribution') ? ' · Finalize: ' + Number(item.timings?.finalization_seconds || 0).toFixed(1) + ' s' : ''}</span>
          {item.timings?.metadata && <span>Metadata input: {Number(item.timings.metadata.preparation_seconds || 0).toFixed(1)} s · Calculate/stage: {Number(item.timings.metadata.cuda_seconds || 0).toFixed(1)} s · GPU batches: {number(item.timings.metadata.batches)}</span>}
          {item.timings?.salience && <span>Salience input: {Number(item.timings.salience.input_seconds || 0).toFixed(1)} s · CUDA: {Number(item.timings.salience.cuda_seconds || 0).toFixed(1)} s · GPU batches: {number(item.timings.salience.batches)}</span>}
          {item.timings?.position_frequency && <span>Frequency input: {Number(item.timings.position_frequency.input_seconds || 0).toFixed(1)} s · CUDA: {Number(item.timings.position_frequency.cuda_seconds || 0).toFixed(1)} s · Stored rows: {number(item.timings.position_frequency.stored_rows)}</span>}
        </article>)}
      </div>}
    </>}
    {(failure || error) && <p {...pageStyleTarget('cuda.error')} role="alert">{failure || error.message}</p>}
  </section>
}
