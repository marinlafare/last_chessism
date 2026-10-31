import { useState } from 'react'
import { pageStyleTarget } from '../../../appearance/styleCore.js'
import { parseCudaReport } from './cudaReport'
import CudaReport from './CudaReport'

export default function CudaValidation({ onRun, disabled = false }) {
  const [player, setPlayer] = useState('')
  const [benchmark, setBenchmark] = useState(false)
  const [report, setReport] = useState(null)
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)
  const open = async event => {
    const file = event.target.files?.[0]
    event.target.value = ''
    if (!file) return
    setLoading(true); setError(''); setReport(null)
    try {
      if (file.size > 2 * 1024 * 1024) throw Error('Report exceeds 2 MiB.')
      setReport(parseCudaReport(await file.text()))
    } catch (failure) { setError(failure.message) }
    finally { setLoading(false) }
  }
  return <section {...pageStyleTarget('cuda.validation.panel')} className="algorithm-panel" aria-label="CUDA validation">
    <h2 {...pageStyleTarget('title')}>CUDA validation</h2>
    <p {...pageStyleTarget('text')}>Score-distribution experiment. CPU summaries remain official. Rating-aware blunders have a separate candidate-fitting section above.</p>
    {onRun && <form className="algorithm-actions" onSubmit={event => { event.preventDefault(); if (player.trim() && !disabled) onRun(player.trim(), { benchmark }) }}>
      <label {...pageStyleTarget('label')}>Player
        <input {...pageStyleTarget('field')} value={player} onChange={event => setPlayer(event.target.value)} maxLength={128} required disabled={disabled} placeholder="Player username" />
      </label>
      <label {...pageStyleTarget('label')}>
        <input {...pageStyleTarget('field')} type="checkbox" checked={benchmark} onChange={event => setBenchmark(event.target.checked)} disabled={disabled} /> Compare batch sizes and input reuse
      </label>
      <button {...pageStyleTarget('button')} type="submit" disabled={disabled || !player.trim()}>Run on GPU</button>
    </form>}
    {onRun && <p {...pageStyleTarget('text')}>One player, all three modes, at most 2,000,000 scored occurrences, streamed in batches of 8,192; a current CPU summary is required. Larger players are rejected, not truncated. Runs and reports appear below. This does not replace live summaries.</p>}
    <label {...pageStyleTarget('label')}>Open comparison report
      <input {...pageStyleTarget('field')} type="file" accept="application/json,.json" disabled={loading} onChange={open} />
    </label>
    <p {...pageStyleTarget('text')}>Opening a report reads a local file only. It does not run an algorithm, upload data, or activate a summary implementation. Imported reports are not approval certificates.</p>
    {error && <p role="alert">{error}</p>}
    {report && <CudaReport report={report} />}
  </section>
}
