import { pageStyleTarget } from '../../../appearance/styleCore.js'

const stages = [
  ['source_queries_seconds', 'Database reads and driver decoding'],
  ['source_pack_seconds', 'Source grouping and packing'],
  ['host_prepare_wall_seconds', 'CPU preparation for CUDA'],
  ['upload_wall_seconds', 'Host → GPU', 'upload_cuda_seconds'],
  ['decisions_wall_seconds', 'Blunder decisions', 'decisions_cuda_seconds'],
  ['reductions_wall_seconds', 'Per-game reductions', 'reductions_cuda_seconds'],
  ['download_wall_seconds', 'GPU → host', 'download_cuda_seconds'],
  ['result_pack_wall_seconds', 'Result packing'],
  ['publication_prepare_sql_seconds', 'Database writes and histogram SQL'],
  ['publication_commit_seconds', 'Database commits'],
]
const seconds = value => value == null ? '—' : `${Number(value).toFixed(3)} s`
export default function CanonicalTimingReport({ report }) {
  return <>
    <p {...pageStyleTarget('text')}>{report.source.games.toLocaleString()} unique games · {report.source.rows.toLocaleString()} moves · {report.hardware.name}</p>
    <p {...pageStyleTarget('text')}>Exact agreement: {report.status}. Published summaries unchanged. Private tables {report.scratch_removed ? 'removed' : 'not yet removed'}.</p>
    {report.source.reader_comparison && <section aria-label="Source reader comparison">
      <h3 {...pageStyleTarget('title')}>Database input comparison</h3>
      <p {...pageStyleTarget('text')}>Same snapshot, identical input bytes; alternating reader order for each batch.</p>
      <p {...pageStyleTarget('text')}>Previous reader: {seconds(report.source.reader_comparison.legacy_seconds)} · Binary reader: {seconds(report.source.reader_comparison.binary_seconds)} · {Number(report.source.reader_comparison.speedup).toFixed(2)}× speedup</p>
      <p {...pageStyleTarget('text')}>Stage timings below use the tested binary reader. This comparison itself did not change the live reader.</p>
    </section>}
    <div className="algorithm-table-wrap"><table {...pageStyleTarget('table')}>
      <thead><tr>{['Stage', 'Wall time', 'CUDA-event interval'].map(label => <th {...pageStyleTarget('table.header')} key={label}>{label}</th>)}</tr></thead>
      <tbody>{stages.map(([key, label, gpu]) => <tr key={key}>
        <th {...pageStyleTarget('table.header')} scope="row">{label}</th>
        <td {...pageStyleTarget('table.cell')}>{seconds(report.timings[key])}</td>
        <td {...pageStyleTarget('table.cell')}>{seconds(gpu && report.timings[gpu])}</td>
      </tr>)}</tbody>
    </table></div>
    <p {...pageStyleTarget('text')}>Sequential stage sum: {seconds(report.timings.sequential_stage_sum_seconds)} · Full benchmark including validation: {seconds(report.timings.run_wall_seconds)}</p>
    <p {...pageStyleTarget('text')}>CUDA intervals are already included in wall times; they are not additional time.</p>
    <details><summary {...pageStyleTarget('disclosure')}>Scope, checks and overhead</summary>
      <p {...pageStyleTarget('text')}>{Object.entries(report.checks).map(([key, value]) => `${key.replaceAll('_', ' ')}: ${value}`).join(' · ')}</p>
      {report.notes.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
      <p {...pageStyleTarget('text')}>Reference inference: {seconds(report.timings.reference_inference_seconds)} · Warmup: {seconds(report.timings.warmup_seconds)}</p>
      <p {...pageStyleTarget('text')}>Input: {report.source.input_sha256}</p>
      <p {...pageStyleTarget('text')}>Model: {report.model_id}</p>
    </details>
  </>
}
