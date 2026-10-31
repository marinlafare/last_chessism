import { pageStyleTarget } from '../../../appearance/styleCore.js'

export default function CudaBenchmark({ benchmark, extractionSeconds }) {
  const ms = value => `${(value * 1000).toFixed(2)} ms`
  return <section aria-label="Batch size benchmark">
    <h3 {...pageStyleTarget('title')}>Batch sizes and input reuse · {benchmark.status}</h3>
    <p {...pageStyleTarget('text')}>One frozen input, one database extraction. Median of {benchmark.repeats} warm passes; all candidates verified against the same positions.</p>
    <table>
      <thead><tr><th>Rows per batch</th><th>Reused input · GPU</th><th>Reused input · CPU</th><th>Fresh input + GPU*</th></tr></thead>
      <tbody>{benchmark.variants.map(row => <tr key={row.batch_rows}>
        <td>{row.batch_rows.toLocaleString()}</td>
        <td>{ms(row.cuda_cupy_median_seconds)}</td>
        <td>{ms(row.numpy_cpu_median_seconds)}</td>
        <td>{ms(extractionSeconds + row.cuda_cupy_median_seconds)}</td>
      </tr>)}</tbody>
    </table>
    <p {...pageStyleTarget('text')}>Both paths include file reading, numeric preparation, hashing and aggregation; GPU includes transfers. *Sum of measured extraction and warm GPU time, excluding initialization and result publication.</p>
    <p {...pageStyleTarget('text')}>Fastest measured GPU batch: {benchmark.fastest_measured_gpu_batch_rows.toLocaleString()}. This does not change the default or replace live summaries. Input reuse is within this test only, not a persistent cache.</p>
  </section>
}
