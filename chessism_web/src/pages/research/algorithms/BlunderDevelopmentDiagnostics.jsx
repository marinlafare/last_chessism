import { useState } from 'react'
import { pageStyleTarget } from '../../../appearance/styleCore.js'

const models = [['coefficient_only', 'Current coefficient rule'], ['frozen_reference', 'Frozen reference'], ['candidate', 'New candidate']]
const n = value => Number.isFinite(value) ? value.toLocaleString(undefined, { maximumFractionDigits: 4 }) : '—'

export default function BlunderDevelopmentDiagnostics({ report }) {
  const [kind, setKind] = useState('overall')
  return <section aria-label="CP-balanced development diagnostics">
    <h3 {...pageStyleTarget('title')}>Development diagnostics — not a final test</h3>
    <p {...pageStyleTarget('text')}>Same inputs for all three models. No automatic promotion or changes to live summaries.</p>
    {report.rapid?.groups ? <>
      <label {...pageStyleTarget('label')}>Rapid breakdown <select {...pageStyleTarget('field')} value={kind} onChange={e => setKind(e.target.value)}>
        {[['overall', 'Overall'], ['average_rating', 'Average-rating band'], ['rating_gap', 'Rating gap'], ['year', 'Year'], ['player_participation', 'Player participation']].map(([key, label]) => <option key={key} value={key}>{label}</option>)}
      </select></label>
      <div className="algorithm-table-wrap"><table><caption>Rapid training / validation — MSE and calibration error (pp)</caption>
        <thead><tr><th>Partition</th><th>Group</th><th>Games</th>{models.map(([key, label]) => <th key={key}>{label}: MSE / ECE</th>)}</tr></thead>
        <tbody>{report.rapid.groups.filter(g => g.kind === kind).map(g => <tr key={`${g.split}-${g.kind}-${g.group}`}>
          <th>{g.split === 0 ? 'Training (in-sample)' : 'Validation'}</th><td>{g.group}</td><td>{n(g.games)}</td>
          {models.map(([key]) => <td key={key}>{n(g.models[key]?.mse)} / {n(g.models[key]?.ece_pp)} pp</td>)}
        </tr>)}</tbody>
      </table></div>
      <details><summary {...pageStyleTarget('disclosure')}>Rapid calibration bins</summary>
        <div className="algorithm-table-wrap"><table><thead><tr><th>Partition / group</th><th>Model</th><th>Bin</th><th>Game weight</th><th>Predicted points</th><th>Observed points</th></tr></thead>
          <tbody>{report.rapid.groups.filter(g => g.kind === kind).flatMap(g => models.flatMap(([key, label]) => (g.models[key]?.bins || []).map(b =>
            <tr key={`${g.split}-${g.group}-${key}-${b.bin}`}><th>{g.split === 0 ? 'Training' : 'Validation'} / {g.group}</th><td>{label}</td><td>{b.bin*10}–{(b.bin+1)*10}%</td><td>{n(b.game_weight)}</td><td>{n(b.predicted)}</td><td>{n(b.observed)}</td></tr>)))}</tbody>
        </table></div>
      </details>
      {report.rapid.notes?.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
    </> : <p {...pageStyleTarget('text')}>Rapid diagnostics unavailable: {report.rapid?.status?.replaceAll('_', ' ')}</p>}
    {report.bullet?.groups && <>
      <div className="algorithm-table-wrap"><table><caption>Bullet validation — changed blunder decisions</caption>
        <thead><tr><th>Mover rating</th><th>Game-sides</th>{models.map(([key, label]) => <th key={key}>{label}</th>)}<th>Added / removed vs current</th><th>Added / removed vs frozen</th></tr></thead>
        <tbody>{report.bullet.groups.map(g => <tr key={g.rating_band}><th>{g.rating_band}</th><td>{n(g.game_sides)}</td>
          {models.map(([key]) => <td key={key}>{n(g.blunders[key])}</td>)}
          {models.slice(0, 2).map(([key]) => <td key={key}>{n(g.changes[key]?.added)} / {n(g.changes[key]?.removed)}</td>)}
        </tr>)}</tbody>
      </table></div>
      <details><summary {...pageStyleTarget('disclosure')}>Changed-move examples</summary>
        <div className="algorithm-table-wrap"><table><thead><tr><th>Player / rating</th><th>Game / ply</th><th>Before → after</th>{models.map(([key, label]) => <th key={key}>{label}: flag / loss</th>)}</tr></thead>
          <tbody>{report.bullet.examples?.map(e => <tr key={`${e.game}-${e.ply}`}><th>{e.player} / {e.rating}</th><td>{e.game} / {e.ply}</td><td>{n(e.before)} → {n(e.after)}</td>
            {models.map(([key]) => <td key={key}>{e.models[key].blunder} / {n(e.models[key].loss)}</td>)}
          </tr>)}</tbody>
        </table></div>
      </details>
      <p {...pageStyleTarget('text')}>Bullet numerical checks: {report.bullet.status}. Independent scalar sample: {n(report.bullet.scalar_checked_moves)} moves.</p>
      {report.bullet.notes?.map(note => <p {...pageStyleTarget('text')} key={note}>{note}</p>)}
    </>}
  </section>
}
