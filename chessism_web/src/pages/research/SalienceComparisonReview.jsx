import { useState } from 'react'
import { pageStyleTarget as style } from '../../appearance/styleCore'
import { saveComparisonReview } from './salienceComparisonApi'

export default function SalienceComparisonReview({ snapshot, gameId, data, positionIndex, onSaved }) {
  const comparison = data.comparison
  const position = comparison.positions[positionIndex - 1]
  const [judgement, setJudgement] = useState(comparison.review?.judgement || 'uncertain')
  const [note, setNote] = useState(comparison.review?.note || '')
  const [message, setMessage] = useState('')
  const [busy, setBusy] = useState(false)
  const save = async () => {
    setBusy(true); setMessage('')
    try { await saveComparisonReview(snapshot, gameId, judgement, note); setMessage('Review saved'); onSaved?.() }
    catch (error) { setMessage(error.message || 'Could not save review') }
    finally { setBusy(false) }
  }
  return <section {...style('review.panel')} className="salience-experiment-review">
    <h3 {...style('review.heading')}>Experimental salience · raw 0–1</h3>
    <div className="salience-review-scores">{['current', 'equal', 'opening'].map((key, index) =>
      <div {...style(`review.score.${key}`)} key={key}><span>{['Current', 'Equal · all', 'Equal · first 15'][index]}</span>
        <strong>{comparison.scores[key].toFixed(4)}</strong></div>)}</div>
    <div {...style('review.position')} className="salience-position-detail" aria-live="polite">
      {position ? <>
        <strong>Half-move {position.ply} · {position.in_window ? 'Inside first 15 moves' : 'Outside first 15 moves'}</strong>
        <span>Occurrence {position.occurrence} in this game</span>
        <span>Games containing this position: {position.games_with_position.toLocaleString()} overall;
          {' '}{position.opening_games_with_position.toLocaleString()} in the opening window</span>
        <table><thead><tr><th>Variant</th><th>Weight</th><th>Contribution</th></tr></thead>
          <tbody>{['current', 'equal', 'opening'].map((key, index) => <tr key={key}>
            <td>{['Current', 'Equal · all', 'Equal · first 15'][index]}</td>
            <td>{key === 'current' ? position.depth_weight.toFixed(3) : key === 'opening' && !position.in_window ? 'Excluded' : '1'}</td>
            <td>{position[`${key}_contribution`].toFixed(6)}</td>
          </tr>)}</tbody></table>
        <small>Contributions sum to the game score. Use ← / → or select a move.</small>
      </> : <span>Select a move to inspect its repetition counts and score contributions.</span>}
    </div>
    <fieldset {...style('review.form')} disabled={busy}>
      <legend>Your assessment of repetition</legend>
      <div className="salience-review-choices">{['repetitive','distinctive','uncertain'].map(value =>
        <button {...style(`review.choice.${value}`)} key={value} type="button" aria-pressed={judgement === value}
          onClick={() => { setJudgement(value); setMessage('') }}>{value}</button>)}</div>
      <label htmlFor="salience-review-note">Notes (optional)</label>
      <textarea {...style('review.note')} id="salience-review-note" value={note} maxLength={2000} rows={2}
        onChange={event => { setNote(event.target.value); setMessage('') }} />
      <button {...style('review.save')} type="button" onClick={save}>{busy ? 'Saving…' : 'Save assessment'}</button>
      <span {...style('review.status')} role="status">{message}</span>
    </fieldset>
  </section>
}
