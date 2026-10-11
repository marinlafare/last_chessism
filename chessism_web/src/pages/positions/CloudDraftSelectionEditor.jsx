import { useState } from 'react'
import { cloudDefaults, cloudGamePreview } from './cloudAnalysisForm'
import { draftEntryForm } from './cloudGameDraft'
import { previewPlayerGameAnalysis } from './positionsApi'

export default function CloudDraftSelectionEditor({ entry, disabled, onSave, onCancel }) {
  const [form, setForm] = useState(() => entry ? draftEntryForm(entry) : { ...cloudDefaults })
  const [preview, setPreview] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const change = (key, value) => {
    setForm(old => ({ ...old, [key]: value })); setPreview(null); setError('')
  }
  const inspect = async () => {
    setLoading(true); setError(''); setPreview(null)
    try { setPreview(await previewPlayerGameAnalysis(cloudGamePreview(form))) }
    catch (err) { setError(err.message) }
    finally { setLoading(false) }
  }
  return <fieldset className="cloud-draft-editor" disabled={disabled || loading} aria-label="Player-game draft selection">
    <legend>{entry ? 'Edit saved selection' : 'Add player games'}</legend>
    <label>Player
      <input className="text-input" aria-label="Draft player" maxLength={200} value={form.player}
        onChange={e => change('player', e.target.value)} placeholder="chess.com nickname" />
    </label>
    <label>Game selection
      <select className="text-input" aria-label="Draft game selection" value={form.selection} onChange={e => change('selection', e.target.value)}>
        <option value="latest">Latest games</option><option value="oldest">Oldest games</option>
        <option value="range">Date range</option><option value="fair_range">Fair range</option>
      </select>
    </label>
    {['range', 'fair_range'].includes(form.selection) && <>
      <label>From<input className="text-input" type="date" value={form.from} onChange={e => change('from', e.target.value)} /></label>
      <label>To<input className="text-input" type="date" value={form.to} onChange={e => change('to', e.target.value)} /></label>
    </>}
    {form.selection === 'range' && <>
      <label><input type="checkbox" checked={form.allGames} onChange={e => change('allGames', e.target.checked)} />All games in range</label>
      <label>Range order<select className="text-input" value={form.order} onChange={e => change('order', e.target.value)}>
        <option value="latest">Newest first</option><option value="oldest">Oldest first</option>
      </select></label>
    </>}
    {!(form.selection === 'range' && form.allGames) && <label>Games
      <input className="text-input" aria-label="Draft games" type="number" min="1" max="50000" step="1"
        value={form.games} onChange={e => change('games', e.target.value)} />
    </label>}
    <div className="cloud-draft-actions">
      <button className="btn btn-secondary" type="button" disabled={!form.player.trim()} onClick={inspect}>
        {loading ? 'Previewing games…' : 'Preview selection'}
      </button>
    </div>
    {preview && <p role="status">{preview.selected_games.toLocaleString('en-US')} selected games;
      {' '}{preview.fens_to_analyze.toLocaleString('en-US')} missing FENs before combined deduplication and reservation checks.</p>}
    {error && <p role="alert">{error}</p>}
    <div className="cloud-draft-actions">
      <button className="btn" type="button" disabled={!preview?.plan_id}
        onClick={() => onSave(preview.plan_id, entry?.id)}>{entry ? 'Save edited selection' : 'Add FENs'}</button>
      {entry && <button className="btn btn-secondary" type="button" onClick={onCancel}>Cancel edit</button>}
    </div>
    <small>{preview?.plan_id ? `${entry ? 'Save this selection in' : 'Add these games’ missing FENs to'} the saved set. This does not start a cloud job.`
      : 'Preview the games to enable saving their missing FENs to the set.'}</small>
  </fieldset>
}
