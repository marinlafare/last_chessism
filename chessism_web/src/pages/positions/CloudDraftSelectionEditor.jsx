import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useState } from 'react'
import { cloudDefaults, cloudGamePreview } from './cloudAnalysisForm'
import { draftEntryForm } from './cloudGameDraft'
import { previewPlayerGameAnalysis } from './positionsApi'

export default function CloudDraftSelectionEditor({ entry, disabled, onSave, onCancel, onRemove, onPendingChange }) {
  const [form, setForm] = useState(() => entry ? draftEntryForm(entry) : { ...cloudDefaults })
  const [preview, setPreview] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [dirty, setDirty] = useState(false)
  const change = (key, value) => {
    setForm(old => ({ ...old, [key]: value })); setPreview(null); setError('')
    setDirty(true); onPendingChange(true)
  }
  const inspect = async () => {
    setLoading(true); setError(''); setPreview(null)
    try { setPreview(await previewPlayerGameAnalysis(cloudGamePreview(form))) }
    catch (err) { setError(err.message) }
    finally { setLoading(false) }
  }
  return <fieldset className="cloud-draft-editor" disabled={disabled || loading} aria-label="Player-game draft selection">
    <label {...pageStyleTarget('label')}>Player
      <input {...pageStyleTarget('field')} className="text-input" aria-label="Draft player" maxLength={200} value={form.player}
        onChange={e => change('player', e.target.value)} placeholder="chess.com nickname" />
    </label>
    <label {...pageStyleTarget('label')}>Game selection
      <select {...pageStyleTarget('field')} className="text-input" aria-label="Draft game selection" value={form.selection} onChange={e => change('selection', e.target.value)}>
        <option value="latest">Latest games</option><option value="oldest">Oldest games</option>
        <option value="range">Date range</option><option value="fair_range">Fair range</option>
      </select>
    </label>
    {['range', 'fair_range'].includes(form.selection) && <>
      <label {...pageStyleTarget('label')}>From<input {...pageStyleTarget('field')} className="text-input" type="date" value={form.from} onChange={e => change('from', e.target.value)} /></label>
      <label {...pageStyleTarget('label')}>To<input {...pageStyleTarget('field')} className="text-input" type="date" value={form.to} onChange={e => change('to', e.target.value)} /></label>
    </>}
    {form.selection === 'range' && <>
      <label {...pageStyleTarget('label')}><input {...pageStyleTarget('field')} type="checkbox" checked={form.allGames} onChange={e => change('allGames', e.target.checked)} />All games in range</label>
      <label {...pageStyleTarget('label')}>Range order<select {...pageStyleTarget('field')} className="text-input" value={form.order} onChange={e => change('order', e.target.value)}>
        <option value="latest">Newest first</option><option value="oldest">Oldest first</option>
      </select></label>
    </>}
    {!(form.selection === 'range' && form.allGames) && <label {...pageStyleTarget('label')}>Games
      <input {...pageStyleTarget('field')} className="text-input" aria-label="Draft games" type="number" min="1" max="50000" step="1"
        value={form.games} onChange={e => change('games', e.target.value)} />
    </label>}
    <div className="cloud-draft-actions cloud-draft-calculate">
      <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button" disabled={!form.player.trim()} onClick={inspect}>
        {loading ? 'Calculating…' : 'Calculate'}
      </button>
    </div>
    {preview && <p {...pageStyleTarget('text')} role="status">{preview.fens_to_analyze.toLocaleString('en-US')} FENs</p>}
    {error && <p {...pageStyleTarget('text')} role="alert">{error}</p>}
    <div className="cloud-draft-actions">
      {preview?.plan_id && <button {...pageStyleTarget('button')} className="btn" type="button"
        onClick={() => onSave(preview.plan_id, entry?.id)}>{entry ? 'Save step' : 'Add'}</button>}
      {(entry || dirty) && <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button" onClick={onCancel}>{entry ? 'Cancel edit' : 'Clear step'}</button>}
      {entry && <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button" onClick={onRemove}>Remove step</button>}
    </div>
  </fieldset>
}
