import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useRef, useState } from 'react'
import { fetchCloudDraft, createCloudDraft, saveCloudDraftEntry, removeCloudDraftEntry,
  previewCloudDraft, submitCloudDraft } from './positionsApi'
import { draftSubmitError } from './cloudGameDraft'
import { MAX_BATCH_VMS } from './cloudAnalysisForm'
import CloudDraftSelectionEditor from './CloudDraftSelectionEditor'
import CloudDraftGroupData from './CloudDraftGroupData'

export default function CloudGameDraft({ launchDisabled, onSubmitted, initialDraft = null, onDraftChange, nVms, onVmChange }) {
  const [draft, setDraft] = useState(initialDraft)
  const [loading, setLoading] = useState(true)
  const [ready, setReady] = useState(false)
  const [error, setError] = useState('')
  const [phase, setPhase] = useState('Loading saved steps…')
  const [editing, setEditing] = useState(null)
  const [pending, setPending] = useState(false)
  const [editorKey, setEditorKey] = useState(0)
  const working = useRef(false)
  useEffect(() => { onDraftChange?.(draft) }, [draft, onDraftChange])
  useEffect(() => {
    let live = true
    fetchCloudDraft().then(value => { if (live) { setDraft(value); setReady(true) } })
      .catch(err => { if (live) setError(err.message) })
      .finally(() => { if (live) setLoading(false) })
    return () => { live = false }
  }, [])
  const resetEditor = () => { setEditing(null); setPending(false); setEditorKey(key => key + 1) }
  const perform = async (operation, { reset = false, group = false } = {}) => {
    if (loading || working.current) return
    working.current = true
    setLoading(true); setError(''); setPhase('Saving steps…')
    try {
      const result = await operation()
      setDraft(result); setReady(true)
      if (reset) resetEditor()
      // Save first: a failed calculation must not discard or duplicate a step.
      if (group && result?.entries.length) {
        setPhase('Calculating group data…')
        setDraft(await previewCloudDraft(result.id, result.revision))
      }
    } catch (err) { setError(err.message) }
    finally { setLoading(false); working.current = false }
  }
  const save = (planId, entryId) => perform(async () => {
    const current = draft || await createCloudDraft()
    setDraft(current)
    return saveCloudDraftEntry(current.id, {
      revision: current.revision, plan_id: planId, ...(entryId ? { replace_entry_id: entryId } : {}),
    })
  }, { reset: true, group: true })
  const submitError = draftSubmitError(draft, nVms)
  const submit = async () => {
    if (loading || working.current || launchDisabled || submitError || pending || editing) return
    working.current = true
    setLoading(true); setError(''); setPhase('Starting process…')
    try {
      const result = await submitCloudDraft(draft.id, { revision: draft.revision, n_vms: Number(nVms) })
      // Preserve the submitted identity even if refreshing progress fails.
      setDraft(old => ({ ...old, submitted_job_id: result.id }))
      await onSubmitted?.()
    } catch (err) { setError(err.message) }
    finally { setLoading(false); working.current = false }
  }
  const entries = draft?.entries || []
  const showEditor = editing || entries.length < (draft?.max_entries ?? 100)
  const step = editing ? entries.findIndex(entry => entry.id === editing.id) + 1 : entries.length + 1
  return <section {...pageStyleTarget('cloud.draft')} className="cloud-game-draft" aria-label="Combined player-game selection" aria-busy={loading}>
    {error && <div role="alert" className="status-banner warn">
      <p {...pageStyleTarget('text')}>{error}</p>
      <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button" disabled={loading}
        onClick={() => perform(fetchCloudDraft, { reset: true })}>Reload saved draft</button>
    </div>}
    {draft?.submitted_job_id ? <>
      <p {...pageStyleTarget('text')} role="status">Submitted as job {draft.submitted_job_id}. Follow its progress below.</p>
      <button {...pageStyleTarget('button')} className="btn" type="button" disabled={loading}
        onClick={() => perform(createCloudDraft, { reset: true })}>Start another process</button>
    </> : <>
      <div className="cloud-draft-process-settings" aria-label="Combined process settings">
        <label {...pageStyleTarget('label')} className="cloud-draft-vms">VM
          <input {...pageStyleTarget('field')} className="text-input" aria-label="Combined Spot VMs"
            type="number" min="1" max={MAX_BATCH_VMS} step="1" required
            disabled={loading || !ready} value={nVms} onChange={e => onVmChange(e.target.value)} />
        </label>
      </div>
      {!!entries.length && <ol className="cloud-draft-entries" aria-label="Saved game selections">
        {entries.map((entry, index) => <li key={entry.id}>
          <button {...pageStyleTarget('button')} type="button" className="cloud-draft-step"
            aria-label={`Edit step ${index + 1}: ${entry.player_name}`} aria-pressed={editing?.id === entry.id}
            title={`${entry.player_name} · ${entry.selected_games.toLocaleString('en-US')} games · ${entry.preview_fens.toLocaleString('en-US')} FENs`}
            disabled={loading || pending} onClick={() => { setEditing(entry); setPending(true); setEditorKey(key => key + 1) }}>
            {index + 1}
          </button>
        </li>)}
      </ol>}
      <section {...pageStyleTarget('cloud.selection')} className="cloud-draft-builder" aria-label="Combined selection setup">
        <div className="cloud-draft-builder-heading">
          {showEditor && <span {...pageStyleTarget('value')} className="cloud-draft-step-number" aria-label={`Step ${step}`}>{step}</span>}
        </div>
        {showEditor ? <CloudDraftSelectionEditor
          key={editorKey} entry={editing} disabled={loading || !ready} onSave={save}
          onPendingChange={setPending} onCancel={resetEditor}
          onRemove={editing ? () => perform(() => removeCloudDraftEntry(draft.id, editing.id, draft.revision), { reset: true, group: true }) : null} />
          : <p {...pageStyleTarget('text')}>Maximum {draft.max_entries} steps. Select a square to edit or remove a step.</p>}
      </section>
      <CloudDraftGroupData draft={draft} nVms={nVms} loading={loading} phase={phase}
        onRefresh={() => perform(() => previewCloudDraft(draft.id, draft.revision))} />
      {launchDisabled && <p {...pageStyleTarget('text')}>Submission requires an online controller and no unfinished cloud job. You can still prepare steps.</p>}
      {pending ? <p {...pageStyleTarget('text')}>Add or clear the current step before running.</p>
        : submitError && !!entries.length && <p {...pageStyleTarget('text')}>{submitError}</p>}
      <button {...pageStyleTarget('button')} className="btn cloud-draft-run" type="button"
        disabled={loading || !ready || launchDisabled || !!submitError || pending || !!editing}
        onClick={submit}>Run process</button>
      <small {...pageStyleTarget('text')}>Billable Google Cloud work. Keep the PC on for uploads, result imports and cleanup. No total-runtime or monetary cap.</small>
    </>}
  </section>
}
