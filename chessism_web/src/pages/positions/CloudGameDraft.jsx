import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import { fetchCloudDraft, createCloudDraft, saveCloudDraftEntry, removeCloudDraftEntry,
  previewCloudDraft, submitCloudDraft } from './positionsApi'
import { MAX_BATCH_VMS } from './cloudAnalysisForm'
import { draftSubmitError, draftTaskSummary, draftVmPlan } from './cloudGameDraft'
import CloudDraftSelectionEditor from './CloudDraftSelectionEditor'

export default function CloudGameDraft({ launchDisabled, onSubmitted, initialDraft = null, onDraftChange, nVms, onVmChange }) {
  const [draft, setDraft] = useState(initialDraft)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [editing, setEditing] = useState(null)
  const [editorKey, setEditorKey] = useState(0)
  useEffect(() => { onDraftChange?.(draft) }, [draft, onDraftChange])
  useEffect(() => {
    let live = true
    fetchCloudDraft().then(value => { if (live) setDraft(value) })
      .catch(err => { if (live) setError(err.message) })
      .finally(() => { if (live) setLoading(false) })
    return () => { live = false }
  }, [])
  const perform = async (operation, resetEditor = false) => {
    if (loading) return
    setLoading(true); setError('')
    try {
      const result = await operation()
      setDraft(result)
      if (resetEditor) { setEditing(null); setEditorKey(key => key + 1) }
    } catch (err) { setError(err.message) }
    finally { setLoading(false) }
  }
  const save = (planId, entryId) => perform(() => saveCloudDraftEntry(draft.id, {
    revision: draft.revision, plan_id: planId, ...(entryId ? { replace_entry_id: entryId } : {}),
  }), true)
  const submitError = draftSubmitError(draft, nVms)
  const vmPlan = draftVmPlan(draft?.preview?.fens_to_analyze, nVms)
  const submit = async () => {
    if (loading || launchDisabled || submitError || editing) return
    setLoading(true); setError('')
    try {
      const result = await submitCloudDraft(draft.id, { revision: draft.revision, n_vms: Number(nVms) })
      // Preserve the submitted identity even if refreshing progress fails.
      setDraft(old => ({ ...old, submitted_job_id: result.id }))
      await onSubmitted?.()
    } catch (err) { setError(err.message) }
    finally { setLoading(false) }
  }
  return <section {...pageStyleTarget('cloud.draft')} className="cloud-game-draft" aria-label="Combined player-game selection">
    <h4 {...pageStyleTarget('title')}>Combined player-game job</h4>
    <p {...pageStyleTarget('text')}>One shared draft, saved in PostgreSQL. Build it while another cloud job runs.
      Adding or previewing selections does not launch cloud work.</p>
    <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button" disabled={loading}
      onClick={() => perform(fetchCloudDraft, true)}>Reload saved draft</button>
    {loading && <p {...pageStyleTarget('text')} role="status">Working on the selection…</p>}
    {error && <p {...pageStyleTarget('text')} role="alert" className="status-banner warn">{error}</p>}
    {!draft || draft.submitted_job_id ? <>
      {draft?.submitted_job_id && <p {...pageStyleTarget('text')} role="status">Submitted as job {draft.submitted_job_id}. Follow its progress below.</p>}
      <button {...pageStyleTarget('button')} className="btn" type="button" disabled={loading} onClick={() => perform(createCloudDraft, true)}>Start new combined draft</button>
    </> : <>
      <p {...pageStyleTarget('text')}>{draft.entries.length} / {draft.max_entries} selections saved. Exact games are retained; available FEN counts are rechecked before launch.</p>
      <ol className="cloud-draft-entries" aria-label="Saved game selections">{draft.entries.map(entry => <li key={entry.id}>
        <strong {...pageStyleTarget('value')}>{entry.player_name}</strong> · {entry.selection_mode.replace('_', ' ')}
        {entry.date_from && ` · ${entry.date_from} to ${entry.date_to}`}
        <span>{entry.selected_games.toLocaleString('en-US')} games · {entry.preview_fens.toLocaleString('en-US')} missing FENs when added</span>
        <div className="cloud-draft-actions">
          <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button" disabled={loading} onClick={() => { setEditing(entry); setEditorKey(key => key + 1) }}>Edit {entry.player_name}</button>
          <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button" disabled={loading}
            onClick={() => perform(() => removeCloudDraftEntry(draft.id, entry.id, draft.revision), true)}>Remove {entry.player_name}</button>
        </div>
      </li>)}</ol>
      {(editing || draft.entries.length < draft.max_entries) && <CloudDraftSelectionEditor
        key={editorKey} entry={editing} disabled={loading} onSave={save}
        onCancel={() => { setEditing(null); setEditorKey(key => key + 1) }} />}
      <label {...pageStyleTarget('label')}>Parallel Spot VMs per cycle
        <input {...pageStyleTarget('field')} className="text-input" aria-label="Combined Spot VMs" aria-describedby="combined-vms-help"
          type="number" min="1" max={MAX_BATCH_VMS} step="1" required
          disabled={loading} value={nVms} onChange={e => onVmChange(e.target.value)} />
      </label>
      <small {...pageStyleTarget('text')} id="combined-vms-help">Each VM: 16 vCPUs / 16 GiB. Changing the VM count updates the split below immediately;
        it does not change your saved FEN set or require another preview. Concurrency is subject to Google quota.</small>
      <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button" disabled={loading || !draft.entries.length || !!editing}
        onClick={() => perform(() => previewCloudDraft(draft.id, draft.revision))}>Preview combined selection</button>
      {draft.preview && <div className="cloud-draft-preview" role="status">
        <strong {...pageStyleTarget('value')}>{draft.preview.fens_to_analyze.toLocaleString('en-US')} unique available FENs</strong>
        <p {...pageStyleTarget('text')}>{draft.preview.unique_games.toLocaleString('en-US')} unique games; {draft.preview.claimed_fens.toLocaleString('en-US')} missing FENs
          {' '}already reserved elsewhere and excluded. Shared positions across players are counted once.</p>
        <p {...pageStyleTarget('text')}>{draftTaskSummary(draft.preview.fens_to_analyze, nVms)}</p>
        {vmPlan && <>
          <p {...pageStyleTarget('text')}>{Number(nVms)} Spot VM{Number(nVms) === 1 ? '' : 's'} selected · up to {Math.min(Number(nVms), draft.preview.fens_to_analyze) * 16} vCPUs concurrently.
            {' '}VMs within a cycle run in parallel; cycles run sequentially.</p>
          <table {...pageStyleTarget('table')} className="cloud-draft-vm-plan" aria-label="Planned Batch VM allocation">
            <thead><tr><th {...pageStyleTarget('table.header')} scope="col">Cycle</th><th {...pageStyleTarget('table.header')} scope="col">Total FENs</th><th {...pageStyleTarget('table.header')} scope="col">FENs per VM</th></tr></thead>
            <tbody>{vmPlan.map(row => <tr key={row.cycle}>
              <th {...pageStyleTarget('table.header')} scope="row">{row.cycle}</th>
              <td {...pageStyleTarget('table.cell')}>{row.total.toLocaleString('en-US')}</td>
              <td {...pageStyleTarget('table.cell')}>{row.vmFens.map((count, index) => <span key={index}>VM {index + 1}: {count.toLocaleString('en-US')}</span>)}</td>
            </tr>)}</tbody>
          </table>
        </>}
        <small {...pageStyleTarget('text')}>Snapshot, not a reservation. Counts may decrease before launch. Maximum {draft.max_fens.toLocaleString('en-US')} FENs.</small>
      </div>}
      <small {...pageStyleTarget('text')}>Keep the PC on until “Safe to turn off your PC” appears. No total-runtime or monetary cap.</small>
      {launchDisabled && <p {...pageStyleTarget('text')}>Draft editing remains available. Submission requires an online controller and no unfinished cloud job.</p>}
      {submitError && <p {...pageStyleTarget('text')}>{submitError}</p>}
      <button {...pageStyleTarget('button')} className="btn" type="button" disabled={loading || launchDisabled || !!submitError || !!editing}
        onClick={submit}>Create combined Batch job — billable</button>
    </>}
  </section>
}
