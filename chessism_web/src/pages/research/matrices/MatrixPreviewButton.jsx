import { pageStyleTarget } from '../../../appearance/styleCore.js'
export default function MatrixPreviewButton({ name, onClick, live = false, disabled = false }) {
  return (
    <button {...pageStyleTarget('button')} className="matrix-view-button" type="button" disabled={disabled} onClick={onClick} title={live ? 'Preview dataframe' : 'View saved snapshot'} aria-label={`Preview ${name}`}>
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z" /><circle cx="12" cy="12" r="3" /></svg>
    </button>
  )
}
