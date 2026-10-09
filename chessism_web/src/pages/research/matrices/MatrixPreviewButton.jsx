export default function MatrixPreviewButton({ name, onClick, live = false }) {
  return (
    <button className="matrix-view-button" type="button" onClick={onClick} title={live ? 'Live preview—not a saved snapshot' : 'View saved snapshot'} aria-label={`Preview ${name}`}>
      <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5" aria-hidden="true"><rect x="2" y="2" width="16" height="16" rx="2" /><path d="M2 7h16M2 12h16M7 2v16M12 2v16" /></svg>
    </button>
  )
}
