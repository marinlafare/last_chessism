import { useState } from 'react'
import './analysis-disclosure.css'

export default function AnalysisDisclosure({ id, label, status, children }) {
  const [open, setOpen] = useState(false)
  return <section className={`analysis-disclosure${open ? ' is-open' : ''}`} aria-label={`${label} analysis`}>
    <h2 className="analysis-disclosure-heading">
      <button className="analysis-disclosure-toggle" type="button" id={`${id}-toggle`}
        aria-label={label} aria-expanded={open} aria-controls={`${id}-panel`}
        onClick={() => setOpen(value => !value)}>
        <span className="analysis-disclosure-name">{label}</span>
        {status && <span className="analysis-disclosure-status" aria-live="polite">{status}</span>}
        <svg className="analysis-disclosure-chevron" viewBox="0 0 20 20" aria-hidden="true">
          <path d="m5 7.5 5 5 5-5" />
        </svg>
      </button>
    </h2>
    {/* Keep hooks, drafts and polling mounted when collapsed. Visibility only. */}
    <div id={`${id}-panel`} className="analysis-disclosure-panel" role="region"
      aria-labelledby={`${id}-toggle`} hidden={!open}>{children}</div>
  </section>
}
