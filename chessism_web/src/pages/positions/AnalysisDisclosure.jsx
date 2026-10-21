import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import './analysis-disclosure.css'

const BUTTON_TARGETS = {
  'analysis-local': 'local.toggle',
  'analysis-gcloud': 'cloud.toggle',
  'analysis-cloud-run': 'cloud.run.toggle',
  'analysis-batch-spot': 'cloud.batch.toggle',
  'cloud-batch-single': 'cloud.single.toggle',
  'cloud-batch-combined': 'cloud.combined.toggle',
}

export default function AnalysisDisclosure({ id, label, status, children, revealKey, level = 2, lazy = false, hidden = false }) {
  const [open, setOpen] = useState(false)
  const [visited, setVisited] = useState(false)
  // Reveal newly restored work once, without reopening on every progress poll.
  useEffect(() => { if (revealKey) { setOpen(true); setVisited(true) } }, [revealKey])
  const target=id==='analysis-local'?'local.panel':'cloud.panel'
  const Heading = `h${level}`
  return <section {...pageStyleTarget(target)} className={`analysis-disclosure${open ? ' is-open' : ''}`} aria-label={`${label} analysis`} hidden={hidden}>
    <Heading {...pageStyleTarget('title')} className="analysis-disclosure-heading">
      <button {...pageStyleTarget(BUTTON_TARGETS[id] || 'button')} className="analysis-disclosure-toggle" type="button" id={`${id}-toggle`}
        aria-label={label} aria-expanded={open} aria-controls={`${id}-panel`}
        onClick={() => { setVisited(true); setOpen(value => !value) }}>
        <span className="analysis-disclosure-name">{label}</span>
        {status && <span className="analysis-disclosure-status" aria-live="polite">{status}</span>}
        <svg className="analysis-disclosure-chevron" viewBox="0 0 20 20" aria-hidden="true">
          <path d="m5 7.5 5 5 5-5" />
        </svg>
      </button>
    </Heading>
    {/* Lazy editors mount on first use, then retain their unsaved input on collapse. */}
    <div id={`${id}-panel`} className="analysis-disclosure-panel" role="region"
      aria-labelledby={`${id}-toggle`} hidden={!open}>{(!lazy || visited) && children}</div>
  </section>
}
