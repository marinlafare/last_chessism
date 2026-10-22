import { pageStyleTarget } from '../../appearance/styleCore.js'
import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import './analysis-disclosure.css'

const BUTTON_TARGETS = {
  'analysis-local': 'local.toggle',
  'analysis-gcloud': 'cloud.toggle',
  'analysis-cloud-run': 'cloud.run.toggle',
  'analysis-batch-spot': 'cloud.batch.toggle',
  'cloud-run-single': 'cloud.run.single.toggle',
  'cloud-run-combined': 'cloud.run.combined.toggle',
  'cloud-batch-single': 'cloud.single.toggle',
  'cloud-batch-combined': 'cloud.combined.toggle',
}

const PANEL_TARGETS = {
  'analysis-local': 'local.panel',
  'analysis-gcloud': 'cloud.panel',
  'analysis-cloud-run': 'cloud.run.panel',
  'analysis-batch-spot': 'cloud.batch.panel',
  'cloud-run-single': 'cloud.run.single.panel',
  'cloud-run-combined': 'cloud.run.combined.panel',
  'cloud-batch-single': 'cloud.single.panel',
  'cloud-batch-combined': 'cloud.combined.panel',
}

const DisclosureGroup = createContext(null)

export function AnalysisDisclosureGroup({ children, enabled = true }) {
  const [activeId, setActiveId] = useState(null)
  const value = useMemo(() => ({ activeId, setActiveId, enabled }), [activeId, enabled])
  return <DisclosureGroup.Provider value={value}>{children}</DisclosureGroup.Provider>
}

export function useAnalysisDisclosure(id, revealKey) {
  const group = useContext(DisclosureGroup)
  const [standaloneOpen, setStandaloneOpen] = useState(false)
  const [visited, setVisited] = useState(false)
  const setActiveId = group?.setActiveId
  const open = group ? group.enabled && group.activeId === id : standaloneOpen
  // Reveal newly restored work once, without reopening on every progress poll.
  // A late restore must not override a sibling the user has already opened.
  useEffect(() => {
    if (!revealKey) return
    if (setActiveId) setActiveId(current => current ?? id)
    else setStandaloneOpen(true)
    setVisited(true)
  }, [id, revealKey, setActiveId])
  const toggle = () => {
    setVisited(true)
    if (setActiveId) setActiveId(current => current === id ? null : id)
    else setStandaloneOpen(current => !current)
  }
  const openSiblingId = group?.enabled && group.activeId !== id ? group.activeId : null
  return { open, visited, toggle, openSiblingId }
}

export default function AnalysisDisclosure({ id, label, indicator, children, revealKey, level = 2, lazy = false,
  hidden = false, hideWhenOpen = null }) {
  const { open, visited, toggle, openSiblingId } = useAnalysisDisclosure(id, revealKey)
  const target = PANEL_TARGETS[id] || 'card'
  const concealed = hidden || Boolean(hideWhenOpen && openSiblingId === hideWhenOpen)
  const Heading = `h${level}`
  return <section {...pageStyleTarget(target)} className={`analysis-disclosure${open ? ' is-open' : ''}`} aria-label={`${label} analysis`} hidden={concealed}>
    <Heading {...pageStyleTarget('title')} className="analysis-disclosure-heading">
      <button {...pageStyleTarget(BUTTON_TARGETS[id] || 'button')} className="analysis-disclosure-toggle" type="button" id={`${id}-toggle`}
        aria-label={label} aria-expanded={open} aria-controls={`${id}-panel`}
        onClick={toggle}>
        {indicator && <span className="analysis-disclosure-indicator" aria-live="polite">{indicator}</span>}
        <span className="analysis-disclosure-name">{label}</span>
        <svg className="analysis-disclosure-chevron" viewBox="0 0 20 20" aria-hidden="true">
          <path d="m5 7.5 5 5 5-5" />
        </svg>
      </button>
    </Heading>
    {/* Lazy editors mount on first use, then retain their unsaved input on collapse. */}
    <div {...pageStyleTarget(PANEL_TARGETS[id] ? `${target}.body` : 'card')} id={`${id}-panel`} className="analysis-disclosure-panel" role="region"
      aria-labelledby={`${id}-toggle`} hidden={!open}>
      <AnalysisDisclosureGroup enabled={open && !concealed}>{(!lazy || visited) && children}</AnalysisDisclosureGroup>
    </div>
  </section>
}
