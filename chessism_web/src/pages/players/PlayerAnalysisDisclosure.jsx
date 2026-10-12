import { createContext, useContext, useId, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import './player-analysis-disclosure.css'

const AnalysisVisible = createContext(true)
export const useAnalysisVisible = () => useContext(AnalysisVisible)

export default function PlayerAnalysisDisclosure({ label, children, target = 'card', level = 3, disabled = false }) {
  const [open, setOpen] = useState(false)
  const [visited, setVisited] = useState(false)
  const parentVisible = useAnalysisVisible()
  const id = useId()
  const Heading = `h${level}`
  return (
    <section {...pageStyleTarget(target)} className={`player-analysis-disclosure${level === 2 ? ' player-analysis-group' : ''}`}>
      <Heading {...pageStyleTarget('title')} className="player-analysis-disclosure-heading">
        <button {...pageStyleTarget('button')} type="button" className="player-analysis-disclosure-toggle"
          id={`${id}-toggle`} aria-expanded={open} aria-controls={`${id}-content`} disabled={disabled}
          onClick={() => { setVisited(true); setOpen(value => !value) }}>
          <span>{label}</span>
          <svg viewBox="0 0 20 20" aria-hidden="true"><path d="m5 7.5 5 5 5-5" /></svg>
        </button>
      </Heading>
      <div id={`${id}-content`} className="player-analysis-disclosure-content" hidden={!open}
        role="region" aria-labelledby={`${id}-toggle`}>
        {/* Mount on first expansion, then preserve filters/zoom while hidden. */}
        <AnalysisVisible.Provider value={parentVisible && open && !disabled}>
          {visited ? children : null}
        </AnalysisVisible.Provider>
      </div>
    </section>
  )
}
