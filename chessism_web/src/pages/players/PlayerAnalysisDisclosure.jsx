import { createContext, useContext, useId, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import './player-analysis-disclosure.css'

const AnalysisVisible = createContext(true)
export const useAnalysisVisible = () => useContext(AnalysisVisible)

export default function PlayerAnalysisDisclosure({ label, children, buttonTarget, target = 'card', level = 3, disabled = false, loading = false, onOpen }) {
  const [open, setOpen] = useState(false)
  const [visited, setVisited] = useState(false)
  const parentVisible = useAnalysisVisible()
  const id = useId()
  const Heading = `h${level}`
  return (
    <section {...pageStyleTarget(target)} className={`player-analysis-disclosure${level === 2 ? ' player-analysis-group' : ''}`}>
      <Heading {...pageStyleTarget('title')} className="player-analysis-disclosure-heading">
        <button {...pageStyleTarget(buttonTarget)} type="button" className="player-analysis-disclosure-toggle"
          id={`${id}-toggle`} aria-expanded={open} aria-controls={`${id}-content`} disabled={disabled || loading}
          aria-busy={loading || undefined}
          onClick={event => { event.currentTarget.focus({ preventScroll: true }); setVisited(true); if (!open) onOpen?.(); setOpen(value => !value) }}
          onKeyDown={event => {
            if (!open || !['player.opponents.toggle', 'player.openings.toggle'].includes(buttonTarget)
              || !['ArrowUp', 'ArrowDown'].includes(event.key)) return
            event.preventDefault()
            const list = document.getElementById(`${id}-content`)?.querySelector('.player-game-catalog-rows')
            list?.focus({ preventScroll: true })
            list?.dispatchEvent(new KeyboardEvent('keydown', { key: event.key, bubbles: true, cancelable: true }))
          }}>
          <span>{label}</span>
          {loading ? <i className="player-analysis-disclosure-spinner" aria-hidden="true" />
            : <svg viewBox="0 0 20 20" aria-hidden="true"><path d="m5 7.5 5 5 5-5" /></svg>}
        </button>
      </Heading>
      <div id={`${id}-content`} className="player-analysis-disclosure-content" hidden={!open}
        role="region" aria-labelledby={`${id}-toggle`}>
        {/* Mount on first expansion, then preserve filters/zoom while hidden. */}
        <AnalysisVisible.Provider value={parentVisible && open && !disabled && !loading}>
          {visited ? children : null}
        </AnalysisVisible.Provider>
      </div>
    </section>
  )
}
