import { lazy, Suspense, useId, useState } from 'react'
import { styleTarget } from '../../appearance/styleCore'
import './commonPositionsDisclosure.css'
const GraphExplorer = lazy(() => import('./PlyMapExplorer'))

export default function CommonPositionsDisclosure() {
  const [open, setOpen] = useState(false)
  const id = useId()

  return (
    <section {...styleTarget('commonPositions.panel')} className="scored-positions-panel common-positions-panel">
      <h2 className="common-positions-heading">
        <button {...styleTarget('commonPositions.toggle')}
          type="button" className="common-positions-toggle" id={`${id}-toggle`}
          aria-expanded={open} aria-controls={`${id}-content`}
          onClick={() => setOpen((current) => !current)}>
          <span>Neo4j test</span>
          <svg viewBox="0 0 20 20" aria-hidden="true" focusable="false">
            <path d="m5 7.5 5 5 5-5" />
          </svg>
        </button>
      </h2>
      <div {...styleTarget('commonPositions.content')} className="common-positions-content"
        id={`${id}-content`} role="region" aria-labelledby={`${id}-toggle`} hidden={!open}>
        {open && <Suspense fallback={<p>Loading opening map…</p>}><GraphExplorer /></Suspense>}
      </div>
    </section>
  )
}
