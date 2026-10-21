import { useEffect, useId, useRef } from 'react'
import { createPortal } from 'react-dom'
import { pageStyleTarget } from '../../appearance/styleCore'
import { CloudPerformanceDetails, cloudPerformanceTitle } from './CloudPerformanceSummary'
import './cloudPerformanceReport.css'

export default function CloudPerformanceReportModal({ run, onClose }) {
  const panel = useRef(null), close = useRef(onClose)
  const titleId = useId()
  close.current = onClose

  useEffect(() => {
    const previous = document.activeElement
    const overflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    panel.current?.focus()
    const keydown = event => {
      if (event.defaultPrevented || event.target?.closest?.('[data-appearance-tools]')) return
      if (event.key === 'Escape') {
        event.preventDefault(); event.stopPropagation(); close.current()
      }
      if (event.key !== 'Tab' || !panel.current) return
      const nodes = [...panel.current.querySelectorAll('button:not(:disabled), [tabindex="0"]')]
      const first = nodes[0], last = nodes.at(-1)
      if (event.shiftKey && (document.activeElement === first || document.activeElement === panel.current)) {
        event.preventDefault(); last?.focus()
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault(); first?.focus()
      }
    }
    document.addEventListener('keydown', keydown)
    return () => {
      document.body.style.overflow = overflow
      document.removeEventListener('keydown', keydown)
      const target = previous?.isConnected ? previous : document.getElementById('analysis-gcloud-toggle')
      target?.focus({ preventScroll: true })
    }
  }, [])

  return createPortal(<div className="cloud-performance-backdrop" onClick={event => {
    if (event.target === event.currentTarget) onClose()
  }}>
    <section {...pageStyleTarget('cloud.performance.panel')} className="cloud-performance-modal" ref={panel}
      role="dialog" aria-modal="true" aria-labelledby={titleId} tabIndex={-1}>
      <header>
        <h3 {...pageStyleTarget('title')} id={titleId}>{cloudPerformanceTitle(run.performance)}</h3>
        <button {...pageStyleTarget('cloud.performance.close')} type="button" onClick={onClose} aria-label="Close performance report">×</button>
      </header>
      <div className="cloud-performance-body" tabIndex={0} role="region" aria-label="Performance report data">
        <CloudPerformanceDetails run={run} />
      </div>
    </section>
  </div>, document.body)
}
