import { useEffect, useId, useRef } from 'react'
import { createPortal } from 'react-dom'
import { pageStyleTarget } from '../../appearance/styleCore'
import './cloudStopConfirmation.css'

export default function CloudStopConfirmation({ backend, onCancel, onConfirm }) {
  const dialog = useRef(null), cancel = useRef(null), submitted = useRef(false), close = useRef(onCancel)
  close.current = onCancel
  const title = useId(), description = useId()
  useEffect(() => {
    const previous = document.activeElement, node = dialog.current
    const overflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    cancel.current?.focus()
    const keydown = event => {
      if (event.defaultPrevented || event.target?.closest?.('[data-appearance-tools]')) return
      if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); close.current() }
      if (event.key !== 'Tab') return
      const buttons = [...node.querySelectorAll('button:not(:disabled)')]
      if (event.shiftKey && document.activeElement === buttons[0]) {
        event.preventDefault(); buttons.at(-1)?.focus()
      } else if (!event.shiftKey && document.activeElement === buttons.at(-1)) {
        event.preventDefault(); buttons[0]?.focus()
      }
    }
    document.addEventListener('keydown', keydown)
    return () => {
      document.removeEventListener('keydown', keydown)
      document.body.style.overflow = overflow
      if (previous?.isConnected) previous.focus({ preventScroll: true })
    }
  }, [])
  return createPortal(<div className="cloud-stop-confirmation-backdrop">
    <section {...pageStyleTarget('cloud.stop.dialog')} ref={dialog}
    className="cloud-stop-confirmation" role="dialog" aria-modal="true" aria-labelledby={title} aria-describedby={description}>
    <h3 id={title}>Stop and clean up {backend === 'cloud_run' ? 'Cloud Run' : 'Batch Spot'}?</h3>
    <p id={description}>Completed result batches will be saved. Unfinished analysis will stop, and temporary cloud data
      and checkpoints will be deleted. This job cannot be restarted.</p>
    <footer>
      <button {...pageStyleTarget('cloud.stop.cancel')} ref={cancel} type="button" onClick={onCancel}>Cancel</button>
      <button {...pageStyleTarget('cloud.stop.yes')} type="button" onClick={() => {
        if (submitted.current) return
        submitted.current = true
        onConfirm()
      }}>Yes, stop and clean up</button>
    </footer>
  </section></div>, document.body)
}
