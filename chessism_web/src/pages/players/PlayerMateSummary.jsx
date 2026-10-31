import { useEffect, useId, useLayoutEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { pageStyleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import { useAnalysisVisible } from './PlayerAnalysisDisclosure'
import { SCORE_ENDINGS } from './scoreDistribution'
import './player-mate-summary.css'

export default function PlayerMateSummary({ playerName, totals }) {
  const [open, setOpen] = useState(false)
  const [position, setPosition] = useState({ left: 12, top: 12 })
  const trigger = useRef(null), panel = useRef(null), id = useId()
  const visible = useAnalysisVisible()
  const close = () => { setOpen(false); trigger.current?.focus({ preventScroll: true }) }

  useEffect(() => { setOpen(false) }, [playerName, visible])
  useLayoutEffect(() => {
    if (!open) return undefined
    const place = () => {
      const anchor = trigger.current.getBoundingClientRect(), box = panel.current.getBoundingClientRect()
      const left = Math.max(12, Math.min(anchor.left, window.innerWidth - box.width - 12))
      const below = anchor.bottom + 8
      const top = Math.max(12, Math.min(below + box.height <= window.innerHeight - 12 ? below : anchor.top - box.height - 8,
        window.innerHeight - box.height - 12))
      setPosition(previous => previous.left === left && previous.top === top ? previous : { left, top })
    }
    place()
    panel.current.focus({ preventScroll: true })
    const observer = new ResizeObserver(place)
    observer.observe(panel.current); observer.observe(trigger.current)
    window.addEventListener('resize', place)
    window.addEventListener('scroll', place, true)
    return () => {
      observer.disconnect()
      window.removeEventListener('resize', place)
      window.removeEventListener('scroll', place, true)
    }
  }, [open])
  useEffect(() => {
    if (!open) return undefined
    const outside = event => {
      if (event.target.closest?.('[data-appearance-tools]')) return
      if (!panel.current?.contains(event.target) && !trigger.current?.contains(event.target)) setOpen(false)
    }
    const keydown = event => {
      if (event.key !== 'Escape' || event.target.closest?.('[data-appearance-tools]')) return
      event.preventDefault(); event.stopPropagation(); close()
    }
    document.addEventListener('pointerdown', outside)
    document.addEventListener('focusin', outside)
    document.addEventListener('keydown', keydown)
    return () => {
      document.removeEventListener('pointerdown', outside)
      document.removeEventListener('focusin', outside)
      document.removeEventListener('keydown', keydown)
    }
  }, [open])

  return <>
    <button {...pageStyleTarget('player.scores.mates.toggle')} className="score-mate-toggle" ref={trigger}
      type="button" aria-haspopup="dialog" aria-expanded={open} aria-controls={open ? id : undefined}
      onClick={() => setOpen(value => !value)}>
      Mates <svg viewBox="0 0 20 20" aria-hidden="true"><path d="m5 7.5 5 5 5-5" /></svg>
    </button>
    {open && createPortal(<section {...pageStyleTarget('player.scores.mates')} className="score-mate-popup"
      ref={panel} id={id} role="dialog" aria-labelledby={`${id}-title`} tabIndex={-1} style={position}>
      <header className="score-mate-popup-heading">
        <strong {...pageStyleTarget('player.scores.mates.title')} id={`${id}-title`}>Mates and game endings</strong>
        <button {...pageStyleTarget('player.scores.mates.close')} type="button" onClick={close} aria-label="Close mate distances">×</button>
      </header>
      <div className="score-mate-popup-body" tabIndex={0} role="region" aria-label="Mate evaluations and game endings">
        <dl {...pageStyleTarget('player.scores.endings')} className="score-mate-endings" aria-label="Recorded game endings">
          {SCORE_ENDINGS.map(([key, label]) => <div {...pageStyleTarget('player.scores.endings.row')} key={key}>
            <dt {...pageStyleTarget('player.scores.endings.label')}>{label}</dt>
            <dd {...pageStyleTarget('player.scores.endings.value')}>{formatNumber(totals.endings[key] || 0)}</dd>
          </div>)}
        </dl>
        {totals.endings.stalemates_unattributed > 0 && <small {...pageStyleTarget('player.scores.status')}>
          {formatNumber(totals.endings.stalemates_unattributed)} stalemates have no reliable final move for attribution.
        </small>}
        <div className="score-mate-table" role="table" aria-label="Mate counts">
          <div {...pageStyleTarget('player.scores.mates.row')} className="score-mate-row" role="row">
            <span {...pageStyleTarget('player.scores.mates.label')} role="columnheader">Distance</span>
            <span {...pageStyleTarget('player.scores.mates.label')} role="columnheader">For {playerName}</span>
            <span {...pageStyleTarget('player.scores.mates.label')} role="columnheader">Against {playerName}</span>
          </div>
          {totals.mateDistances.map(row => <div {...pageStyleTarget('player.scores.mates.row')} className="score-mate-row" role="row" key={row.moves ?? 'unknown'}>
            <span {...pageStyleTarget('player.scores.mates.label')} role="rowheader">{row.moves === null ? 'Distance unavailable' : row.moves === 0 ? 'Checkmate' : `Mate in ${row.moves}`}</span>
            <strong {...pageStyleTarget('player.scores.mates.for')} role="cell">{formatNumber(row.for)}</strong>
            <strong {...pageStyleTarget('player.scores.mates.against')} role="cell">{formatNumber(row.against)}</strong>
          </div>)}
        </div>
        {!totals.mateDistances.length && <p {...pageStyleTarget('player.scores.status')}>No mate evaluations in the selected modes.</p>}
      </div>
      <small {...pageStyleTarget('player.scores.status')}>Position occurrences · {formatNumber(totals.unscored)} unscored · {formatNumber(totals.tablebases)} tablebase</small>
    </section>, document.body)}
  </>
}
