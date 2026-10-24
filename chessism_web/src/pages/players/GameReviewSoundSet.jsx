import { useEffect, useId, useLayoutEffect, useRef, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { BOARD_SOUND_SETS, getBoardSoundSet } from '../../components/chess/boardSoundSets'
import './game-review-sound-set.css'

export default function GameReviewSoundSet({ value, onChange }) {
  const [open, setOpen] = useState(false)
  const [highlighted, setHighlighted] = useState(0)
  const root = useRef(null), trigger = useRef(null), list = useRef(null)
  const search = useRef({ text: '', time: 0 })
  const id = useId(), selected = getBoardSoundSet(value)
  const selectedIndex = BOARD_SOUND_SETS.findIndex(set => set.id === selected.id)
  const optionId = index => `${id}-option-${index}`
  const show = () => { setHighlighted(selectedIndex); setOpen(true); search.current.text = '' }
  const choose = index => {
    onChange(BOARD_SOUND_SETS[index].id)
    setOpen(false)
    trigger.current.focus({ preventScroll: true })
  }
  useEffect(() => {
    if (!open) return undefined
    const outside = event => {
      // Keep the menu available as a live preview while editing its styles.
      if (!root.current.contains(event.target) && !event.target.closest?.('[data-appearance-tools]')) setOpen(false)
    }
    document.addEventListener('pointerdown', outside)
    document.addEventListener('focusin', outside)
    return () => {
      document.removeEventListener('pointerdown', outside)
      document.removeEventListener('focusin', outside)
    }
  }, [open])
  useLayoutEffect(() => {
    if (!open) return
    const menu = list.current, option = menu.children[highlighted]
    if (!option) return
    const top = option.offsetTop, bottom = top + option.offsetHeight
    if (top < menu.scrollTop) menu.scrollTop = top
    else if (bottom > menu.scrollTop + menu.clientHeight) menu.scrollTop = bottom - menu.clientHeight
  }, [open, highlighted])
  const keyDown = event => {
    if (event.key === 'Escape') {
      if (open) { event.preventDefault(); event.stopPropagation(); setOpen(false) }
      return
    }
    if (event.key === 'Tab') { setOpen(false); return }
    event.stopPropagation() // Arrow keys here must not navigate the chess game.
    if (event.ctrlKey || event.metaKey || event.altKey) return
    if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(event.key)) {
      event.preventDefault()
      const index = event.key === 'Home' ? 0 : event.key === 'End' ? BOARD_SOUND_SETS.length - 1
        : open ? Math.max(0, Math.min(BOARD_SOUND_SETS.length - 1, highlighted + (event.key === 'ArrowDown' ? 1 : -1))) : selectedIndex
      setHighlighted(index); setOpen(true)
    } else if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault()
      if (open) choose(highlighted)
      else show()
    } else if (event.key.length === 1) {
      event.preventDefault()
      const now = performance.now(), letter = event.key.toLowerCase()
      const text = now - search.current.time < 700 ? search.current.text + letter : letter
      search.current = { text, time: now }
      const index = BOARD_SOUND_SETS.findIndex(set => set.name.toLowerCase().startsWith(text))
      if (index >= 0) { setHighlighted(index); setOpen(true) }
    }
  }
  return <span {...pageStyleTarget('gameReview.soundSet')} className="game-review-sound-set" ref={root}
    data-open={open} data-selected-label={selected.name}>
    <button ref={trigger} type="button" role="combobox" aria-label="Board sound set" title="Board sound set"
      aria-describedby={`${id}-value`} aria-haspopup="listbox" aria-expanded={open} aria-controls={`${id}-list`}
      aria-activedescendant={open ? optionId(highlighted) : undefined}
      onClick={() => open ? setOpen(false) : show()} onKeyDown={keyDown}>
      <span id={`${id}-value`}>{selected.name}</span>
      <svg viewBox="0 0 16 16" aria-hidden="true"><path d="m4 6 4 4 4-4" /></svg>
    </button>
    {/* Keep style targets mounted when closed so right-clicking the trigger
        can always edit the list and options through editorRelated. */}
    <span {...pageStyleTarget('gameReview.soundSet.list')} className="game-review-sound-list" ref={list}
      id={`${id}-list`} role="listbox" aria-label="Sound sets" hidden={!open}>
      {BOARD_SOUND_SETS.map((set, index) => <span {...pageStyleTarget('gameReview.soundSet.option')}
        className="game-review-sound-option" key={set.id} id={optionId(index)} role="option"
        aria-selected={set.id === selected.id} data-highlighted={index === highlighted} data-sound-set={set.id}
        onPointerMove={() => setHighlighted(index)} onPointerDown={event => { if (event.button === 0) event.preventDefault() }}
        onClick={() => choose(index)}>{set.name}</span>)}
    </span>
  </span>
}
