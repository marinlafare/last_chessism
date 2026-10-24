import { useEffect, useId, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import PlayerAvatar from '../../components/players/PlayerAvatar'
import { playerProfileSections } from './playerProfileDetails.js'
import './player-profile-photo.css'

function ProfilePopup({ profile, latestGame, positionStats, hours, onClose }) {
  const titleId = useId(), panel = useRef(null), close = useRef(onClose)
  close.current = onClose
  useEffect(() => {
    const previous = document.activeElement, overflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    panel.current?.focus({ preventScroll: true })
    const keydown = event => {
      // The appearance editor is a separate portal and must remain usable.
      if (event.target.closest?.('[data-appearance-tools]')) return
      if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); close.current(); return }
      if (event.key !== 'Tab' || !panel.current) return
      const nodes = [...panel.current.querySelectorAll('button:not(:disabled), a[href]')]
      const first = nodes[0], last = nodes.at(-1), active = document.activeElement
      if (event.shiftKey && (active === first || active === panel.current || !panel.current.contains(active))) {
        event.preventDefault(); last?.focus()
      } else if (!event.shiftKey && (active === last || !panel.current.contains(active))) {
        event.preventDefault(); first?.focus()
      }
    }
    document.addEventListener('keydown', keydown)
    return () => {
      document.body.style.overflow = overflow
      document.removeEventListener('keydown', keydown)
      if (previous?.isConnected) previous.focus({ preventScroll: true })
    }
  }, [])

  return createPortal(<div {...pageStyleTarget('player.profilePopup.backdrop')} className="player-profile-popup-backdrop"
    onClick={event => { if (event.target === event.currentTarget) onClose() }}>
    <section {...pageStyleTarget('player.profilePopup')} ref={panel} className="player-profile-popup"
      role="dialog" aria-modal="true" aria-labelledby={titleId} tabIndex={-1}>
      <header className="player-profile-popup-header">
        <h2 {...pageStyleTarget('player.profilePopup.title')} id={titleId}>{profile.player_name} · Player profile</h2>
        <button {...pageStyleTarget('player.profilePopup.close')} type="button" onClick={onClose} aria-label="Close player profile">×</button>
      </header>
      <PlayerAvatar {...pageStyleTarget('player.profilePopup.photo')} className="player-profile-popup-photo" src={profile.avatar} name={profile.player_name} />
      {playerProfileSections(profile, { latestGame, positionStats, hours }).map(section => <section key={section.title}>
        <h3 {...pageStyleTarget('player.profilePopup.sectionTitle')}>{section.title}</h3>
        <dl className="player-profile-popup-fields">
          {section.rows.map(row => <div {...pageStyleTarget('player.profilePopup.field')} key={row.key} data-profile-field={row.key}>
            <dt {...pageStyleTarget('player.profilePopup.label')}>{row.label}</dt>
            <dd {...pageStyleTarget('player.profilePopup.value')}>{row.href
              ? <a href={row.href} target="_blank" rel="noopener noreferrer">{row.value}</a> : row.value}</dd>
          </div>)}
        </dl>
      </section>)}
    </section>
  </div>, document.body)
}

export default function PlayerProfilePhoto({ profile, name, latestGame, positionStats, hours }) {
  const [open, setOpen] = useState(false)
  return <>
    <button {...pageStyleTarget('player.photo')} className="players-profile-avatar-box" type="button"
      disabled={!profile} aria-label={`Open full profile for ${profile?.player_name || name || 'player'}`}
      aria-haspopup="dialog" onClick={() => setOpen(true)}>
      <PlayerAvatar className="players-profile-avatar" src={profile?.avatar} name={profile?.player_name || name} />
    </button>
    {open && profile ? <ProfilePopup key={profile.player_name} profile={profile} latestGame={latestGame}
      positionStats={positionStats} hours={hours} onClose={() => setOpen(false)} /> : null}
  </>
}
