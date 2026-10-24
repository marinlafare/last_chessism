import { pageStyleTarget } from '../../appearance/styleCore.js'

export default function PlayerProfileField({ field, label, children, title, date = false }) {
  const target = `player.metadata.${field}`
  return <div {...pageStyleTarget(target)} className={date ? 'players-profile-date' : 'profile-item'}>
    <span {...pageStyleTarget(`${target}.label`)} className="profile-item-label">{label}</span>
    <strong {...pageStyleTarget(`${target}.value`)} className="profile-item-value" title={title}>{children}</strong>
  </div>
}
