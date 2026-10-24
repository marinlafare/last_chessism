import { useEffect, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { fetchPlayerLatestGame } from './playerApi'
import PlayerProfileField from './PlayerProfileField'

function dateLabel(value) {
  const date = new Date(value)
  return Number.isFinite(date.getTime())
    ? date.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: '2-digit', timeZone: 'UTC' })
      .replace(',', '').toUpperCase()
    : 'N/A'
}

export default function PlayerProfileDates({ profile, onLatestChange }) {
  const [latest, setLatest] = useState(null)
  const name = profile.player_name
  useEffect(() => {
    const controller = new AbortController()
    let timer
    setLatest(null)
    if (!name || profile.deleted_at) return () => controller.abort()
    // Fetch only the most recent stored game; Chess.com's last-online date
    // does not tell us when this player's latest game in our DB was played.
    const load = () => fetchPlayerLatestGame(name, { signal: controller.signal }).then(payload => {
      if (controller.signal.aborted) return
      const playedAt = payload.played_at
      const day = typeof playedAt === 'string' ? playedAt.match(/^\d{4}-\d{2}-\d{2}/)?.[0] : null
      setLatest({ name, playedAt, label: day ? dateLabel(`${day}T00:00:00Z`) : 'N/A' })
      if (payload.summary_stale) timer = window.setTimeout(load, 2000)
    }).catch(error => {
      if (!controller.signal.aborted) setLatest({ name, label: 'Unavailable', error: error.message })
    })
    load()
    return () => { controller.abort(); window.clearTimeout(timer) }
  }, [profile, name])

  useEffect(() => { onLatestChange?.(latest?.name === name ? latest : null) }, [latest, name, onLatestChange])

  const joined = Number(profile.joined) > 0 ? dateLabel(Number(profile.joined) * 1000) : 'N/A'
  const lastPlay = profile.deleted_at ? 'N/A' : latest?.name === name ? latest.label : '…'
  return (
    <div {...pageStyleTarget('player.metadata.dates')} className="profile-item players-profile-dates" aria-label="Joined and last played dates">
      <PlayerProfileField field="joined" label="Joined" title={joined} date>{joined}</PlayerProfileField>
      <PlayerProfileField field="last-play" label="Last play" date title={latest?.name === name && latest.error
        ? `Last stored game unavailable: ${latest.error}` : `Latest game in the database: ${lastPlay}`}>{lastPlay}</PlayerProfileField>
    </div>
  )
}
