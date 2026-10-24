import { formatNumber } from '../../utils/formatters.js'

const text = value => value === null || value === undefined || value === '' ? 'Not available' : String(value)
const yesNo = value => value === true ? 'Yes' : value === false ? 'No' : 'Not available'
const count = value => value === null || value === undefined || value === '' || !Number.isFinite(Number(value))
  ? 'Not available' : formatNumber(value)

export function profileDate(value, epochSeconds = false) {
  if (!value || (epochSeconds && Number(value) <= 0)) return 'Not available'
  const date = new Date(epochSeconds ? Number(value) * 1000 : value)
  return Number.isFinite(date.getTime()) ? `${date.toLocaleString('en-US', {
    year: 'numeric', month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit', timeZone: 'UTC',
  })} UTC` : 'Not available'
}

export function profileLink(value) {
  try {
    const url = new URL(value)
    return ['https:', 'http:'].includes(url.protocol) ? url.href : null
  } catch { return null }
}

// Use the data already loaded by the hero. Opening a profile must not invoke
// the stats endpoint, which refreshes Chess.com data and writes to the DB.
export function playerProfileSections(profile, { latestGame, positionStats, hours } = {}) {
  const row = (key, label, value, href) => ({ key, label, value, href })
  return [
    { title: 'Profile', rows: [
      row('player', 'Player', text(profile.player_name)),
      row('name', 'Name', text(profile.name)),
      row('title', 'Chess title', text(profile.title)),
      row('country', 'Country', text(profile.country)),
      row('location', 'Location', text(profile.location)),
      row('timezone', 'Time zone', text(profile.timezone)),
      row('timezone-source', 'Time zone source', text(profile.timezone_source)),
      row('joined', 'Joined', profileDate(profile.joined, true)),
      row('last-play', 'Last stored game', latestGame?.name === profile.player_name
        ? latestGame.error ? 'Unavailable' : profileDate(latestGame.playedAt) : 'Not available'),
      row('followers', 'Followers', count(profile.followers)),
      row('status', 'Account status', text(profile.status)),
      row('verified', 'Verified', yesNo(profile.verified)),
      row('streamer', 'Streamer', yesNo(profile.is_streamer)),
      row('league', 'League', text(profile.league)),
      row('url', 'Player profile', text(profile.url), profileLink(profile.url)),
      row('twitch', 'Twitch', text(profile.twitch_url), profileLink(profile.twitch_url)),
      ...(profile.deleted_at ? [row('deleted', 'Deleted from Chessism', profileDate(profile.deleted_at))] : []),
    ] },
    { title: 'Stored games and positions', rows: [
      row('games', 'Games', count(positionStats?.total_games)),
      row('scored-games', 'Scored games', count(positionStats?.analyzed_games)),
      row('positions', 'Positions', count(positionStats?.total_fens ?? positionStats?.total_positions)),
      row('scored-positions', 'Scored positions', count(positionStats?.analyzed_fens ?? positionStats?.analyzed_positions)),
      ...['bullet', 'blitz', 'rapid'].flatMap(mode => {
        const label = mode[0].toUpperCase() + mode.slice(1)
        return [row(`${mode}-games`, `${label} games`, count(hours?.[`${mode}_games`])),
          row(`${mode}-hours`, `${label} hours`, count(hours?.[`${mode}_hours`]))]
      }),
    ] },
  ]
}
