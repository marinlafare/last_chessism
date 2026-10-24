import test from 'node:test'
import assert from 'node:assert/strict'
import { playerProfileSections, profileDate, profileLink } from '../src/pages/players/playerProfileDetails.js'

test('profile details use existing data, preserve zero/false, and distinguish missing values', () => {
  const profile = { player_name: 'example', followers: 0, verified: false, is_streamer: true, joined: 1609459200,
    country: 'MX', location: 'Mexico City', title: 'GM', timezone: 'America/Mexico_City', timezone_source: 'manual',
    status: 'premium', league: 'Legend', url: 'https://www.chess.com/member/example' }
  const rows = Object.fromEntries(playerProfileSections(profile, {
    latestGame: { name: 'example', playedAt: '2026-10-23T08:00:00Z' },
    positionStats: { total_games: 1234, analyzed_games: 0, total_positions: 5500, analyzed_positions: 50 },
    hours: { bullet_games: 0, bullet_hours: 2.5 },
  }).flatMap(section => section.rows).map(row => [row.key, row]))
  assert.equal(rows.followers.value, '0')
  assert.equal(rows.verified.value, 'No')
  assert.equal(rows.streamer.value, 'Yes')
  assert.equal(rows.games.value, '1,234')
  assert.equal(rows['scored-games'].value, '0')
  assert.equal(rows.positions.value, '5,500')
  assert.equal(rows['bullet-hours'].value, '2.5')
  assert.equal(rows['rapid-games'].value, 'Not available')
  assert.equal(rows.name.value, 'Not available')
  assert.match(rows['last-play'].value, /Oct 23, 2026.*08:00 AM UTC/)
  assert.match(rows.joined.value, /Jan 01, 2021/)
  assert.equal(rows.url.href, profile.url)
  assert.equal(rows['timezone-source'].value, 'manual')
  assert.equal(rows.deleted, undefined)
})

test('profile links/dates and missing or stale game data are safe', () => {
  for (const value of ['javascript:alert(1)', 'data:text/html,test', '/relative', '', null]) assert.equal(profileLink(value), null)
  assert.equal(profileLink('https://example.com'), 'https://example.com/')
  for (const value of [null, '', 'bad date']) assert.equal(profileDate(value), 'Not available')
  assert.equal(profileDate(0, true), 'Not available')
  const profile = { player_name: 'new', deleted_at: '2026-10-23T01:00:00Z' }
  const rows = playerProfileSections(profile, { latestGame: { name: 'old', playedAt: '2025-01-01' } })[0].rows
  assert.equal(rows.find(row => row.key === 'last-play').value, 'Not available')
  assert.match(rows.find(row => row.key === 'deleted').value, /UTC$/)
})
