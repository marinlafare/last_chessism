import assert from 'node:assert/strict'
import { balanceScale, balanceOf, balanceDescription, volumeOpacityScale } from '../src/pages/players/activityBalance.js'

const rows = [
  { games: 10, wins: 5, losses: 5 }, { games: 10, wins: 6, losses: 5 },
  { games: 10, wins: 8, losses: 2 }, { games: 10, wins: 2, losses: 3 },
  { games: 10, wins: 1, losses: 9 }, { games: 0 },
]
const scale = balanceScale(rows)
assert.equal(scale(rows[0]).kind, 'balanced')
assert.equal(scale(rows[1]).intensity, 0.25)
assert.equal(scale(rows[2]).intensity, 1)
assert.equal(scale(rows[3]).intensity, 0.25)
assert.equal(scale(rows[4]).intensity, 1)
assert.equal(scale(rows[5]).kind, 'empty')
assert.equal(balanceOf({ games: 10, wins: 2, losses: 2, draws: 6 }), 0)
assert.equal(balanceScale([{ games: 1, wins: 1 }])({ games: 1, wins: 1 }).intensity, 0.25)
assert.equal(balanceScale([])({ games: 0 }).intensity, 0)
assert.equal(balanceDescription(rows[2]), 'Win–loss balance: +60.0%')
assert.equal(balanceDescription(rows[5]), 'No games')
const volume = volumeOpacityScale([{ games: 100 }, { games: 50 }, { games: 1 }, { games: 0 }])
assert.equal(volume({ games: 100 }), 1)
assert.equal(volume({ games: 50 }), 0.5)
assert.equal(volume({ games: 1 }), 0.01)
assert.equal(volume({ games: 0 }), 0)
assert.equal(volume({}), 0)
assert.equal(volumeOpacityScale([])({}), 0)
assert.equal(volumeOpacityScale([{ games: 0 }])({ games: 0 }), 0)
assert.equal(volumeOpacityScale([{ games: 25 }, { games: 25 }])({ games: 25 }), 1)
assert.equal(volumeOpacityScale([{ games: 50 }])({ games: 25 }), 0.5)
assert.equal(volume({ games: 50, wins: 50 }), volume({ games: 50, losses: 50 }))
console.log('PASS balance colors and independent proportional volume opacity, transparent empty bins, and per-day scaling')
