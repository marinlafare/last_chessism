import test from 'node:test'
import assert from 'node:assert/strict'
import { normalizePath, normalizePlayer, playerHomeHref, playerFromLocation, isPlainClick } from '../src/services/navigation.js'

test('legacy player routes resolve to home without changing unrelated routes', () => {
  for (const path of ['/', '', '/players', '/players/']) assert.equal(normalizePath(path), '/')
  assert.equal(normalizePath('/system_status/'), '/system_status')
  assert.equal(normalizePath('/games/123/'), '/games/123')
  assert.equal(normalizePath('/analize_positions'), '/analize_positions')
})
test('player links and selections round-trip without losing names or changing routes', () => {
  assert.equal(playerHomeHref(' HiKaru '), '/?player=hikaru')
  assert.equal(playerFromLocation('/?player=HiKaru'), 'hikaru')
  assert.equal(playerFromLocation('/players?player=big_tonka_t'), 'big_tonka_t')
  assert.equal(playerHomeHref(null), '/')
  assert.equal(playerFromLocation('/'), '')
  assert.equal(normalizePlayer(undefined), '')
  const name = 'name&player=other'
  assert.equal(playerFromLocation(playerHomeHref(name)), name)
})
test('inline selection preserves modified clicks and normal link behavior', () => {
  assert.equal(isPlainClick({ button: 0 }), true)
  for (const modifier of ['defaultPrevented', 'metaKey', 'ctrlKey', 'shiftKey', 'altKey']) {
    assert.equal(isPlainClick({ button: 0, [modifier]: true }), false)
  }
  assert.equal(isPlainClick({ button: 1 }), false)
})
