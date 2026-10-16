import test from 'node:test'
import assert from 'node:assert/strict'
import { commonnessScale } from '../src/pages/scored-positions/commonness.js'

test('commonness has evenly spaced whole-number ticks from zero, with no log transform', () => {
  for (const maximum of [1, 2, 3, 10, 376228, 9990001]) {
    const scale = commonnessScale([{ appearances: maximum }])
    assert.equal(scale.ticks[0], 0)
    assert.ok(scale.upper >= maximum)
    assert.ok(scale.ticks.length >= 2 && scale.ticks.length <= 6)
    assert.ok(scale.ticks.every(Number.isSafeInteger))
    assert.ok(scale.ticks.every((tick, i) => tick === i * scale.ticks[1]))
    assert.equal(scale.ticks.at(-1), scale.upper)
  }
  assert.deepEqual(commonnessScale([{ appearances: 376228 }]), { upper: 400000, ticks: [0, 100000, 200000, 300000, 400000] })
})
