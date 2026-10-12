import test from 'node:test'
import assert from 'node:assert/strict'
import { rotateCircle, wheelRotation, circlePercentage } from '../src/utils/circleRotation.js'

test('rotation wraps in both directions and scrolling normalizes device units', () => {
  assert.equal(rotateCircle(0, -18), 342)
  assert.equal(rotateCircle(342, 18), 0)
  assert.equal(rotateCircle(350, 25), 15)
  assert.equal(rotateCircle(45, 720), 45)
  assert.equal(wheelRotation(120), 18)
  assert.equal(wheelRotation(-120), -18)
  assert.equal(wheelRotation(3, 1), 7.199999999999999)
  assert.equal(wheelRotation(1, 2, 180), 27)
  assert.equal(wheelRotation(100000), 90)
  for (const value of [NaN, Infinity, -Infinity]) {
    assert.equal(rotateCircle(30, value), 30)
    assert.equal(wheelRotation(value), 0)
  }
})

test('percentages use each slice fraction, and do not round tiny positive slices to zero', () => {
  assert.equal(circlePercentage(1), '100%')
  assert.equal(circlePercentage(0), '0%')
  assert.equal(circlePercentage(.96782), '96.78%')
  assert.equal(circlePercentage(.0002085), '0.02%')
  assert.equal(circlePercentage(.000001), '<0.01%')
})
