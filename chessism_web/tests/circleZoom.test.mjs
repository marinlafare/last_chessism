import test from 'node:test'
import assert from 'node:assert/strict'
import { INITIAL_CIRCLE_VIEW, zoomCircle } from '../src/pages/scored-positions/circleZoom.js'

test('circle zoom centers on the pointer and keeps that point fixed', () => {
  const view = zoomCircle(INITIAL_CIRCLE_VIEW, 2, .8, .2)
  assert.deepEqual(view, { x: 80, y: 20, size: 100 })
  assert.equal(view.x + .8 * view.size, .8 * 200)
  assert.equal(view.y + .2 * view.size, .2 * 200)
  assert.deepEqual(zoomCircle(view, .5, .8, .2), INITIAL_CIRCLE_VIEW)
})

test('zoom is bounded between 1x and 4x; extreme scrolling cannot lose the circle', () => {
  const close = zoomCircle(INITIAL_CIRCLE_VIEW, 1000, 2, -2)
  assert.deepEqual(close, { x: 150, y: 0, size: 50 })
  assert.equal(zoomCircle(close, 2), close)
  assert.deepEqual(zoomCircle(close, .001), INITIAL_CIRCLE_VIEW)
  assert.equal(zoomCircle(INITIAL_CIRCLE_VIEW, .5), INITIAL_CIRCLE_VIEW)
  for (const factor of [NaN, Infinity, 0, -1]) assert.equal(zoomCircle(INITIAL_CIRCLE_VIEW, factor), INITIAL_CIRCLE_VIEW)
  assert.deepEqual(zoomCircle(INITIAL_CIRCLE_VIEW, 2, NaN, Infinity), { x: 50, y: 50, size: 100 })
})
