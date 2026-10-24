import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { appearanceCss } from '../src/appearance/styleCore.js'
import { editorTargets } from '../src/appearance/editorTargets.js'

const json = name => JSON.parse(readFileSync(new URL(`../../chessism_api/${name}`, import.meta.url)))
const base = json('appearance_registry.json'), scopes = json('appearance_scopes.json'), charts = json('appearance_charts.json')
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: scopes.scopes }
const targets = registry.scopes.home.targets
const row = (target, property, value) => ({ scope: 'home', target, property, value })

test('all rating chart elements have registered, page-local style properties', () => {
  const ids = ['blitz', 'bullet', 'rapid', 'previous', 'next', 'legend', 'legend.blitz', 'legend.bullet', 'legend.rapid',
    'yTitle', 'yTicks', 'grid', 'dividers', 'xTicks', 'missing', 'photos', 'tooltip', 'empty', 'status', 'retry']
  for (const id of ['', ...ids.map(id => `.${id}`)]) {
    const target = targets[`characters.chart${id}`]
    assert.ok(target, id)
    assert.ok(target.properties.every(key => registry.properties[key]), id)
  }
  for (const mode of ['blitz', 'bullet', 'rapid']) {
    const id = `characters.chart.${mode}`, spec = targets[id]
    for (const property of ['fill', 'barOpacity', 'stroke', 'strokeWidth', 'barHoverColor', 'barHoverBorder']) assert.ok(spec.properties.includes(property))
    const css = appearanceCss(registry, [row(id, 'fill', '#123456'), row(id, 'barHoverColor', '#abcdef')])
    assert.ok(css.includes('fill:#123456'))
    assert.ok(css.includes(`${id}`))
    assert.ok(css.includes(':hover'))
    for (const other of ['blitz', 'bullet', 'rapid'].filter(other => other !== mode)) assert.ok(!css.includes(`characters.chart.${other}`))
    assert.ok(!css.includes('data-style-scope="players"'))
  }
  for (const property of ['background', 'hoverBackground', 'padding', 'radius']) assert.ok(targets['characters.chart'].properties.includes(property))
})

test('a hidden tooltip remains editable via its chart but never another page', () => {
  const tooltip = { dataset: { styleScope: 'home', styleId: 'characters.chart.tooltip' } }
  const selection = { scope: 'home', id: 'characters.chart', element: {
    querySelector: selector => selector === '[data-style-id="characters.chart.tooltip"]' ? tooltip : null,
  } }
  assert.deepEqual(editorTargets(registry, selection).map(item => item.id), ['characters.chart', 'characters.chart.tooltip'])
  tooltip.dataset.styleScope = 'players'
  assert.deepEqual(editorTargets(registry, selection).map(item => item.id), ['characters.chart'])
})

test('photo radius and opacity are bounded and independent of other charts', () => {
  const id = 'characters.chart.photos'
  const css = appearanceCss(registry, [row(id, 'photoCornerRadius', 12), row(id, 'imageOpacity', .5)])
  assert.ok(css.includes('--player-photo-radius:12px'))
  assert.ok(css.includes('opacity:0.5'))
  assert.equal(appearanceCss(registry, [row(id, 'photoCornerRadius', -1), row(id, 'imageOpacity', 2)]), '')
})

test('bar opacity is independent per mode, bounded to 0–1 and cannot dim the whole chart', () => {
  assert.equal(registry.properties.barOpacity.default, 1)
  for (const mode of ['blitz', 'bullet', 'rapid']) {
    const id = `characters.chart.${mode}`
    for (const value of [0, .35, 1]) {
      const css = appearanceCss(registry, [row(id, 'barOpacity', value)])
      assert.ok(css.includes(`opacity:${value}`))
      assert.ok(css.includes(`data-style-id="${id}"`))
      assert.ok(!css.includes('data-style-id="characters.chart"'))
      for (const other of ['blitz', 'bullet', 'rapid'].filter(other => other !== mode)) assert.ok(!css.includes(`characters.chart.${other}`))
    }
    for (const value of [-.01, 1.01, NaN, Infinity, '0.5']) assert.equal(appearanceCss(registry, [row(id, 'barOpacity', value)]), '')
  }
  for (const id of ['characters.chart', 'characters.chart.photos', 'characters.chart.yTicks']) {
    assert.equal(appearanceCss(registry, [row(id, 'barOpacity', .5)]), '')
  }
})
