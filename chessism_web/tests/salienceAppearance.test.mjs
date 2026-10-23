import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { appearanceCss, registryTarget, seedIndependentStyles } from '../src/appearance/styleCore.js'

const read = name => JSON.parse(readFileSync(new URL(`../../chessism_api/${name}.json`, import.meta.url)))
const base = read('appearance_registry'), scopes = read('appearance_scopes'), charts = read('appearance_charts')
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: Object.fromEntries(
  Object.entries(scopes.scopes).map(([scope, spec]) => [scope, { ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets } }])) }
const prefix = 'player.accuracy.salience.'
const targets = Object.entries(scopes.scopes.home.targets).filter(([id]) => id.startsWith(prefix))

test('salience exposes independent container, label, range, tick and handle styles', () => {
  assert.equal(targets.length, 10)
  for (const [target, spec] of targets) {
    for (const property of registryTarget(registry, 'home', target).properties) {
      const field = registry.properties[property]
      const value = field.kind === 'color' ? '#123456' : field.kind === 'number' ? field.min : field.options[0]
      const row = { scope: 'home', target, property, value }
      assert.ok(appearanceCss(registry, [row]).includes(`data-style-id="${target}"`), `${target}/${property}`)
      assert.equal(appearanceCss(registry, [{ ...row, scope: 'positions' }]), '')
    }
    for (const [normal, hover] of [['background', 'hoverBackground'], ['borderColor', 'hoverBorderColor'], ['color', 'hoverColor']]) {
      if (spec.properties.includes(normal)) assert.ok(spec.properties.includes(hover), `${target}/${hover}`)
    }
  }
})

test('the salience container exposes all parts and preserves previous shared title/handle settings once', () => {
  const panel = registryTarget(registry, 'home', prefix+'panel')
  assert.deepEqual(new Set(panel.editorRelated), new Set(targets.map(([id])=>id).filter(id=>id!==prefix+'panel')))
  const value = { scope: 'home', target: 'value', property: 'color', value: '#123456' }
  const button = { scope: 'home', target: 'button', property: 'background', value: '#654321' }
  const seeded = seedIndependentStyles(registry, [value, button])
  assert.equal(seeded.find(row=>row.target===prefix+'title').value, value.value)
  for (const part of ['minimum', 'maximum']) assert.equal(seeded.find(row=>row.target===prefix+part+'.handle').value, button.value)
  assert.deepEqual(seedIndependentStyles(registry, seeded), seeded)
  assert.equal(appearanceCss(registry, []), '')
})
