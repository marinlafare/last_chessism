import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { appearanceCss, seedIndependentStyles } from '../src/appearance/styleCore.js'

const read = name => JSON.parse(readFileSync(new URL(`../../chessism_api/${name}.json`, import.meta.url)))
const base = read('appearance_registry'), scopes = read('appearance_scopes'), charts = read('appearance_charts')
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: Object.fromEntries(
  Object.entries(scopes.scopes).map(([scope, spec]) => [scope, { ...spec, targets: { ...scopes.common_targets, ...spec.targets } }])) }
const targets = scopes.scopes.research.targets

test('every Research subcontainer color has an independent hover counterpart', () => {
  assert.equal(Object.keys(targets).length, 25)
  for (const [target, spec] of Object.entries(targets)) {
    for (const [normal, hover] of [['background', 'hoverBackground'], ['color', 'hoverColor'], ['borderColor', 'hoverBorderColor']]) {
      if (!spec.properties.includes(normal)) continue
      assert.ok(spec.properties.includes(hover), `${target}/${hover}`)
      const row = { scope: 'research', target, property: hover, value: '#12345680' }
      const css = appearanceCss(registry, [row])
      assert.ok(css.includes(`[data-style-scope="research"][data-style-id="${target}"]`))
      assert.ok(css.includes(`:hover{${registry.properties[hover].css}:#12345680;`))
      for (const scope of ['home', 'navigation', 'matrices', 'algorithms']) {
        assert.equal(appearanceCss(registry, [{ ...row, scope }]), '')
      }
    }
  }
})

test('Research hover additions do not replace normal colors or require preset migration', () => {
  assert.equal(appearanceCss(registry, []), '')
  const normal = { scope: 'research', target: 'area.matrices.panel', property: 'background', value: '#112233' }
  const hover = { ...normal, property: 'hoverBackground', value: '#abcdef' }
  assert.deepEqual(seedIndependentStyles(registry, [normal, hover]), [normal, hover])
  const css = appearanceCss(registry, [normal, hover])
  assert.ok(css.includes('{background-color:#112233;'))
  assert.ok(css.includes(':hover{background-color:#abcdef;'))
  assert.ok(!css.includes('area.algorithms.panel'))
  assert.equal(appearanceCss(registry, [{ ...hover, value: 'red;display:none' }]), '')
})
