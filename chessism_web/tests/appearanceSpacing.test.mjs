import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { appearanceCss, registryTarget } from '../src/appearance/styleCore.js'
import { changeStyle, MARGINS, numericStyleValue } from '../src/appearance/spacing.js'

const read = name => JSON.parse(readFileSync(new URL(`../../chessism_api/${name}.json`, import.meta.url)))
const base = read('appearance_registry'), scopes = read('appearance_scopes'), charts = read('appearance_charts')
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: Object.fromEntries(
  Object.entries(scopes.scopes).map(([scope, spec]) => [scope, { ...spec, targets: { ...scopes.common_targets, ...charts.targets, ...spec.targets } }])) }
const row = (property, value, target = 'intro.panel') => ({ scope: 'research', target, property, value })

test('every registered element exposes bounded spacing with stable per-page selectors', () => {
  for (const [scope, spec] of Object.entries(registry.scopes)) {
    for (const target of Object.keys(spec.targets)) {
      for (const property of MARGINS) {
        assert.ok(registryTarget(registry, scope, target).properties.includes(property))
        assert.ok(appearanceCss(registry, [{ scope, target, property, value: 12 }]).includes(`${registry.properties[property].css}:12px;`))
      }
    }
  }
  assert.equal(appearanceCss(registry, [row('margin', '12px;display:none')]), '')
  assert.equal(appearanceCss(registry, [row('margin', Infinity)]), '')
  assert.equal(appearanceCss(registry, [row('margin', 257)]), '')
  assert.equal(appearanceCss(registry, [row('margin', 12, 'unknown')]), '')
  assert.equal(appearanceCss(registry, []), '')
})

test('all sides resets per-side drafts; individual sides override regardless of save order', () => {
  const untouched = row('marginTop', 40, 'area.matrices.panel')
  let rows = [row('marginTop', 25), untouched]
  rows = changeStyle(rows, 'research', 'intro.panel', 'margin', 8)
  assert.deepEqual(rows, [untouched, row('margin', 8)])
  rows = changeStyle(rows, 'research', 'intro.panel', 'marginLeft', 20)
  const css = appearanceCss(registry, rows)
  assert.ok(css.indexOf('margin:8px') < css.indexOf('margin-left:20px'))
  const reversed = appearanceCss(registry, [...rows].reverse())
  assert.ok(reversed.indexOf('margin:8px') < reversed.indexOf('margin-left:20px'))
  assert.equal(numericStyleValue(registry.properties.margin, '5px 10px'), '')
  assert.equal(numericStyleValue(registry.properties.marginLeft, 'auto'), '')
  assert.equal(numericStyleValue(registry.properties.margin, '12px'), 12)
})

test('Research width and VM/Local borders remain independently scoped', () => {
  assert.ok(appearanceCss(registry, [row('researchIntroWidth', 60)]).includes('--research-intro-width:60%'))
  assert.equal(appearanceCss(registry, [row('researchIntroWidth', 60, 'areas.panel')]), '')
  for (const target of ['cloud.job.vms', 'cloud.vm.toggle', 'cloud.vm.details', 'local.toggle', 'local.heading']) {
    for (const [property, value] of [['borderColor', '#123456'], ['hoverBorderColor', '#abcdef'], ['borderWidth', 2], ['radius', 12], ['hoverBackground', '#111111']]) {
      const setting = { scope: 'positions', target, property, value }
      assert.ok(appearanceCss(registry, [setting]), `${target}/${property}`)
      assert.equal(appearanceCss(registry, [{ ...setting, scope: 'research' }]), '')
    }
  }
})
