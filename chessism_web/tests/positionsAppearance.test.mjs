import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import { appearanceCss, registryTarget, seedIndependentStyles } from '../src/appearance/styleCore.js'

const read = name => JSON.parse(fs.readFileSync(new URL(`../../chessism_api/${name}.json`, import.meta.url)))
const base = read('appearance_registry'), scopes = read('appearance_scopes'), charts = read('appearance_charts')
const registry = { ...base, properties: { ...base.properties, ...charts.properties }, scopes: {
  positions: { ...scopes.scopes.positions, targets: { ...scopes.common_targets, ...charts.targets, ...scopes.scopes.positions.targets } },
} }

test('every disclosure has an independent heading with accessible corner/border controls', () => {
  const prefixes = ['local', 'cloud', 'cloud.run', 'cloud.batch', 'cloud.run.single', 'cloud.run.combined', 'cloud.single', 'cloud.combined']
  for (const prefix of prefixes) {
    const toggle = registryTarget(registry, 'positions', `${prefix}.toggle`)
    const heading = registryTarget(registry, 'positions', `${prefix}.heading`)
    assert.equal(toggle.editorParent, `${prefix}.heading`)
    assert.equal(heading.initialStyleFrom, 'title')
    for (const property of ['radius', 'background', 'hoverBackground', 'borderColor', 'hoverBorderColor', 'borderWidth']) {
      assert.ok(heading.properties.includes(property), `${prefix}: ${property}`)
    }
  }
  const old = { scope: 'positions', target: 'title', property: 'color', value: '#123456' }
  const seeded = seedIndependentStyles(registry, [old])
  for (const target of ['cloud.heading', 'cloud.job.title', 'cloud.performance.title', 'cloud.vm.title']) {
    assert.equal(seeded.find(row => row.target === target)?.value, old.value)
    const css = appearanceCss(registry, [{ ...old, target, value: '#abcdef' }])
    assert.ok(css.includes(`data-style-id="${target}"`))
    assert.ok(!css.includes('data-style-id="title"'))
  }
  assert.deepEqual(seedIndependentStyles(registry, seeded), seeded)
})

test('Cloud Run, Batch Spot, local and history controls are independently registered', () => {
  const roles = ['single.player.input', 'single.total.input', 'single.submit', 'combined.editor.player.input',
    'combined.editor.calculate', 'combined.group.title', 'combined.group.fens.value', 'combined.submit']
  for (const role of roles) for (const backend of ['run', 'batch']) {
    const target = `cloud.${backend}.${role}`
    assert.ok(registryTarget(registry, 'positions', target), target)
    const css = appearanceCss(registry, [{ scope: 'positions', target, property: 'radius', value: 14 }])
    assert.ok(css.includes('border-radius:14px'))
    assert.ok(!css.includes(`data-style-id="cloud.${backend === 'run' ? 'batch' : 'run'}.${role}"`))
  }
  for (const backend of ['local', 'cloud_run', 'batch_spot']) {
    for (const role of ['toggle', 'job', 'toggle-entry', 'imported.value', 'target.value', 'newer', 'older']) {
      assert.ok(registryTarget(registry, 'positions', `history.${backend}.${role}`))
    }
  }
})

test('analysis components no longer use shared generic heading/field/button identities', () => {
  const files = ['CloudAnalysisSection', 'CloudGameDraft', 'CloudDraftSelectionEditor', 'CloudDraftGroupData',
    'CloudComputeField', 'LocalAnalysisSection', 'LocalAnalysisActivity', 'LocalGameSelection', 'LocalLoopSelection',
    'JobStatus', 'AnalysisHistory', 'CloudJobOverview', 'CloudVmTiles', 'CloudPerformanceReportModal', 'CloudPerformanceSummary']
  for (const file of files) {
    const source = fs.readFileSync(new URL(`../src/pages/positions/${file}.jsx`, import.meta.url), 'utf8')
    assert.doesNotMatch(source, /pageStyleTarget\('(title|text|label|value|button|field|panel|card|disclosure|table(?:\.header|\.cell)?)'\)/, file)
  }
})

test('coverage containers, tracks, values and fills are separate and retain old colors', () => {
  const rows = [{ scope: 'positions', target: 'chart.coverage', property: 'chartColor1', value: '#abcdef' }]
  const seeded = seedIndependentStyles(registry, rows)
  for (const key of ['games', 'games-analyzed', 'positions', 'analyzed']) {
    for (const part of ['track', 'fill', 'value']) assert.ok(registryTarget(registry, 'positions', `coverage.${key}.${part}`))
    assert.equal(seeded.find(row => row.target === `coverage.${key}.track`)?.value, '#abcdef')
  }
})
