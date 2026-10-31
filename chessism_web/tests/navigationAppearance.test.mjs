import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync, existsSync } from 'node:fs'
import { appearanceCss } from '../src/appearance/styleCore.js'

const source = file => readFileSync(new URL(file, import.meta.url), 'utf8')
const registry = {
  ...JSON.parse(source('../../chessism_api/appearance_registry.json')),
  scopes: JSON.parse(source('../../chessism_api/appearance_scopes.json')).scopes,
}
Object.assign(registry.properties, JSON.parse(source('../../chessism_api/appearance_charts.json')).properties)

test('account buttons expose independent global navigation styles', () => {
  for (const target of ['account.online', 'account.logout']) {
    for (const property of ['background', 'hoverBackground', 'color', 'hoverColor', 'borderColor', 'hoverBorderColor']) {
      const row = { scope: 'navigation', target, property, value: '#123456' }
      const css = appearanceCss(registry, [row])
      assert.ok(css.includes(`[data-style-scope="navigation"][data-style-id="${target}"]`))
      assert.equal(appearanceCss(registry, [{ ...row, scope: 'research' }]), '')
    }
    for (const property of ['fontFamily', 'fontSize', 'textAlign', 'borderWidth', 'radius', 'padding']) {
      assert.ok(registry.scopes.navigation.targets[target].properties.includes(property))
    }
    assert.ok(source('../src/App.jsx').includes(`styleTarget('${target}', 'navigation')`))
  }
})

test('Hegel page, document, navigation, asset alias and Markdown-only dependencies are removed', () => {
  for (const file of ['../src/pages/Hegel.jsx', '../src/pages/hegel/hegel.css', '../../hegel_.md']) {
    assert.equal(existsSync(new URL(file, import.meta.url)), false)
  }
  for (const file of ['../src/App.jsx', '../src/components/layout/SideRail.jsx', '../vite.config.js', '../Dockerfile', '../../docker-compose.yml']) {
    assert.ok(!source(file).toLowerCase().includes('hegel'), file)
  }
  for (const file of ['../package.json', '../package-lock.json']) {
    assert.ok(!source(file).includes('react-markdown'))
    assert.ok(!source(file).includes('remark-gfm'))
  }
  // Old saved styles remain valid, without a rendered target or new defaults.
  assert.ok(appearanceCss(registry, [{ scope: 'navigation', target: 'item.hegel', property: 'color', value: '#abcdef' }]))
  assert.equal(registry.scopes.navigation.targets['item.hegel'].initialStyleFrom, undefined)
})
