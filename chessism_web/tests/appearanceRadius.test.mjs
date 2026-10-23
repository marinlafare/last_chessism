import test from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { appearanceCss, registryTarget } from '../src/appearance/styleCore.js'
import { supportsCornerRadius, computedCornerRadius } from '../src/appearance/cornerRadius.js'
import { numericStyleValue } from '../src/appearance/spacing.js'

const read = name => JSON.parse(readFileSync(new URL(`../../chessism_api/${name}.json`, import.meta.url)))
const base = read('appearance_registry'), scopes = read('appearance_scopes'), charts = read('appearance_charts')
const registry = { ...base, properties: {...base.properties,...charts.properties}, scopes:Object.fromEntries(
  Object.entries(scopes.scopes).map(([scope,spec])=>[scope,{...spec,targets:{...scopes.common_targets,...charts.targets,...spec.targets}}])) }
const html = name => ({ namespaceURI:'http://www.w3.org/1999/xhtml', localName:name })
const svg = name => ({ namespaceURI:'http://www.w3.org/2000/svg', localName:name })

test('all registered targets allow independently saved corner radius without changing defaults',()=>{
  assert.equal(appearanceCss(registry,[]),'')
  for(const [scope,spec] of Object.entries(registry.scopes)) for(const target of Object.keys(spec.targets)) {
    assert.equal(registryTarget(registry,scope,target).properties.filter(p=>p==='radius').length,1)
    const css=appearanceCss(registry,[{scope,target,property:'radius',value:12}])
    assert.ok(css.includes('border-radius:12px;'))
    assert.ok(css.includes(':where(rect){rx:12px;ry:12px;}'))
    assert.ok(css.includes(`data-style-scope="${scope}"`))
    assert.ok(!css.includes('overflow:'))
  }
  for(const value of [-1,41,Infinity,NaN,'12px',true]) {
    assert.equal(appearanceCss(registry,[{scope:'home',target:'heading',property:'radius',value}]),'')
  }
})

test('only elements whose box or rectangular geometry supports rounding expose the control',()=>{
  for(const name of ['div','span','h2','button','input','select','img','canvas']) assert.ok(supportsCornerRadius(html(name),{display:'block'}),name)
  for(const name of ['svg','rect']) assert.ok(supportsCornerRadius(svg(name),{}),name)
  for(const name of ['g','path','circle','ellipse','line','text','foreignObject']) assert.equal(supportsCornerRadius(svg(name),{}),false,name)
  assert.equal(supportsCornerRadius(html('div'),{display:'contents'}),false)
  for(const display of ['table-row','table-row-group','table-header-group','table-column','table-column-group']) {
    assert.equal(supportsCornerRadius(html('div'),{display}),false)
  }
  for(const [name,display] of [['table','table'],['td','table-cell']]) {
    assert.equal(supportsCornerRadius(html(name),{display,borderCollapse:'collapse'}),false)
    assert.ok(supportsCornerRadius(html(name),{display,borderCollapse:'separate'}))
  }
})

test('relative/mixed corners are not misreported as uniform pixels; SVG rx/ry are read correctly',()=>{
  const spec=registry.properties.radius
  assert.equal(numericStyleValue(spec,'50%'),'')
  assert.equal(numericStyleValue(spec,'2px 8px'),'')
  assert.equal(numericStyleValue(spec,'8px'),8)
  const computed=(rx,ry)=>({getPropertyValue:key=>key==='rx'?rx:ry})
  assert.equal(computedCornerRadius(svg('rect'),computed('auto','auto')),'0px')
  assert.equal(computedCornerRadius(svg('rect'),computed('6px','auto')),'6px')
  assert.equal(computedCornerRadius(svg('rect'),computed('auto','6px')),'6px')
  assert.equal(computedCornerRadius(svg('rect'),computed('6px','12px')),'6px 12px')
})
