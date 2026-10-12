import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import { appearanceCss,normalizeColor,replaceRgb,sameOverrides } from '../src/appearance/styleCore.js'
import { findPositionOccurrence } from '../src/components/chess/positionOccurrence.js'
import { editorTargets, restoreSavedStyle } from '../src/appearance/editorTargets.js'
const registry=JSON.parse(fs.readFileSync(new URL('../../chessism_api/appearance_registry.json',import.meta.url)))
const row={scope:'scored_positions',target:'unique.slice',property:'fill',value:'#123456'}
test('default emits no overrides; registered styles are scoped and injection is rejected',()=>{
  assert.equal(appearanceCss(registry,[]),'')
  assert.match(appearanceCss(registry,[row]),/data-style-scope="scored_positions"/)
  assert.match(appearanceCss(registry,[row]),/fill:#123456/)
  for(const bad of [{...row,value:'red; display:none'},{...row,target:'body'},{...row,scope:'games'},{...row,property:'position'}])assert.equal(appearanceCss(registry,[bad]),'')
  assert.equal(sameOverrides([row],[{...row}]),true)
  assert.equal(sameOverrides([row],[]),false)
})
test('RGB edits preserve partial opacity, transparent RGB becomes visible',()=>{
  assert.equal(normalizeColor('rgba(255, 128, 0, 0.5)'),'#ff800080')
  assert.equal(normalizeColor('transparent'),'#00000000')
  assert.equal(replaceRgb('#11111180','#abcdef'),'#abcdef80')
  assert.equal(replaceRgb('#11111100','#abcdef'),'#abcdefff')
})
test('return restores saved settings, not defaults, without discarding unrelated drafts',()=>{
  const sibling={...row,property:'stroke',target:'unique.outline',value:'#abcdef80'}
  const saved=[row,sibling],draft=[{...row,value:'#ffffff'},{...sibling,value:'#111111'}]
  assert.deepEqual(restoreSavedStyle(draft,saved,row.scope,row.target,'fill'),[draft[1],row])
  assert.deepEqual(restoreSavedStyle(draft,[],row.scope,row.target,'fill'),[draft[1]])
  assert.deepEqual(restoreSavedStyle(draft,[{...row,value:'#789012'}],row.scope,row.target,'fill'),[draft[1],{...row,value:'#789012'}])
  assert.equal(draft[0].value,'#ffffff')
})
test('pie slice popups edit their own fill and the shared parent border',()=>{
  for(const id of ['unique.slice','nonunique.slice',...Array.from({length:9},(_,i)=>`slice.${i+2}`)]){
    const parentId=registry.targets[id].editorParent
    const parent={dataset:{styleId:parentId,styleScope:'scored_positions'},parentElement:null}
    const element={parentElement:{dataset:{},parentElement:parent}}
    const selected={id,scope:'scored_positions',element}
    const targets=editorTargets(registry,selected)
    assert.deepEqual(targets.map(item=>item.id),[id,parentId])
    assert.ok(targets[1].spec.properties.includes('stroke'))
    parent.dataset.styleScope='another_page'
    assert.equal(editorTargets(registry,selected).length,1)
  }
})
test('game jump uses exact FEN identity and requested occurrence, not just piece placement',()=>{
  const fen='8/8/8/8/8/8/4K3/7k w - -'
  const data={moves:[{fen_after:fen+' 0 1',move_number:1,move_color:'white'},{fen_after:fen+' 5 9',move_number:9,move_color:'black'}]}
  assert.deepEqual(findPositionOccurrence(data,{fen,n_move:9,move_color:'black'}),{index:2,found:true})
  assert.equal(findPositionOccurrence(data,{fen:fen.replace(' w ',' b ')}).found,false)
})
