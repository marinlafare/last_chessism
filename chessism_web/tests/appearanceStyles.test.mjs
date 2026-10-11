import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import { appearanceCss,normalizeColor,replaceRgb,sameOverrides } from '../src/appearance/styleCore.js'
import { findPositionOccurrence } from '../src/components/chess/positionOccurrence.js'
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
test('game jump uses exact FEN identity and requested occurrence, not just piece placement',()=>{
  const fen='8/8/8/8/8/8/4K3/7k w - -'
  const data={moves:[{fen_after:fen+' 0 1',move_number:1,move_color:'white'},{fen_after:fen+' 5 9',move_number:9,move_color:'black'}]}
  assert.deepEqual(findPositionOccurrence(data,{fen,n_move:9,move_color:'black'}),{index:2,found:true})
  assert.equal(findPositionOccurrence(data,{fen:fen.replace(' w ',' b ')}).found,false)
})
