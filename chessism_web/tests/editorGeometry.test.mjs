import test from 'node:test'
import assert from 'node:assert/strict'
import { fitEditor, resizeEditor, RESIZE_EDGES } from '../src/appearance/editorGeometry.js'
const viewport={width:1200,height:900},start={left:200,top:100,width:350,height:560}
test('every border and corner resizes without losing the opposite anchor',()=>{
  assert.equal(resizeEditor(start,'w',-80,0,viewport).left,120)
  assert.equal(resizeEditor(start,'w',-80,0,viewport).width,430)
  assert.equal(resizeEditor(start,'n',0,-30,viewport).top,70)
  assert.equal(resizeEditor(start,'n',0,-30,viewport).height,590)
  assert.deepEqual(resizeEditor(start,'se',120,50,viewport),{left:200,top:100,width:470,height:610})
  for(const edge of RESIZE_EDGES)for(const delta of [-5000,5000]){
    const box=resizeEditor(start,edge,delta,delta,viewport)
    assert.ok(box.left>=8&&box.top>=8&&box.width>=280&&box.height>=160)
    assert.ok(box.left+box.width<=1192&&box.top+box.height<=892)
  }
})
test('moving and viewport shrink keep the whole popup reachable',()=>{
  assert.deepEqual(fitEditor({...start,left:5000,top:5000},viewport),{left:842,top:332,width:350,height:560})
  const box=fitEditor(start,{width:260,height:150})
  assert.deepEqual(box,{left:8,top:8,width:244,height:134})
})
