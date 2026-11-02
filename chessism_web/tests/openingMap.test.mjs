import {test} from 'node:test'
import assert from 'node:assert/strict'
import {prepareLayout} from '../src/pages/scored-positions/openingMapLayout.js'
import {mapView,slotIndex} from '../src/pages/scored-positions/openingMapModel.js'
const counts=[2,1,2,2,1,0,1,1,2,1]
const snapshot={revision:'test',slots:['all','a','b','both','a_white','a_black','b_white','b_black','both_white','both_black'],coverage:{games:counts},
 nodes:['chess:initial','chess:1','chess:2','chess:3'].map((key,i)=>({key,fen_id:i,appearances:counts,games:counts})),
 edges:[['chess:initial','chess:1'],['chess:1','chess:2'],['chess:2','chess:3'],['chess:3','chess:1']].map(([source,target],i)=>({key:`e${i}`,source,target,appearances:counts,games:counts}))}
test('layout is deterministic and filters reuse coordinates',()=>{
 const a=prepareLayout(snapshot),b=prepareLayout(snapshot)
 assert.deepEqual(a,b)
 assert.equal(a.groups.reduce((n,g)=>n+g.keys.length,0),4)
 assert.notEqual(a.positions['chess:initial'].group,a.positions['chess:1'].group)
 const options={color:'all',minimum:1,community:null,colorBy:'players'}
 const all=mapView(snapshot,a,{...options,scope:'all'}),player=mapView(snapshot,a,{...options,scope:'a'})
 assert.deepEqual(all.bounds,player.bounds)
 for(const node of player.nodes){const same=all.nodes.find(n=>n.key===node.key);assert.equal(node.x,same.x);assert.equal(node.y,same.y)}
 assert.equal(slotIndex(snapshot,'both','black'),9)
})
test('detail preserves directed moves and rare positions can be hidden',()=>{
 const layout=prepareLayout(snapshot),community=`group:${layout.positions['chess:1'].group}`
 const view=mapView(snapshot,layout,{scope:'both',color:'all',minimum:1,community,colorBy:'communities'})
 assert.ok(view.nodes.length)
 const keys=new Set(view.nodes.map(n=>n.key))
 assert.ok(view.edges.every(e=>keys.has(e.source)&&keys.has(e.target)))
 const hidden=mapView(snapshot,layout,{scope:'all',color:'all',minimum:10,community,colorBy:'communities'})
 assert.equal(hidden.nodes.length,0)
})
test('empty graphs and disconnected positions stay finite',()=>{
 const layout=prepareLayout({...snapshot,edges:[]})
 assert.ok(Object.values(layout.positions).every(p=>Number.isFinite(p.x)&&Number.isFinite(p.y)))
})
