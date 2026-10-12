import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import path from 'node:path'
import { PAGE_SCOPES,scopeForPath } from '../src/appearance/pageScopes.js'
import { appearanceCss,registryTarget } from '../src/appearance/styleCore.js'
const legacy=JSON.parse(fs.readFileSync(new URL('../../chessism_api/appearance_registry.json',import.meta.url)))
const extra=JSON.parse(fs.readFileSync(new URL('../../chessism_api/appearance_scopes.json',import.meta.url)))
const charts=JSON.parse(fs.readFileSync(new URL('../../chessism_api/appearance_charts.json',import.meta.url)))
const registry={...legacy,properties:{...legacy.properties,...charts.properties},scopes:Object.fromEntries(Object.entries(extra.scopes).map(([id,spec])=>[id,{...spec,targets:{...extra.common_targets,...charts.targets,...spec.targets,...(id===legacy.scope?legacy.targets:{})}}]))}

test('every app route and alias has an explicit, server-validated scope',()=>{
  const routes=Object.fromEntries(Object.entries(extra.scopes).flatMap(([scope,spec])=>spec.routes.map(route=>[route,scope])))
  assert.deepEqual(PAGE_SCOPES,routes)
  assert.equal(scopeForPath('/positions/'),'positions')
  assert.equal(scopeForPath('/games/123?tab=moves'),'games')
  const app=fs.readFileSync(new URL('../src/App.jsx',import.meta.url),'utf8')
  const pathBlock=app.slice(app.indexOf('const APP_PATHS'),app.indexOf('const getLocationKey'))
  for(const [,route] of pathBlock.matchAll(/'([^']+)'/g))assert.ok(routes[route],route)
})

test('styles remain page-local and legacy presets still work',()=>{
  const row={scope:'positions',target:'button',property:'color',value:'#abcdef'}
  const css=appearanceCss(registry,[row])
  assert.ok(css.includes('data-style-scope="positions"'))
  assert.ok(!css.includes('data-style-scope="games"'))
  assert.ok(appearanceCss(registry,[{...row,scope:'scored_positions',target:'unique.slice',property:'fill'}]))
  assert.equal(appearanceCss(registry,[{...row,target:'unique.slice',property:'fill'}]),'')
  assert.equal(appearanceCss(registry,[{...row,scope:'invented'}]),'')
})

test('registered target annotations stay in their owning page scopes',()=>{
  const root=new URL('../src/pages/',import.meta.url)
  const scopeForFile=file=>{
    if(file==='players/PlayerGameReviewModal.jsx')return ['home','scored_positions','games']
    if(file.startsWith('scored-positions/'))return ['scored_positions']
    if(file.startsWith('players/'))return ['home']
    const dirs={home:'home',games:'games',positions:'positions','system-status':'system_status'}
    if(dirs[file.split('/')[0]])return [dirs[file.split('/')[0]]]
    if(file.startsWith('research/algorithms/'))return ['algorithms']
    if(file.startsWith('research/matrices/'))return ['matrices','algorithms']
    return [{Home:'home',Games:'games',SystemStatus:'system_status',DownloadNewGames:'download_games',MainCharacters:'main_characters',SecondaryCharacter:'secondary_character',Positions:'positions',LiveAnalysis:'live_analysis',ScoredPositions:'scored_positions',AnalyzeTimes:'analyze_times',Research:'research',AccuracyCoefficient:'accuracy_coefficient',MatricesConstructor:'matrices',Algorithms:'algorithms',SalienceResearch:'salience',DatabaseBackups:'backups'}[file.replace('.jsx','')]].filter(Boolean)
  }
  const walk=(dir,prefix='')=>{
    for(const entry of fs.readdirSync(dir,{withFileTypes:true})){
      const relative=prefix+entry.name,absolute=path.join(dir,entry.name)
      if(entry.isDirectory()){walk(absolute,relative+'/');continue}
      if(!entry.name.endsWith('.jsx'))continue
      const source=fs.readFileSync(absolute,'utf8'),scopes=scopeForFile(relative)
      for(const [,id] of source.matchAll(/pageStyleTarget\('([^']+)'\)/g)){
        for(const scope of scopes)assert.ok(registryTarget(registry,scope,id),`${relative}: ${scope}/${id}`)
      }
    }
  }
  walk(root.pathname)
})

test('chart palettes are scoped, validated and independent of chart data',()=>{
  const row={scope:'positions',target:'chart.coverage',property:'chartColor1',value:'#11223380'}
  const css=appearanceCss(registry,[row])
  assert.ok(css.includes('--chart-color-1:#11223380'))
  assert.ok(css.includes('data-style-scope="positions"'))
  assert.equal(appearanceCss(registry,[{...row,property:'chartColor99'}]),'')
  assert.equal(appearanceCss(registry,[{...row,value:'url(https://invalid.test)'}]),'')
})

test('homepage charts expose colors and borders without changing other charts',()=>{
  for(const [target,property,value] of [
    ['summary.titled','fill','#ffffff'],['summary.other','fill','#112233'],
    ['summary.positions','stroke','#abcdef'],['summary.positions','strokeWidth',2],
    ['summary.games','stroke','#cdefab'],['summary.games','chartColor1','#223344'],
  ]){
    assert.ok(appearanceCss(registry,[{scope:'home',target,property,value}]))
    assert.equal(appearanceCss(registry,[{scope:'positions',target,property,value}]),'')
  }
  assert.equal(registryTarget(registry,'home','summary.titled').editorParent,'summary.positions')
})
