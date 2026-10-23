import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import path from 'node:path'
import { PAGE_SCOPES,scopeForPath } from '../src/appearance/pageScopes.js'
import { appearanceCss,registryTarget,seedIndependentStyles } from '../src/appearance/styleCore.js'
import { cloudJobProgress } from '../src/pages/positions/cloudJobProgress.js'
const legacy=JSON.parse(fs.readFileSync(new URL('../../chessism_api/appearance_registry.json',import.meta.url)))
const extra=JSON.parse(fs.readFileSync(new URL('../../chessism_api/appearance_scopes.json',import.meta.url)))
const charts=JSON.parse(fs.readFileSync(new URL('../../chessism_api/appearance_charts.json',import.meta.url)))
const registry={...legacy,properties:{...legacy.properties,...charts.properties},scopes:Object.fromEntries(Object.entries(extra.scopes).map(([id,spec])=>[id,{...spec,targets:{...extra.common_targets,...charts.targets,...spec.targets,...(id===legacy.scope?legacy.targets:{})}}]))}

test('each normal and cancellation step exposes an independent progress-bar style', () => {
  const normal = cloudJobProgress({ status: 'queued', runs: [] }).steps
  const stopping = cloudJobProgress({ status: 'running', runs: [], stop_cleanup: { phase: 'stopping' } }).steps
  const targets = [...normal, ...stopping].map(step => `cloud.step.progress.${step.id}`)
  assert.equal(new Set(targets).size, targets.length)
  for (const target of targets) {
    const spec = registryTarget(registry, 'positions', target)
    assert.equal(spec.editorParent, 'cloud.steps')
    for (const [property, value] of Object.entries({
      chartColor1: '#123456', barHoverColor: '#abcdef', background: '#101010', hoverBackground: '#202020',
      borderColor: '#112233', hoverBorderColor: '#334455', borderWidth: 2, summaryBarHeight: 10, radius: 4, padding: 2,
    })) {
      assert.ok(appearanceCss(registry, [{ scope: 'positions', target, property, value }]), `${target}/${property}`)
      assert.equal(appearanceCss(registry, [{ scope: 'home', target, property, value }]), '')
    }
  }
})

test('every editable app route and alias has an explicit, server-validated scope',()=>{
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
    if(file.startsWith('database/'))return ['database']
    if(file==='Database.jsx')return ['database']
    if(file==='positions/FenIngestionPanel.jsx')return ['positions','scored_positions']
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
    ['summary.games','barHoverColor','#335577'],['summary.games','barHoverBorder','#eeffee'],
    ['summary.total','color','#112233'],['overview.layout','homeTopMargin',3.5],
  ]){
    assert.ok(appearanceCss(registry,[{scope:'home',target,property,value}]))
    assert.equal(appearanceCss(registry,[{scope:'positions',target,property,value}]),'')
  }
  assert.equal(registryTarget(registry,'home','summary.titled').editorParent,'summary.positions')
  assert.equal(registryTarget(registry,'home','summary.panel').editorParent,'overview.layout')
  assert.equal(registryTarget(registry,'home','characters.panel').editorParent,'overview.layout')
  assert.ok(appearanceCss(registry,[{scope:'home',target:'overview.layout',property:'homeTopMargin',value:3.5}]).includes('--home-top-gap:3.5rem'))
  const games=registryTarget(registry,'home','summary.games')
  assert.deepEqual(games.hiddenProperties,['chartColor3','chartColor4'])
  // Total and scored-game fills are editable; legacy extra colors stay valid.
  for(const property of games.hiddenProperties)assert.ok(appearanceCss(registry,[{scope:'home',target:'summary.games',property,value:'#112233'}]))
})

test('individual home controls seed saved shared styles once, without relinking drafts',()=>{
  const legacy={scope:'home',target:'button',property:'background',value:'#123456'}
  const own={...legacy,target:'player.name',value:'#ffffff'}
  const old=[legacy,own,{...legacy,scope:'games',value:'#654321'}]
  const copy=structuredClone(old), seeded=seedIndependentStyles(registry,old)
  assert.deepEqual(old,copy)
  const targets=Object.entries(registry.scopes.home.targets).filter(([,spec])=>spec.initialStyleFrom==='button').map(([id])=>id)
  assert.equal(targets.length,22)
  for(const target of targets){
    assert.equal(seeded.find(row=>row.scope==='home' && row.target===target).value,target==='player.name'?'#ffffff':'#123456')
    assert.ok(appearanceCss(registry,[{...legacy,target}]).includes(`data-style-id="${target}"`))
  }
  assert.deepEqual(seedIndependentStyles(registry,seeded),seeded)
  assert.deepEqual(seedIndependentStyles(registry,[]),[])
  const changed=seeded.map(row=>row.target==='button'?{...row,value:'#abcdef'}:row)
  assert.equal(changed.find(row=>row.target==='summary.games.next').value,'#123456')
  for(const row of seeded.filter(row=>row.target!=='button'))assert.equal(row.scope,'home')
  const files=['PlayerAnalysisWorkspace.jsx','PlayerBehaviouralPanel.jsx','PlayerMeasuresWorkspace.jsx','PlayerMeasuresPanel.jsx']
  const buttons=files.flatMap(file=>[...fs.readFileSync(new URL(`../src/pages/players/${file}`,import.meta.url),'utf8').matchAll(/buttonTarget="([^"]+)"/g)].map(match=>match[1]))
  assert.equal(buttons.length,9)
  assert.equal(new Set(buttons).size,9)
  for(const target of buttons)assert.ok(registryTarget(registry,'home',target))
})

test('only the opted-in medal keeps both hover colors when pressed',()=>{
  const row={scope:'home',target:'characters.medal',property:'hoverColor',value:'#abcdef'}
  assert.match(appearanceCss(registry,[row]),/:hover,.*\[aria-pressed="true"\]\{color:#abcdef;/)
  assert.ok(!appearanceCss(registry,[{...row,property:'color'}]).includes('aria-pressed'))
  assert.match(appearanceCss(registry,[{...row,property:'hoverBackground'}]),/:hover,.*\[aria-pressed="true"\]\{background-color:#abcdef;/)
  assert.ok(!appearanceCss(registry,[{...row,property:'background'}]).includes('aria-pressed'))
  assert.ok(!appearanceCss(registry,[{...row,target:'player.name'}]).includes('aria-pressed'))
  assert.ok(!appearanceCss(registry,[{...row,target:'player.name',property:'hoverBackground'}]).includes('aria-pressed'))
})

test('player update and delete styles are independent for every editable property',()=>{
  const properties=registryTarget(registry,'home','button').properties
  for(const target of ['player.update','player.delete']){
    assert.deepEqual(registryTarget(registry,'home',target).properties,properties)
    for(const property of properties){
      const spec=registry.properties[property]
      const value=spec.kind==='color'?'#123456':spec.kind==='number'?spec.min:spec.options[0]
      const shared={scope:'home',target:'button',property,value}
      const seeded=seedIndependentStyles(registry,[shared])
      assert.equal(seeded.find(row=>row.target===target)?.value,value)
      const css=appearanceCss(registry,[{...shared,target}])
      assert.ok(css.includes(`data-style-id="${target}"`))
      assert.ok(!css.includes(`data-style-id="${target==='player.update'?'player.delete':'player.update'}"`))
      assert.ok(!css.includes('data-style-id="button"'))
    }
  }
})

test('list rating styles inherit saved metric settings once, then remain independent',()=>{
  const shared={scope:'home',target:'value',property:'color',value:'#123456'}
  const seeded=seedIndependentStyles(registry,[shared])
  assert.deepEqual(registryTarget(registry,'home','characters.rating').properties,registryTarget(registry,'home','value').properties.filter(property=>property!=='background'))
  assert.deepEqual(seeded.filter(row=>row.target==='value'||row.target==='characters.rating'),[shared,{...shared,target:'characters.rating'}])
  const changed=seeded.map(row=>row.target==='value'?{...row,value:'#abcdef'}:row)
  assert.deepEqual(seedIndependentStyles(registry,changed),changed)
  assert.ok(!appearanceCss(registry,[seeded[1]]).includes('data-style-id="value"'))
  assert.equal(appearanceCss(registry,[{...seeded[1],scope:'games'}]),'')
})
