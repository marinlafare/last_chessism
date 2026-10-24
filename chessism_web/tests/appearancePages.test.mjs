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

test('profile fields have separate backgrounds and preserve saved title/value styles',()=>{
  const fields=['player','country','name','joined','last-play']
  const legacy=[
    {scope:'home',target:'player.metadata.label',property:'color',value:'#abcdef'},
    {scope:'home',target:'player.metadata.value',property:'fontSize',value:16},
  ]
  const seeded=seedIndependentStyles(registry,legacy)
  for(const field of fields){
    const target=`player.metadata.${field}`
    for(const property of ['background','hoverBackground']){
      const css=appearanceCss(registry,[{scope:'home',target,property,value:'#12345680'}])
      assert.ok(css.includes(`data-style-id="${target}"`))
      assert.ok(css.includes('background-color:#12345680'))
      assert.equal(css.includes(':hover'),property==='hoverBackground')
      assert.equal(appearanceCss(registry,[{scope:'positions',target,property,value:'#12345680'}]),'')
      for(const other of fields.filter(item=>item!==field))assert.ok(!css.includes(`data-style-id="player.metadata.${other}"`))
    }
    for(const part of ['label','value']){
      assert.equal(registryTarget(registry,'home',`${target}.${part}`).editorParent,target)
      assert.equal(seeded.find(row=>row.target===`${target}.${part}`).value,part==='label'?'#abcdef':16)
    }
  }
  assert.deepEqual(seedIndependentStyles(registry,seeded),seeded)
  assert.ok(registryTarget(registry,'home','player.metadata.dates').properties.includes('background'))
})

test('hero photo and data containers have independent hover styles without resetting saved backgrounds',()=>{
  for(const target of ['player.hero','player.navigation','player.profile','player.photo','player.profilePopup','player.profilePopup.field']){
    for(const [property,value] of Object.entries({background:'#123456',hoverBackground:'#654321',borderColor:'#112233',hoverBorderColor:'#445566',radius:14,padding:6})){
      const css=appearanceCss(registry,[{scope:'home',target,property,value}])
      assert.ok(css.includes(`data-style-id="${target}"`),`${target}/${property}`)
      assert.equal(css.includes(':hover'),property.startsWith('hover'))
      assert.equal(appearanceCss(registry,[{scope:'positions',target,property,value}]),'')
    }
  }
  const seeded=seedIndependentStyles(registry,[
    {scope:'home',target:'panel',property:'background',value:'#112233'},
    {scope:'home',target:'player.profile',property:'background',value:'#123456'},
  ])
  assert.equal(seeded.find(row=>row.target==='player.hero').value,'#112233')
  assert.equal(seeded.find(row=>row.target==='player.navigation').value,'#123456')
  assert.equal(seeded.find(row=>row.target==='player.photo'),undefined)
})

test('Behavioral analysis container hover is independent of its button and other panels',()=>{
  const css=appearanceCss(registry,[{scope:'home',target:'player.analysis',property:'hoverBackground',value:'#12345680'}])
  assert.ok(css.includes('data-style-id="player.analysis"'))
  assert.ok(css.includes(':hover{background-color:#12345680;}'))
  for(const target of ['player.analysis.toggle','player.measures','player.profile'])assert.ok(!css.includes(`data-style-id="${target}"`))
})

test('new optional hover controls do not introduce inherited colors into existing presets',()=>{
  const testRegistry=structuredClone(registry)
  const target=testRegistry.scopes.home.targets['player.days.count']
  target.properties.push('hoverBackground')
  target.initialStyleExcludedProperties=['hoverBackground']
  const old=[{scope:'home',target:'value',property:'color',value:'#abcdef'},
    {scope:'home',target:'value',property:'hoverBackground',value:'#123456'}]
  const seeded=seedIndependentStyles(testRegistry,old)
  assert.ok(seeded.some(row=>row.target==='player.days.count'&&row.property==='color'))
  assert.ok(!seeded.some(row=>row.target==='player.days.count'&&row.property==='hoverBackground'))
  const own={scope:'home',target:'player.days.count',property:'hoverBackground',value:'#654321'}
  assert.ok(seedIndependentStyles(testRegistry,[...old,own]).some(row=>row.target===own.target&&row.property===own.property&&row.value===own.value))
  assert.ok(appearanceCss(testRegistry,[own]).includes(':hover{background-color:#654321;}'))
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
  assert.equal(targets.filter(target=>!target.startsWith('gameReview.')).length,22)
  for(const target of targets){
    assert.equal(seeded.find(row=>row.scope==='home' && row.target===target).value,target==='player.name'?'#ffffff':'#123456')
    assert.ok(appearanceCss(registry,[{...legacy,target}]).includes(`data-style-id="${target}"`))
  }
  assert.deepEqual(seedIndependentStyles(registry,seeded),seeded)
  assert.deepEqual(seedIndependentStyles(registry,[]),[])
  const changed=seeded.map(row=>row.target==='button'?{...row,value:'#abcdef'}:row)
  assert.equal(changed.find(row=>row.target==='summary.games.next').value,'#123456')
  for(const row of seeded.filter(row=>row.target!=='button'&&!row.target.startsWith('gameReview.')))assert.equal(row.scope,'home')
  assert.equal(seeded.find(row=>row.scope==='games'&&row.target==='gameReview.move').value,'#654321')
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
  const rating=seeded.find(row=>row.target==='characters.rating')
  assert.ok(!appearanceCss(registry,[rating]).includes('data-style-id="value"'))
  assert.equal(appearanceCss(registry,[{...rating,scope:'games'}]),'')
})

test('game review owns its targets and preserves old styles without remaining linked',()=>{
  for(const file of ['PlayerGameReviewModal.jsx','GameReviewBoard.jsx','GameReviewSoundSet.jsx']){
    const source=fs.readFileSync(new URL(`../src/pages/players/${file}`,import.meta.url),'utf8')
    for(const [,id] of source.matchAll(/pageStyleTarget\(['"`]([^'"`]+)['"`]/g))assert.ok(id.startsWith('gameReview.'),id)
  }
  const sources=[['panel','background'],['title','color'],['text','color'],['value','fontSize'],['button','background']]
  const old=sources.map(([target,property])=>({scope:'home',target,property,value:property==='fontSize'?16:'#abcdef'}))
  const seeded=seedIndependentStyles(registry,old)
  for(const target of ['gameReview.panel','gameReview.title','gameReview.mode','gameReview.clock.white.remaining','gameReview.move'])assert.ok(seeded.some(row=>row.target===target),target)
  const independent=seeded.map(row=>row.target==='gameReview.title'?{...row,value:'#123456'}:row)
  assert.deepEqual(seedIndependentStyles(registry,independent),independent)
  assert.equal(independent.find(row=>row.target==='title').value,'#abcdef')
  for(const [scope,spec] of Object.entries(registry.scopes))for(const [target,definition] of Object.entries(spec.targets)){
    if(!target.startsWith('gameReview.'))continue
    for(const property of ['background','hoverBackground','borderColor','hoverBorderColor','radius','padding'])assert.ok(definition.properties.includes(property),`${scope}/${target}/${property}`)
  }
  const css=appearanceCss(registry,[{scope:'home',target:'gameReview.move',property:'reviewSelectedBackground',value:'#123456'}])
  assert.match(css,/\.active\{background-color:#123456/)
  assert.ok(!css.includes('data-style-id="button"'))
})

test('sound dropdown exposes list/options from its trigger and independent active borders',()=>{
  const targets=registry.scopes.home.targets
  assert.deepEqual(targets['gameReview.soundSet'].editorRelated,['gameReview.soundSet.list','gameReview.soundSet.option'])
  for(const property of ['reviewSoundActiveBorderColor','reviewSoundActiveBorderWidth'])assert.ok(targets['gameReview.soundSet'].properties.includes(property))
  const css=appearanceCss(registry,[
    {scope:'home',target:'gameReview.soundSet',property:'reviewSoundActiveBorderColor',value:'#123456'},
    {scope:'home',target:'gameReview.soundSet.option',property:'hoverBackground',value:'#abcdef'},
  ])
  assert.match(css,/--review-sound-active-border-color:#123456/)
  assert.match(css,/\[data-highlighted="true"\]\{background-color:#abcdef/)
  assert.ok(!css.includes('data-style-id="button"'))
})
