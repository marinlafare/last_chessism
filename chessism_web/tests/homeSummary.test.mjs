import assert from 'node:assert/strict'
import { positionSlices, summaryGamePlayers, rankedGamePlayers, gamePlayerPage, gameAxisMax, gameBarLabel, sliceDescription, fenTotalLabel, playerAvatarUrl } from '../src/pages/home/summaryData.js'
import { pieSegments } from '../src/utils/pieSegments.js'

assert.deepEqual(positionSlices(null), [])
for (const [games, label] of [[0,'0'],[19.1,'20'],[999,'999'],[1000,'1K'],[1001,'2K'],[7500,'8K'],[12340,'13K'],[999999,'1M'],[1000001,'2M']]) {
  assert.equal(gameBarLabel(games),label)
}
assert.equal(playerAvatarUrl(' https://images.example.invalid/avatar.png '),'https://images.example.invalid/avatar.png')
assert.equal(playerAvatarUrl('/chessism.jpg'),'/chessism.jpg')
for (const value of [null, '', 123, 'javascript:alert(1)', 'data:image/svg+xml,test', '//another.invalid/image.png', '/\\another.invalid/image.png']) {
  assert.equal(playerAvatarUrl(value),'')
}
const summary = {refreshed_at:'2026-10-11',total_fens:4,titled_fens:2,players:[
  {player_name:'a',title:'GM',games:2,game_half_units:3},
  {player_name:'b',title:'IM',games:1,game_half_units:1},
  {player_name:'c',title:'WFM',games:0,game_half_units:0},
]}
assert.equal(summaryGamePlayers(null), undefined)
assert.equal(summaryGamePlayers(summary), summary.players)
const livePlayers = [{player_name:'just-ingested',title:'GM',games:12}]
const fanPlayers = [{player_name:'new-fan',title:'',games:8}]
const liveSummary = {...summary, refreshed_at:null, stale:true,
  game_players:{titled:livePlayers,untitled:fanPlayers}}
assert.equal(summaryGamePlayers(liveSummary),livePlayers)
assert.equal(summaryGamePlayers(liveSummary,false),fanPlayers)
assert.deepEqual(summaryGamePlayers({...summary,game_players:{titled:[]}}),[])
assert.equal(gamePlayerPage(summaryGamePlayers(liveSummary)).players[0].player_name,'just-ingested')
assert.deepEqual(positionSlices(summary).map(s=>s.value),[2,2])
assert.equal(sliceDescription(pieSegments(positionSlices(summary))[0]),'professionals\n50%\n2')
assert.equal(sliceDescription(pieSegments(positionSlices(summary))[1]),'fans\n50%\n2')
assert.equal(sliceDescription({key:'titled',value:400000,fraction:2/3}),'professionals\n67%\n400,000')
for (const [value, label] of [[999999,'999,999'],[1000000,'1.0M'],[3800000,'3.8M'],[10000000,'10.0M']]) {
  assert.equal(sliceDescription({key:'titled',value,fraction:0.5}),`professionals\n50%\n${label}`)
}
for (const [total, label] of [[null,'—'],[undefined,'—'],[0,'0.0M'],[600000,'0.6M'],[51510523,'51.5M'],[2000000,'2.0M']]) assert.equal(fenTotalLabel(total),label)
assert.equal(pieSegments(positionSlices({...summary,titled_fens:4}))[0].full,true)
assert.equal(pieSegments(positionSlices({...summary,total_fens:0,titled_fens:0})).length,0)
const players = [
  {player_name:'a', games:10,game_half_units:20,avatar:'https://images.example.invalid/a.jpg'},
  {player_name:'b', games:15,game_half_units:15},
  ...['f','d','e','c'].map(player_name=>({player_name,games:8,game_half_units:16})),
  {player_name:'no-games',games:0,game_half_units:0},
]
const original=structuredClone(players)
assert.deepEqual(rankedGamePlayers(players).map(p=>p.player_name),['b','a','c','d','e','f'])
assert.deepEqual(players,original)
const first=gamePlayerPage(players), last=gamePlayerPage(players,1)
assert.deepEqual(first.players.map(p=>p.games),[15,10,8,8])
assert.equal(first.players[1].avatar,players[0].avatar)
assert.deepEqual(last.players.map(p=>p.player_name),['e','f'])
assert.equal(first.maxGames,15)
assert.equal(last.maxGames,8)
assert.ok(gameAxisMax(last.maxGames)<gameAxisMax(first.maxGames))
assert.equal(last.start,4)
assert.equal(gamePlayerPage(players,99).page,1)
assert.equal(gamePlayerPage(players,-1).page,0)
assert.equal(gamePlayerPage([players[0]],1).page,0)
assert.equal(gamePlayerPage().total,0)
assert.deepEqual(gamePlayerPage([]).players,[])
for(const max of [0,1,2,7,15,390,7500,1000000]){
  assert.ok(gameAxisMax(max)>=max)
  assert.equal(gameAxisMax(max)%4,0)
}
const widelySpaced=[100000,90000,80000,70000,19,12,8,1].map((games,i)=>({games,player_name:`p${i}`}))
assert.equal(gameAxisMax(gamePlayerPage(widelySpaced,1).maxGames),20)
assert.equal(gameAxisMax(gamePlayerPage(widelySpaced,0).maxGames),100000)
assert.equal(gameAxisMax(5000),5000)
console.log('PASS: position totals; game-count sorting, ties, four-player pages, bounds, per-page scales, whole-game ticks and empty data.')
