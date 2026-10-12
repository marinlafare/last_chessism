import assert from 'node:assert/strict'
import { positionSlices, rankedGamePlayers, gamePlayerPage, gameAxisMax, sliceDescription, playerAvatarUrl } from '../src/pages/home/summaryData.js'
import { pieSegments } from '../src/utils/pieSegments.js'

assert.deepEqual(positionSlices(null), [])
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
assert.deepEqual(positionSlices(summary).map(s=>s.value),[2,2])
assert.equal(sliceDescription(pieSegments(positionSlices(summary))[0]),'Titled-player positions: 2 FENs · 50.0%')
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
