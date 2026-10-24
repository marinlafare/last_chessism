import React, { useState } from 'react'
import { createRoot } from 'react-dom/client'
import AppearanceProvider, { AppearanceControls } from '/src/appearance/AppearanceProvider.jsx'
import PlayerProfileField from '/src/pages/players/PlayerProfileField.jsx'
import PlayerProfileDates from '/src/pages/players/PlayerProfileDates.jsx'
import PlayerAnalysisDisclosure from '/src/pages/players/PlayerAnalysisDisclosure.jsx'
import PlayerProfilePhoto from '/src/pages/players/PlayerProfilePhoto.jsx'
import { pageStyleTarget } from '/src/appearance/styleCore.js'
import '/src/index.css'
import '/src/pages/players/players.css'

history.replaceState(null, '', '/')
const key='profile-styles-fixture', state=window.profileStylesTest={requests:[]}
const profile={player_name:'example',name:'Example Player',country:'MX',location:'Mexico City',title:'GM',
  joined:1609459200,followers:0,verified:false,is_streamer:true,timezone:'America/Mexico_City',timezone_source:'manual',
  status:'premium',league:'Legend',url:'https://www.chess.com/member/example',twitch_url:'https://www.twitch.tv/example'}
window.fetch=async(input,options={})=>{
  const path=new URL(input,location.href).pathname, method=options.method||'GET'
  state.requests.push({path,method})
  const response=data=>new Response(JSON.stringify(data),{headers:{'Content-Type':'application/json'}})
  let saved={revision:1,active_preset_id:'official',default_preset_id:'official',presets:[{id:'official',name:'Default'}],
    overrides:[{scope:'home',target:'player.metadata.label',property:'color',value:'#abcdef'}],
    ...JSON.parse(localStorage.getItem(key)||'null')}
  if(method==='POST'&&path.endsWith('/appearance/save')){
    const payload=JSON.parse(options.body)
    if(payload.preset_id!=='official'||payload.revision!==saved.revision)throw Error('Unexpected style save')
    saved={...saved,revision:saved.revision+1,overrides:payload.overrides}
    localStorage.setItem(key,JSON.stringify(saved))
  }else if(method==='GET'&&path.endsWith('/games/example/recent')){
    return response({games:[{played_at:'2026-10-23T08:00:00Z'}]})
  }else if(method!=='GET'||!path.endsWith('/appearance'))throw Error('No live API access: '+path)
  return response({...saved,registry:window.profileStylesRegistry})
}
function ProfileFixture(){
  const [latestGame,setLatestGame]=useState(null)
  return <main style={{padding:24,maxWidth:800}}>
    <section {...pageStyleTarget('player.hero')}>
    <div {...pageStyleTarget('player.profile')}>
    <div className="players-profile-metadata">
      <PlayerProfileField field="player" label="Player">example</PlayerProfileField>
      <PlayerProfileField field="country" label="Country">MX</PlayerProfileField>
      <PlayerProfileField field="name" label="Name">Example Player</PlayerProfileField>
      <PlayerProfileDates profile={profile} onLatestChange={setLatestGame} />
    </div>
    <div style={{maxWidth:180}}><PlayerProfilePhoto profile={profile} latestGame={latestGame}
      positionStats={{total_games:1000,analyzed_games:700,total_fens:10000,analyzed_fens:7000}}
      hours={{bullet_games:800,bullet_hours:20,blitz_games:200,blitz_hours:18,rapid_games:0,rapid_hours:0}} /></div>
    </div>
    </section>
    <PlayerAnalysisDisclosure label="Behavioral analysis" target="player.analysis" buttonTarget="player.analysis.toggle" level={2}>
      <PlayerAnalysisDisclosure label="Hours" buttonTarget="player.activityHours.toggle"><p>Hours fixture</p></PlayerAnalysisDisclosure>
    </PlayerAnalysisDisclosure>
    <PlayerAnalysisDisclosure label="Stockfish analysis" target="player.measures" buttonTarget="player.measures.toggle" level={2}><p>Stockfish fixture</p></PlayerAnalysisDisclosure>
  </main>
}
createRoot(document.getElementById('root')).render(<AppearanceProvider account={{role:'admin'}} pageKey="/">
  <AppearanceControls /><ProfileFixture />
</AppearanceProvider>)
