import Home from './pages/Home'
import Games from './pages/Games'
import Players from './pages/Players'
import DownloadNewGames from './pages/DownloadNewGames'
import MainCharacters from './pages/MainCharacters'
import SecondaryCharacter from './pages/SecondaryCharacter'
import Positions from './pages/Positions'

function App() {
  const path = window.location.pathname.replace(/\/+$/, '') || '/'

  if (path === '/games') {
    return <Games />
  }

  if (path === '/players') {
    return <Players />
  }

  if (path === '/download_new_games') {
    return <DownloadNewGames />
  }

  if (path === '/main_characters') {
    return <MainCharacters />
  }

  if (path === '/secondary_character') {
    return <SecondaryCharacter />
  }

  if (path === '/positions') {
    return <Positions />
  }

  return <Home />
}

export default App
