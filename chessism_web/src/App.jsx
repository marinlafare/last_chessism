import Home from './pages/Home'
import Games from './pages/Games'
import Players from './pages/Players'

function App() {
  const path = window.location.pathname.replace(/\/+$/, '') || '/'

  if (path === '/games') {
    return <Games />
  }

  if (path === '/players') {
    return <Players />
  }

  return <Home />
}

export default App
