import GamesView from './games/GamesView'
import { useGamesPage } from './games/useGamesPage'

export default function Games() {
  const page = useGamesPage()
  return <GamesView page={page} />
}
