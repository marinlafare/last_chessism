import PositionsView from './positions/PositionsView'
import { usePositionsPage } from './positions/usePositionsPage'

export default function Positions() {
  const page = usePositionsPage()
  return <PositionsView page={page} />
}
