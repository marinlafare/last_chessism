import PositionsView from './positions/PositionsView'
import { usePositionsPage } from './positions/usePositionsPage'
import './positions/positions.css'

export default function Positions() {
  const page = usePositionsPage()
  return <PositionsView page={page} />
}
