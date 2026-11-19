import { useEffect, useRef, useState } from 'react'
import { fetchPlayerAnalysisCounts } from './positionsApi'
import { MAX_ANALYSIS_BATCH_SIZE } from './positionPageSupport.js'

export function usePlayerPositionInspection() {
  const [playerJob, renderPlayer] = useState({ playerName: '', totalFens: 100, batchSize: MAX_ANALYSIS_BATCH_SIZE })
  const [playerInspection, setPlayerInspection] = useState({ loading: false, error: '', data: null })
  const current = useRef(playerJob)
  const request = useRef(null)
  useEffect(() => () => request.current?.abort(), [])
  const setPlayerJob = update => {
    const next = typeof update === 'function' ? update(current.current) : update
    if (next.playerName !== current.current.playerName) {
      request.current?.abort()
      setPlayerInspection({ loading: false, error: '', data: null })
    }
    current.current = next
    renderPlayer(next)
  }
  const handleInspectPlayer = async () => {
    request.current?.abort()
    const playerName = current.current.playerName.trim().toLowerCase()
    if (!playerName) {
      setPlayerInspection({ loading: false, error: 'Player name is required.', data: null })
      return
    }
    const controller = new AbortController()
    request.current = controller
    setPlayerInspection({ loading: true, error: '', data: null })
    try {
      const data = await fetchPlayerAnalysisCounts(playerName, { signal: controller.signal })
      if (!controller.signal.aborted) setPlayerInspection({ loading: false, error: '', data })
    } catch (error) {
      if (!controller.signal.aborted) setPlayerInspection({ loading: false, error: error.message || 'Unable to inspect player positions.', data: null })
    }
  }
  return { playerJob, setPlayerJob, playerInspection, setPlayerInspection, handleInspectPlayer }
}
