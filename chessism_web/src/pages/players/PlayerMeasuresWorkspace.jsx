import { useEffect, useRef, useState } from 'react'
import PlayerMeasuresPanel from './PlayerMeasuresPanel'
import {
  fetchGameMeasures,
  fetchHourMeasures,
  fetchQualityCalendar,
} from './playerAnalysisApi'
import './player-measures.css'

const emptyRequest = { data: null, error: '', loading: false }

export default function PlayerMeasuresWorkspace({ playerName, disabled = false }) {
  const [quality, setQuality] = useState(null)
  const [qualityError, setQualityError] = useState('')
  const [qualityLoading, setQualityLoading] = useState(false)
  const [date, setDate] = useState('')
  const [hour, setHour] = useState('0')
  const [hourMode, setHourMode] = useState('all')
  const [hourRequest, setHourRequest] = useState(emptyRequest)
  const [gameId, setGameId] = useState('')
  const [gameRequest, setGameRequest] = useState(emptyRequest)
  const hourController = useRef(null)
  const gameController = useRef(null)

  useEffect(() => {
    hourController.current?.abort()
    gameController.current?.abort()
    setDate('')
    setHour('0')
    setHourMode('all')
    setHourRequest(emptyRequest)
    setGameId('')
    setGameRequest(emptyRequest)
    if (!playerName || disabled) {
      setQuality(null)
      setQualityError('')
      setQualityLoading(false)
      return undefined
    }

    const controller = new AbortController()
    setQualityLoading(true)
    setQualityError('')
    fetchQualityCalendar({ playerName, signal: controller.signal, bypassCache: true })
      .then((payload) => {
        if (!controller.signal.aborted) setQuality(payload)
      })
      .catch((requestError) => {
        if (requestError.name !== 'AbortError') {
          setQualityError(requestError.message || 'Unable to load Stockfish measures.')
        }
      })
      .finally(() => {
        if (!controller.signal.aborted) setQualityLoading(false)
      })
    return () => {
      controller.abort()
      hourController.current?.abort()
      gameController.current?.abort()
    }
  }, [playerName, disabled])

  const loadGame = async (requestedGameId = gameId) => {
    const normalizedGameId = String(requestedGameId || '').replace(/\D/g, '')
    if (!normalizedGameId || !playerName) return
    gameController.current?.abort()
    const controller = new AbortController()
    gameController.current = controller
    setGameId(normalizedGameId)
    setGameRequest((current) => ({ ...current, error: '', loading: true }))
    try {
      const data = await fetchGameMeasures({
        playerName,
        gameId: normalizedGameId,
        signal: controller.signal,
      })
      if (!controller.signal.aborted) setGameRequest({ data, error: '', loading: false })
    } catch (requestError) {
      if (requestError.name !== 'AbortError') {
        setGameRequest({ data: null, error: requestError.message || 'Unable to load this game.', loading: false })
      }
    }
  }

  const loadHour = async ({ cursor = null } = {}) => {
    if (!date || !playerName) return
    hourController.current?.abort()
    const controller = new AbortController()
    hourController.current = controller
    setHourRequest((current) => ({ ...current, error: '', loading: true }))
    try {
      const data = await fetchHourMeasures({
        playerName,
        date,
        hour,
        mode: hourMode,
        cursor,
        limitGames: 20,
        signal: controller.signal,
      })
      if (controller.signal.aborted) return
      setHourRequest((current) => ({
        data: cursor && current.data
          ? { ...data, games: [...current.data.games, ...data.games] }
          : data,
        error: '',
        loading: false,
      }))
    } catch (requestError) {
      if (requestError.name !== 'AbortError') {
        setHourRequest((current) => ({
          ...current,
          error: requestError.message || 'Unable to load this hour.',
          loading: false,
        }))
      }
    }
  }

  return (
    <section className="games-mode-detail player-measures-workspace">
      <div className="player-analysis-heading">
        <div>
          <p className="eyebrow">STOCKFISH ANALYSIS</p>
          <h2>Position quality</h2>
        </div>
      </div>

      {qualityError ? <p className="player-analysis-error" role="alert">{qualityError}</p> : null}
      {qualityLoading ? <div className="player-analysis-loading" aria-live="polite"><span />Calculating engine measures…</div> : null}
      {!qualityLoading && !qualityError && quality ? (
        <PlayerMeasuresPanel
          playerName={playerName}
          quality={quality}
          gameInspector={{
            ...gameRequest,
            gameId,
            setGameId: (value) => setGameId(value.replace(/\D/g, '')),
            load: () => loadGame(),
            selectGame: (selectedGameId) => loadGame(selectedGameId),
          }}
          hourInspector={{
            ...hourRequest,
            date,
            setDate,
            hour,
            setHour,
            mode: hourMode,
            setMode: setHourMode,
            load: () => loadHour(),
            loadMore: () => loadHour({ cursor: hourRequest.data?.pagination?.next_cursor }),
          }}
        />
      ) : null}
    </section>
  )
}
