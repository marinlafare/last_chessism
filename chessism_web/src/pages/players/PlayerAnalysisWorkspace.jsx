import { useEffect, useState } from 'react'
import './player-analysis.css'
import './player-hero-analytics.css'
import PlayerBehaviouralPanel from './PlayerBehaviouralPanel'
import {
  fetchBehaviouralActivity,
  fetchBehaviouralDay,
  fetchBehaviouralRatings,
} from './playerAnalysisApi'

const MODES = ['all', 'bullet', 'blitz', 'rapid']

export default function PlayerAnalysisWorkspace({ playerName, disabled = false }) {
  const [mode, setMode] = useState('all')
  const [selectedDate, setSelectedDate] = useState('')
  const [activity, setActivity] = useState(null)
  const [ratings, setRatings] = useState(null)
  const [dayData, setDayData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [dayLoading, setDayLoading] = useState(false)
  const [error, setError] = useState('')
  const [dayError, setDayError] = useState('')

  useEffect(() => {
    setSelectedDate('')
    setDayData(null)
    setDayError('')
  }, [playerName])

  useEffect(() => {
    if (!playerName || disabled) {
      setActivity(null)
      setRatings(null)
      return undefined
    }

    const controller = new AbortController()
    setLoading(true)
    setError('')
    Promise.all([
      fetchBehaviouralActivity({ playerName, mode, signal: controller.signal }),
      fetchBehaviouralRatings({ playerName, mode, signal: controller.signal }),
    ]).then(([nextActivity, nextRatings]) => {
      if (controller.signal.aborted) return
      setActivity(nextActivity)
      setRatings(nextRatings)
    }).catch((requestError) => {
      if (requestError.name !== 'AbortError') {
        setError(requestError.message || 'Unable to load playing patterns.')
      }
    }).finally(() => {
      if (!controller.signal.aborted) setLoading(false)
    })
    return () => controller.abort()
  }, [playerName, disabled, mode])

  useEffect(() => {
    if (!playerName || disabled || !selectedDate) {
      setDayData(null)
      setDayError('')
      setDayLoading(false)
      return undefined
    }

    const controller = new AbortController()
    setDayData(null)
    setDayLoading(true)
    setDayError('')
    fetchBehaviouralDay({
      playerName,
      date: selectedDate,
      mode,
      signal: controller.signal,
    }).then((payload) => {
      if (!controller.signal.aborted) setDayData(payload)
    }).catch((requestError) => {
      if (requestError.name !== 'AbortError') {
        setDayError(requestError.message || 'Unable to load the selected date.')
      }
    }).finally(() => {
      if (!controller.signal.aborted) setDayLoading(false)
    })
    return () => controller.abort()
  }, [playerName, disabled, mode, selectedDate])

  return (
    <section className="games-mode-detail player-analysis-workspace">
      <div className="player-analysis-heading">
        <div>
          <p className="eyebrow">BEHAVIORAL ANALYSIS</p>
          <h2>Playing patterns</h2>
          <p>When this player starts games, how those games end, and how rating changes over time.</p>
        </div>
        <div className="player-behavior-controls">
          <div className="player-analysis-mode-filter" aria-label="Game type">
            {MODES.map((item) => (
              <button
                className={mode === item ? 'active' : ''}
                key={item}
                type="button"
                disabled={disabled}
                onClick={() => setMode(item)}
              >
                {item}
              </button>
            ))}
          </div>
          <label className="player-specific-date">
            <span>Inspect a specific date</span>
            <input
              className="text-input"
              type="date"
              value={selectedDate}
              min={ratings?.date_from || undefined}
              max={ratings?.date_to || undefined}
              disabled={disabled}
              onChange={(event) => setSelectedDate(event.target.value)}
            />
          </label>
          {selectedDate ? (
            <button className="btn btn-secondary btn-inline" type="button" onClick={() => setSelectedDate('')}>
              All dates
            </button>
          ) : null}
        </div>
      </div>

      {error ? <p className="player-analysis-error" role="alert">{error}</p> : null}
      {loading ? <div className="player-analysis-loading" aria-live="polite"><span />Calculating playing patterns…</div> : null}
      {!loading && !error && activity && ratings ? (
        <PlayerBehaviouralPanel
          activity={activity}
          ratings={ratings}
          selectedDate={selectedDate}
          dayData={dayData}
          dayLoading={dayLoading}
          dayError={dayError}
        />
      ) : null}
    </section>
  )
}
