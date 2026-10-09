import { useCallback, useEffect, useMemo, useState } from 'react'

import Footer from '../components/layout/Footer'
import Header from '../components/layout/Header'
import SideRail from '../components/layout/SideRail'
import {
  fetchSalienceOverview,
  rebuildPlayerSalience,
  startSalienceBackfill,
} from './research/salienceApi'
import './research/salienceResearch.css'

const number = new Intl.NumberFormat('en-US')
const decimal = new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 })
const ACTIVE = new Set(['queued', 'running'])

export default function SalienceResearch() {
  const [payload, setPayload] = useState({ players: [], status_counts: {} })
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')

  const refresh = useCallback(async (signal) => {
    try {
      const next = await fetchSalienceOverview(signal)
      if (signal?.aborted) return null
      setPayload(next)
      setError('')
      return next
    } catch (requestError) {
      if (signal?.aborted) return null
      setError(requestError instanceof Error ? requestError.message : 'Could not load salience state.')
      return null
    }
  }, [])

  useEffect(() => {
    const controller = new AbortController()
    let timer
    const poll = async () => {
      const next = await refresh(controller.signal)
      if (controller.signal.aborted) return
      const active = next?.players?.some((item) => ACTIVE.has(item.status))
      timer = window.setTimeout(poll, active ? 4000 : 30000)
    }
    poll()
    return () => {
      controller.abort()
      window.clearTimeout(timer)
    }
  }, [refresh])

  const totals = useMemo(() => {
    const players = payload.players || []
    return {
      tracked: players.length,
      ready: players.filter((item) => item.status === 'ready').length,
      active: players.filter((item) => ACTIVE.has(item.status)).length,
      pending: players.filter((item) => ['missing', 'stale', 'failed'].includes(item.status)).length,
    }
  }, [payload.players])

  const run = async (operation) => {
    setBusy(true)
    setError('')
    try {
      await operation()
      await refresh()
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : 'Request failed.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="salience-main">
          <section className="salience-hero">
            <div>
              <p className="eyebrow">CORPUS PROJECTION</p>
              <h1>Game salience</h1>
              <p>Measure how distinctive every game and move is inside each player&apos;s complete corpus.</p>
            </div>
            <div className="salience-actions">
              <a href="/research">Research</a>
              <button type="button" disabled={busy} onClick={() => run(startSalienceBackfill)}>
                {busy ? 'Queueing…' : 'Build pending players'}
              </button>
            </div>
          </section>

          <section className="salience-stat-grid" aria-label="Salience projection totals">
            {Object.entries(totals).map(([label, value]) => (
              <article key={label}>
                <span>{label}</span>
                <strong>{number.format(value)}</strong>
              </article>
            ))}
          </section>

          {error ? <p className="salience-error">{error}</p> : null}

          <section className="salience-panel">
            <div className="salience-table-head">
              <span>Player</span>
              <span>Status</span>
              <span>Games</span>
              <span>Occurrences</span>
              <span>Effective games</span>
              <span>Action</span>
            </div>
            <div className="salience-player-list">
              {(payload.players || []).map((player) => (
                <article className="salience-player-row" key={player.player_name}>
                  <strong>{player.player_name}</strong>
                  <span className={`salience-status is-${player.status}`}>{player.status}</span>
                  <span>{number.format(player.source_game_count || 0)}</span>
                  <span>{number.format(player.source_position_count || 0)}</span>
                  <span>{decimal.format(player.effective_game_count || 0)}</span>
                  <button
                    type="button"
                    disabled={busy || ACTIVE.has(player.status)}
                    onClick={() => run(() => rebuildPlayerSalience(player.player_name))}
                  >
                    rebuild
                  </button>
                  {player.error ? <p>{player.error}</p> : null}
                </article>
              ))}
            </div>
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}
