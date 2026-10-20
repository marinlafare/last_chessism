import { useEffect, useRef, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import { fetchPlayerGameCatalog } from './playerAnalysisApi'
import { useAnalysisVisible } from './PlayerAnalysisDisclosure'
import PlayerGameExplorerModal from './PlayerGameExplorerModal'
import PlayerGameReviewModal from './PlayerGameReviewModal'
import './player-game-catalog.css'

const modes = ['bullet', 'blitz', 'rapid']
const results = [['wins', 'Wins'], ['draws', 'Draws'], ['losses', 'Losses'], ['unknown', 'Unknown']]
const openingLabel = key => key ? key.replaceAll('-', ' ') : 'Unknown opening'

export default function PlayerGameCatalog({ playerName, kind }) {
  const visible = useAnalysisVisible()
  const openings = kind === 'openings'
  const target = `player.${kind}`
  const [filters, setFilters] = useState({ mode: 'all', color: openings ? 'white' : 'all', page: 1, search: '' })
  const [searchText, setSearchText] = useState('')
  const [request, setRequest] = useState({ key: '', data: null, error: '', loading: false })
  const [retry, setRetry] = useState(0)
  const [selection, setSelection] = useState(null)
  const [reviewGameId, setReviewGameId] = useState(null)
  const cache = useRef(new Map())
  const requestKey = JSON.stringify([playerName, kind, filters])
  const data = request.key === requestKey ? request.data : null
  const error = request.key === requestKey ? request.error : ''
  const searchPending = searchText.trim() !== filters.search
  const loading = searchPending || request.key !== requestKey || request.loading
  const pagination = data?.pagination

  useEffect(() => {
    if (!visible || !searchPending) return undefined
    const timer = window.setTimeout(() => {
      setSelection(null)
      setReviewGameId(null)
      setFilters(current => ({ ...current, search: searchText.trim(), page: 1 }))
    }, 300)
    return () => window.clearTimeout(timer)
  }, [searchText, searchPending, visible])

  useEffect(() => {
    if (!visible || searchPending) return undefined
    const cached = cache.current.get(requestKey)
    if (cached && cached.until > Date.now()) {
      setRequest({ key: requestKey, data: cached.data, error: '', loading: false })
      return undefined
    }
    const controller = new AbortController()
    setRequest(current => ({
      key: requestKey, data: current.key === requestKey ? current.data : null, error: '', loading: true,
    }))
    const timeout = window.setTimeout(() => {
      controller.abort()
      setRequest(current => ({ ...current, loading: false, error: 'Game summary took too long. Please retry.' }))
    }, 20000)
    fetchPlayerGameCatalog({ playerName, kind, ...filters, signal: controller.signal })
      .then(payload => {
        if (controller.signal.aborted) return
        cache.current.set(requestKey, { data: payload, until: Date.now() + 60000 })
        if (cache.current.size > 20) cache.current.delete(cache.current.keys().next().value)
        setRequest({ key: requestKey, data: payload, error: '', loading: false })
      })
      .catch(failure => {
        if (!controller.signal.aborted) setRequest(current => ({
          ...current, loading: false, error: failure.message || 'Unable to load game summary.',
        }))
      })
      .finally(() => window.clearTimeout(timeout))
    return () => { window.clearTimeout(timeout); controller.abort() }
  }, [playerName, kind, filters, requestKey, retry, visible, searchPending])

  useEffect(() => {
    if (!visible) { setSelection(null); setReviewGameId(null) }
  }, [visible])

  const changeFilter = values => {
    setSelection(null)
    setReviewGameId(null)
    setFilters(current => ({ ...current, ...values, page: 1 }))
  }
  const openGames = row => {
    setSelection({
      scope: openings ? 'opening' : 'opponent',
      ...(openings ? { opening: row.key, color: filters.color } : { opponent: row.key }),
      modes: filters.mode === 'all' ? modes : [filters.mode],
      label: openings ? openingLabel(row.key) : row.key,
      analyzed_only: false,
    })
  }
  const refresh = () => { cache.current.delete(requestKey); setRetry(value => value + 1) }

  return <section {...pageStyleTarget(`${target}.panel`)} className="player-game-catalog" aria-busy={loading}>
    <div className="player-game-catalog-toolbar">
      <span {...pageStyleTarget(`${target}.text`)}>
        {data ? `${formatNumber(pagination.total_groups)} ${openings ? 'openings' : 'opponents'} · ${formatNumber(data.total_games)} games` : ''}
      </span>
      <div className="player-game-catalog-filters">
        <input {...pageStyleTarget(`${target}.search`)} className="player-game-catalog-search"
          type="search" maxLength={128} value={searchText}
          aria-label={openings ? 'Search openings' : 'Search opponents'}
          placeholder={openings ? 'Search openings…' : 'Search opponents…'}
          onChange={event => setSearchText(event.target.value)} />
        {openings ? <div className="player-game-catalog-sides" aria-label="Player color">
          {['white', 'black'].map(color => <button {...pageStyleTarget(`${target}.side`)}
            type="button" key={color} aria-pressed={filters.color === color}
            onClick={() => changeFilter({ color })}>
            <i className={`player-game-catalog-color ${color}`} aria-hidden="true" />
            {color === 'white' ? 'White' : 'Black'}
          </button>)}
        </div> : null}
        <select {...pageStyleTarget(`${target}.filter`)} aria-label={`${openings ? 'Opening' : 'Opponent'} game mode`}
          value={filters.mode} onChange={event => changeFilter({ mode: event.target.value })}>
          <option value="all">All modes</option>
          {modes.map(mode => <option key={mode} value={mode}>{mode.charAt(0).toUpperCase() + mode.slice(1)}</option>)}
        </select>
      </div>
    </div>
    <div className="player-game-catalog-legend" aria-label="Recorded game results from this player's perspective">
      {results.slice(0, 3).map(([key, label]) => <span key={key}><i className={key} />{label}</span>)}
    </div>
    {error ? <p role="alert" className="player-game-catalog-message">{error}{' '}
      <button {...pageStyleTarget(`${target}.paging`)} type="button" onClick={refresh}>Retry</button>
    </p> : null}
    {loading && !data ? <p className="player-game-catalog-message" role="status">Loading {openings ? 'openings' : 'opponents'}…</p> : null}
    {data && !data.rows.length ? <p className="player-game-catalog-message">No {openings ? 'openings' : 'opponents'} {filters.search ? 'match this search.' : 'in this selection.'}</p> : null}
    {data ? <div className="player-game-catalog-rows">
      {data.rows.map(row => {
        const label = openings ? openingLabel(row.key) : row.key || 'Unknown opponent'
        const description = `${label}: ${formatNumber(row.games)} games; ${results.map(([key, name]) => `${formatNumber(row[key])} ${name.toLowerCase()}`).join(', ')}`
        return <button {...pageStyleTarget(`${target}.row`)} type="button" key={row.key}
          className="player-game-catalog-row" disabled={loading || (!openings && !row.key)}
          onClick={() => openGames(row)} aria-label={`${description}. Browse games.`}>
          <span {...pageStyleTarget(`${target}.rank`)} className="player-game-catalog-rank"
            title="Rank by games in the selected mode and color">#{formatNumber(row.rank)}</span>
          <span className="player-game-catalog-name" title={label}>{label}</span>
          <span className="player-game-catalog-results" title={description} aria-hidden="true">
            <span className="player-game-catalog-result-bar">
              {results.map(([key, name]) => row[key] > 0 ? <i key={key} className={key}
                style={{ width: `${100 * row[key] / row.games}%` }}
                title={`${name}: ${formatNumber(row[key])} · ${(100 * row[key] / row.games).toFixed(1)}%`} /> : null)}
            </span>
            <span className="player-game-catalog-result-counts">
              {results.map(([key, name]) => key !== 'unknown' || row.unknown > 0
                ? <span key={key} title={name}>{formatNumber(row[key])}{key === 'unknown' ? ' ?' : ` ${name[0]}`}</span> : null)}
            </span>
          </span>
          <span {...pageStyleTarget(`${target}.games`)} className="player-game-catalog-games"
            title={openings ? 'Games with this opening' : 'Games against this opponent'}>{formatNumber(row.games)}</span>
        </button>
      })}
    </div> : null}
    {pagination?.total_pages > 1 || filters.page > 1 ? <nav className="player-game-catalog-pagination" aria-label={`${kind} pages`}>
      <button {...pageStyleTarget(`${target}.paging`)} type="button" aria-label="Previous ten"
        disabled={loading || filters.page <= 1} onClick={() => setFilters(current => ({ ...current, page: current.page - 1 }))}>‹</button>
      <span {...pageStyleTarget(`${target}.text`)}>{filters.page} / {pagination ? Math.max(1, pagination.total_pages) : '…'}</span>
      <button {...pageStyleTarget(`${target}.paging`)} type="button" aria-label="Next ten"
        disabled={loading || !pagination?.has_more} onClick={() => setFilters(current => ({ ...current, page: current.page + 1 }))}>›</button>
    </nav> : null}
    {!reviewGameId && selection ? <PlayerGameExplorerModal playerName={playerName} selection={selection}
      onClose={() => setSelection(null)} onOpenGame={setReviewGameId} /> : null}
    <PlayerGameReviewModal gameId={reviewGameId} playerName={playerName}
      onBack={() => setReviewGameId(null)} onClose={() => { setReviewGameId(null); setSelection(null) }} />
  </section>
}
