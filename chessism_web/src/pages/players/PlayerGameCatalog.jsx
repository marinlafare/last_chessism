import { useCallback, useEffect, useRef, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'
import { formatNumber } from '../../utils/formatters'
import { fetchPlayerGameCatalog } from './playerAnalysisApi'
import { useAnalysisVisible } from './PlayerAnalysisDisclosure'
import PlayerGameExplorerModal from './PlayerGameExplorerModal'
import PlayerGameReviewModal from './PlayerGameReviewModal'
import './player-game-catalog.css'

const modes = ['bullet', 'blitz', 'rapid']
const ratingModeLabels = { bullet: 'Bu', blitz: 'Bl', rapid: 'Ra' }
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
  const listRef = useRef(null)
  const paginationSentinel = useRef(null)
  const paginationController = useRef(null)
  const dataSource = useRef(null)
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
    const controller = new AbortController()
    let timeout
    const cleanup = () => {
      window.clearTimeout(timeout)
      controller.abort()
      paginationController.current?.abort()
      paginationController.current = null
      dataSource.current = null
    }
    if (listRef.current) listRef.current.scrollTop = 0
    const cached = cache.current.get(requestKey)
    if (cached && cached.until > Date.now()) {
      dataSource.current = requestKey
      setRequest({ key: requestKey, data: cached.data, error: '', loading: false })
      return cleanup
    }
    dataSource.current = null
    setRequest(current => ({
      key: requestKey, data: current.key === requestKey ? current.data : null, error: '', loading: true,
    }))
    timeout = window.setTimeout(() => {
      controller.abort()
      setRequest(current => ({ ...current, loading: false, error: 'Game summary took too long. Please retry.' }))
    }, 20000)
    fetchPlayerGameCatalog({ playerName, kind, ...filters, signal: controller.signal })
      .then(payload => {
        if (controller.signal.aborted) return
        cache.current.set(requestKey, { data: payload, until: Date.now() + 60000 })
        if (cache.current.size > 20) cache.current.delete(cache.current.keys().next().value)
        dataSource.current = requestKey
        setRequest({ key: requestKey, data: payload, error: '', loading: false })
      })
      .catch(failure => {
        if (!controller.signal.aborted) setRequest(current => ({
          ...current, loading: false, error: failure.message || 'Unable to load game summary.',
        }))
      })
      .finally(() => window.clearTimeout(timeout))
    return cleanup
  }, [playerName, kind, filters, requestKey, retry, visible, searchPending])

  const loadMore = useCallback(async () => {
    if (openings || !visible || loading || !pagination?.has_more || request.loadingMore
      || paginationController.current || dataSource.current !== requestKey || selection || reviewGameId) return
    const page = pagination.page + 1
    const nextKey = JSON.stringify([playerName, kind, { ...filters, page }])
    const controller = new AbortController()
    // Lock synchronously: several observer notifications must request just one page.
    paginationController.current = controller
    setRequest(current => ({ ...current, loadingMore: true, moreError: '' }))
    const timeout = window.setTimeout(() => {
      controller.abort()
      if (paginationController.current === controller) {
        paginationController.current = null
        setRequest(current => ({ ...current, loadingMore: false, moreError: 'Loading more opponents took too long. Please retry.' }))
      }
    }, 20000)
    try {
      const cached = cache.current.get(nextKey)
      const next = cached && cached.until > Date.now() ? cached.data
        : await fetchPlayerGameCatalog({ playerName, kind, ...filters, page, signal: controller.signal })
      if (controller.signal.aborted) return
      cache.current.set(nextKey, { data: next, until: Date.now() + 60000 })
      if (cache.current.size > 20) cache.current.delete(cache.current.keys().next().value)
      setRequest(current => current.key !== requestKey ? current : ({
        ...current,
        data: {
          ...next,
          rows: [...new Map([...(current.data?.rows || []), ...next.rows].map(row => [row.key, row])).values()],
        },
        loadingMore: false,
        moreError: '',
      }))
    } catch (failure) {
      if (!controller.signal.aborted) setRequest(current => ({
        ...current, loadingMore: false, moreError: failure.message || 'Unable to load more opponents.',
      }))
    } finally {
      window.clearTimeout(timeout)
      if (paginationController.current === controller) paginationController.current = null
    }
  }, [openings, visible, loading, pagination, request.loadingMore, requestKey, playerName, kind, filters, selection, reviewGameId])

  useEffect(() => {
    const list = listRef.current
    const sentinel = paginationSentinel.current
    if (openings || !visible || !list || !sentinel || !pagination?.has_more || loading
      || request.loadingMore || request.moreError || error || selection || reviewGameId) return undefined
    let active = true
    const observer = new IntersectionObserver(entries => {
      if (active && entries.some(entry => entry.isIntersecting)) loadMore()
    }, { root: list, rootMargin: '0px 0px 48px 0px' })
    observer.observe(sentinel)
    return () => { active = false; observer.disconnect() }
  }, [loadMore, openings, visible, pagination?.has_more, loading, request.loadingMore, request.moreError, error, selection, reviewGameId])

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
  const totals = <span {...pageStyleTarget(`${target}.text`)} className="player-game-catalog-totals">
    {data ? `${formatNumber(pagination.total_groups)} ${openings ? 'openings' : 'opponents'} · ${formatNumber(data.total_games)} games` : ''}
  </span>

  return <section {...pageStyleTarget(`${target}.panel`)} className={`player-game-catalog${openings ? '' : ' is-opponents'}`} aria-busy={loading}>
    <div className="player-game-catalog-toolbar">
      {openings ? totals : null}
      <div className="player-game-catalog-filters">
        <input {...pageStyleTarget(`${target}.search`)} className="player-game-catalog-search"
          type="search" maxLength={128} value={searchText}
          aria-label={openings ? 'Search openings' : 'Search opponents'}
          placeholder={openings ? 'Search openings…' : 'Search opponents…'}
          onChange={event => setSearchText(event.target.value)} />
        {!openings ? totals : null}
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
          <option value="all">{openings ? 'All modes' : 'All'}</option>
          {modes.map(mode => <option key={mode} value={mode}>{mode.charAt(0).toUpperCase() + mode.slice(1)}</option>)}
        </select>
      </div>
    </div>
    {openings ? <div className="player-game-catalog-legend" aria-label="Recorded game results from this player's perspective">
      {results.slice(0, 3).map(([key, label]) => <span key={key}><i className={key} />{label}</span>)}
    </div> : null}
    {error ? <p role="alert" className="player-game-catalog-message">{error}{' '}
      <button {...pageStyleTarget(`${target}.paging`)} type="button" onClick={refresh}>Retry</button>
    </p> : null}
    {loading && !data ? <p className="player-game-catalog-message" role="status">Loading {openings ? 'openings' : 'opponents'}…</p> : null}
    {data && !data.rows.length ? <p className="player-game-catalog-message">No {openings ? 'openings' : 'opponents'} {filters.search ? 'match this search.' : 'in this selection.'}</p> : null}
    {data ? <div key={requestKey} ref={listRef} className="player-game-catalog-rows"
      tabIndex={openings ? undefined : 0} role={openings ? undefined : 'region'}
      aria-label={openings ? undefined : 'Opponents — scroll to load more'}>
      {data.rows.map(row => {
        const label = openings ? openingLabel(row.key) : row.key || 'Unknown opponent'
        const description = `${label}: ${formatNumber(row.games)} games; ${results.map(([key, name]) => `${formatNumber(row[key])} ${name.toLowerCase()}`).join(', ')}`
        // Older API instances omit this field until the prepared backend is deployed.
        const showRatings = !openings && row.latest_ratings != null
        return <button {...(openings ? pageStyleTarget(`${target}.row`) : {})} type="button" key={row.key}
          className="player-game-catalog-row" disabled={loading || (!openings && !row.key)}
          onClick={() => openGames(row)} aria-label={`${description}. Browse games.`}>
          <span {...(!openings ? pageStyleTarget(`${target}.row`) : {})} className={`player-game-catalog-identity${showRatings ? ' has-ratings' : ''}`}>
            <span {...pageStyleTarget(`${target}.rank`)} className="player-game-catalog-rank"
              title="Rank by games in the selected mode and color">#{formatNumber(row.rank)}</span>
            <span className="player-game-catalog-name" title={label}>{label}</span>
            {showRatings ? <span className="player-game-catalog-ratings">
              {(filters.mode === 'all' ? modes : [filters.mode]).map(mode => {
                const rating = row.latest_ratings[mode]
                const available = Number.isInteger(rating) && rating > 0
                const detail = available ? `Latest recorded ${mode} rating: ${rating}` : `No recorded ${mode} rating`
                return <span key={mode} className="player-game-catalog-rating" title={detail} aria-label={detail}>
                  {filters.mode === 'all' ? <small aria-hidden="true">{ratingModeLabels[mode]}</small> : null}
                  <span aria-hidden="true">{available ? rating : '—'}</span>
                </span>
              })}
            </span> : null}
          </span>
          <span {...(!openings ? pageStyleTarget(`${target}.row`) : {})} className="player-game-catalog-statistics">
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
          </span>
        </button>
      })}
      {!openings ? <div ref={paginationSentinel} className="player-game-catalog-more">
        {request.moreError ? <p className="player-game-catalog-message" role="alert">
          {request.moreError}{' '}
          <button {...pageStyleTarget(`${target}.paging`)} type="button" onClick={loadMore} disabled={loading}>Retry</button>
        </p> : <span role="status">{request.loadingMore ? 'Loading more opponents…' : ''}</span>}
      </div> : null}
    </div> : null}
    {openings && (pagination?.total_pages > 1 || filters.page > 1) ? <nav className="player-game-catalog-pagination" aria-label={`${kind} pages`}>
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
