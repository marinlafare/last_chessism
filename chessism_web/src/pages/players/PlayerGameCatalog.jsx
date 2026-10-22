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
  const [highlightedKey, setHighlightedKey] = useState(null)
  const cache = useRef(new Map())
  const listRef = useRef(null)
  const paginationSentinel = useRef(null)
  const paginationController = useRef(null)
  const dataSource = useRef(null)
  const lastPointer = useRef(null)
  const requestKey = JSON.stringify([playerName, kind, filters])
  const data = request.key === requestKey ? request.data : null
  const error = request.key === requestKey ? request.error : ''
  const searchPending = searchText.trim() !== filters.search
  const loading = searchPending || request.key !== requestKey || request.loading
  const pagination = data?.pagination

  useEffect(() => { setHighlightedKey(null) }, [requestKey])

  useEffect(() => {
    const list = listRef.current
    if (!visible || loading || !data?.rows.length || !list || selection || reviewGameId) return
    const region = list.closest('.player-analysis-disclosure-content')
    // Opening a disclosure leaves focus on its button. Transfer it once rows
    // arrive, but never steal focus from search, filters, or another section.
    if (document.activeElement?.id === region?.getAttribute('aria-labelledby')) {
      list.focus({ preventScroll: true })
    }
  }, [visible, loading, data?.rows.length, selection, reviewGameId])

  const highlightPointerRow = (event, rowKey) => {
    const coordinates = `${event.clientX}:${event.clientY}`
    // Scrolling the list under a stationary mouse must not undo arrow selection.
    if (lastPointer.current === coordinates) return
    lastPointer.current = coordinates
    setHighlightedKey(rowKey)
    const focused = document.activeElement
    const editing = focused?.matches('input,textarea,select,[contenteditable="true"]')
    if (!editing && !selection && !reviewGameId) event.currentTarget.focus({ preventScroll: true })
  }

  const handleListKey = event => {
    if (event.target !== listRef.current && !event.target.closest('.player-game-catalog-row')) return
    if (!['ArrowUp', 'ArrowDown', 'Home', 'End', 'Enter', ' '].includes(event.key)) return
    const rows = [...listRef.current.querySelectorAll('.player-game-catalog-row:not(:disabled)')]
    if (!rows.length) return
    event.preventDefault()
    event.stopPropagation()
    const current = rows.findIndex(row => row.dataset.catalogKey === highlightedKey)
    if (event.key === 'Enter' || event.key === ' ') {
      if (current >= 0) rows[current].click()
      return
    }
    const next = event.key === 'Home' ? 0 : event.key === 'End' ? rows.length - 1
      : current < 0 ? (event.key === 'ArrowUp' ? rows.length - 1 : 0)
      : Math.min(rows.length - 1, Math.max(0, current + (event.key === 'ArrowDown' ? 1 : -1)))
    setHighlightedKey(rows[next].dataset.catalogKey)
    rows[next].focus({ preventScroll: true })
    // Scroll only the results viewport, never the entire document.
    const viewport = listRef.current.getBoundingClientRect()
    const bounds = rows[next].getBoundingClientRect()
    if (bounds.top < viewport.top) listRef.current.scrollTop -= viewport.top - bounds.top
    else if (bounds.bottom > viewport.bottom) listRef.current.scrollTop += bounds.bottom - viewport.bottom
    if (event.key === 'ArrowDown' && next === rows.length - 1) loadMore()
  }

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
    if (!visible || loading || !pagination?.has_more || request.loadingMore
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
        setRequest(current => ({ ...current, loadingMore: false, moreError: `Loading more ${kind} took too long. Please retry.` }))
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
        ...current, loadingMore: false, moreError: failure.message || `Unable to load more ${kind}.`,
      }))
    } finally {
      window.clearTimeout(timeout)
      if (paginationController.current === controller) paginationController.current = null
    }
  }, [visible, loading, pagination, request.loadingMore, requestKey, playerName, kind, filters, selection, reviewGameId])

  useEffect(() => {
    const list = listRef.current
    const sentinel = paginationSentinel.current
    if (!visible || !list || !sentinel || !pagination?.has_more || loading
      || request.loadingMore || request.moreError || error || selection || reviewGameId) return undefined
    let active = true
    const observer = new IntersectionObserver(entries => {
      if (active && entries.some(entry => entry.isIntersecting)) loadMore()
    }, { root: list, rootMargin: '0px 0px 48px 0px' })
    observer.observe(sentinel)
    return () => { active = false; observer.disconnect() }
  }, [loadMore, visible, pagination?.has_more, loading, request.loadingMore, request.moreError, error, selection, reviewGameId])

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

  return <section {...pageStyleTarget(`${target}.panel`)} className={`player-game-catalog is-compact ${openings ? 'is-openings' : 'is-opponents'}`} aria-busy={loading}>
    <div className="player-game-catalog-toolbar">
      <div className="player-game-catalog-filters">
        <input {...pageStyleTarget(`${target}.search`)} className="player-game-catalog-search"
          type="search" maxLength={128} value={searchText}
          aria-label={openings ? 'Search openings' : 'Search opponents'}
          placeholder={openings ? 'Search openings…' : 'Search opponents…'}
          onChange={event => setSearchText(event.target.value)} />
        {totals}
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
          <option value="all">All</option>
          {modes.map(mode => <option key={mode} value={mode}>{mode.charAt(0).toUpperCase() + mode.slice(1)}</option>)}
        </select>
      </div>
    </div>
    {error ? <p role="alert" className="player-game-catalog-message">{error}{' '}
      <button {...pageStyleTarget(`${target}.paging`)} type="button" onClick={refresh}>Retry</button>
    </p> : null}
    {loading && !data ? <p className="player-game-catalog-message" role="status">Loading {openings ? 'openings' : 'opponents'}…</p> : null}
    {data && !data.rows.length ? <p className="player-game-catalog-message">No {openings ? 'openings' : 'opponents'} {filters.search ? 'match this search.' : 'in this selection.'}</p> : null}
    {data ? <div key={requestKey} ref={listRef} className="player-game-catalog-rows"
      tabIndex={0} role="region" onKeyDown={handleListKey}
      aria-label={`${openings ? 'Openings' : 'Opponents'} — scroll to load more`}>
      {data.rows.map(row => {
        const rowKey = String(row.key ?? '')
        const highlighted = highlightedKey === rowKey
        const label = openings ? openingLabel(row.key) : row.key || 'Unknown opponent'
        const description = `${label}: ${formatNumber(row.games)} games; ${results.map(([key, name]) => `${formatNumber(row[key])} ${name.toLowerCase()}`).join(', ')}`
        // Older API instances omit this field until the prepared backend is deployed.
        const showRatings = !openings && row.latest_ratings != null
        return <button type="button" key={row.key}
          className={`player-game-catalog-row${highlighted ? ' is-highlighted' : ''}`}
          data-catalog-key={rowKey} tabIndex={highlighted ? 0 : -1}
          disabled={loading || (!openings && !row.key)}
          onPointerMove={event => highlightPointerRow(event, rowKey)}
          onFocus={() => setHighlightedKey(rowKey)}
          onClick={() => openGames(row)} aria-label={`${description}. Browse games.`}>
          <span {...pageStyleTarget(`${target}.row`)} data-active={highlighted ? 'true' : undefined} className={`player-game-catalog-identity${showRatings ? ' has-ratings' : ''}`}>
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
          <span {...pageStyleTarget(`${target}.row`)} data-active={highlighted ? 'true' : undefined} className="player-game-catalog-statistics">
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
      <div ref={paginationSentinel} className="player-game-catalog-more">
        {request.moreError ? <p className="player-game-catalog-message" role="alert">
          {request.moreError}{' '}
          <button {...pageStyleTarget(`${target}.paging`)} type="button" onClick={loadMore} disabled={loading}>Retry</button>
        </p> : <span role="status">{request.loadingMore ? `Loading more ${kind}…` : ''}</span>}
      </div>
    </div> : null}
    {!reviewGameId && selection ? <PlayerGameExplorerModal playerName={playerName} selection={selection}
      onClose={() => setSelection(null)} onOpenGame={setReviewGameId} /> : null}
    <PlayerGameReviewModal gameId={reviewGameId} playerName={playerName}
      onBack={() => setReviewGameId(null)} onClose={() => { setReviewGameId(null); setSelection(null) }} />
  </section>
}
