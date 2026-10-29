import { useCallback, useEffect, useState } from 'react'
import { pageStyleTarget as style } from '../appearance/styleCore'
import Header from '../components/layout/Header'
import SideRail from '../components/layout/SideRail'
import PlayerGameReviewModal from './players/PlayerGameReviewModal'
import SalienceComparisonReview from './research/SalienceComparisonReview'
import { fetchComparisons, fetchComparisonGames, fetchComparisonGame } from './research/salienceComparisonApi'
import './research/salienceComparison.css'
import './players/player-game-explorer.css'

const count = value => Number(value).toLocaleString('en-US')
const score = value => Number(value).toFixed(4)
const difference = value => `${value > 0 ? '+' : ''}${score(value)}`
const date = value => value ? new Date(value).toLocaleString() : 'Unknown date'

function Distribution({ variant, maximum }) {
  return <article {...style(`variant.${variant.id}.panel`)} className="salience-variant">
    <h2 {...style(`variant.${variant.id}.title`)}>{variant.label}</h2>
    <div {...style(`variant.${variant.id}.summary`)} className="salience-variant-summary">
      <strong>Median {score(variant.median)}</strong><span>Mean {score(variant.mean)}</span>
      <span>{count(variant.at_least_075)} games ≥ 0.75</span>
    </div>
    <svg {...style(`variant.${variant.id}.chart`)} viewBox="0 0 320 145" role="img"
      aria-label={`${variant.label}: distribution on raw salience 0 to 1`}>
      <text {...style(`variant.${variant.id}.axis`)} x="2" y="13">{count(maximum)} games</text>
      {variant.histogram.map((n, index) => <rect {...style(`variant.${variant.id}.bar`)} key={index}
        x={25 + index * 28} y={119 - n / maximum * 95} width="24" height={n / maximum * 95}>
        <title>{`${(index/10).toFixed(1)}–${((index+1)/10).toFixed(1)}${index===9 ? ' inclusive' : ' (upper excluded)'}: ${count(n)} games`}</title>
      </rect>)}
      {[0, .5, 1].map(value => <text {...style(`variant.${variant.id}.axis`)} key={value}
        x={25 + value * 280} y="138" textAnchor="middle">{value.toFixed(1)}</text>)}
    </svg>
  </article>
}

export function SalienceComparisonContent() {
  const [reports, setReports] = useState([])
  const [id, setId] = useState('')
  const [error, setError] = useState('')
  const [initialLoading, setInitialLoading] = useState(true)
  const [loading, setLoading] = useState(false)
  const [filters, setFilters] = useState({sort:'difference',direction:'desc',mode:'all',offset:0,limit:25})
  const [page, setPage] = useState({games:[],total:0})
  const [selected, setSelected] = useState(null)
  const [highlight, setHighlight] = useState(0)
  const [revision, setRevision] = useState(0)
  const snapshot = reports.find(report => report.id === id)
  useEffect(() => {
    const controller = new AbortController()
    fetchComparisons(controller.signal).then(payload => {
      if (controller.signal.aborted) return
      const saved = payload.comparisons.filter(report => report.player === 'lafareto')
      setReports(saved); setId(saved[0]?.id || '')
    }).catch(e => { if (!controller.signal.aborted) setError(e.message) })
      .finally(() => { if (!controller.signal.aborted) setInitialLoading(false) })
    return () => controller.abort()
  }, [])
  useEffect(() => {
    if (!id) return undefined
    const controller = new AbortController()
    setLoading(true); setError('')
    fetchComparisonGames(id, filters, controller.signal).then(payload => {
      if (!controller.signal.aborted) { setPage(payload); setHighlight(0) }
    }).catch(e => { if (!controller.signal.aborted) setError(e.message) })
      .finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [id, filters, revision])
  const change = (key, value) => setFilters(previous => ({...previous,[key]:value,offset:0}))
  const loadGame = useCallback(({gameId,signal}) => fetchComparisonGame(id,gameId,signal), [id])
  const maximum = Math.max(1,...(snapshot?.variants || []).flatMap(v => v.histogram))
  const keyDown = event => {
    if (loading || !['ArrowUp','ArrowDown'].includes(event.key)) return
    event.preventDefault()
    const next = Math.max(0,Math.min(page.games.length-1,highlight+(event.key==='ArrowDown'?1:-1)))
    setHighlight(next)
    event.currentTarget.querySelectorAll('[data-game-button]')[next]?.focus()
  }
  return <main {...style('content')} className="salience-comparison-main">
    <header {...style('intro.panel')} className="salience-comparison-intro">
      <a {...style('intro.back')} href="/research">← Research</a>
      <h1 {...style('intro.title')}>Salience comparison · lafareto</h1>
      <p {...style('intro.text')}>Experimental results only. Official scores are unchanged. Raw game salience, not daily averages or a relative slider.</p>
      {reports.length>1 && <label>Snapshot <select {...style('snapshot.field')} value={id}
        onChange={e => { setId(e.target.value); setSelected(null); setFilters(p => ({...p,offset:0})) }}>
        {reports.map(r => <option key={r.id} value={r.id}>{date(r.created_at)}</option>)}</select></label>}
      {snapshot && <p {...style('snapshot.summary')}>
        {count(snapshot.games)} games · {count(snapshot.positions)} position occurrences · Snapshot {date(snapshot.created_at)}
        <br />{count(snapshot.short_games)} games shorter than 15 full moves; no padding. Modes pooled for frequency counts; White/Black histories separate.
        <br />Current-formula check: {count(snapshot.baseline_check.compared_games)} official scores compared;
        {' '}maximum difference {Number(snapshot.baseline_check.max_absolute_difference || 0).toExponential(2)}.
      </p>}
    </header>
    {initialLoading && <p role="status">Loading saved comparison…</p>}
    {!initialLoading && !id && !error && <p>No saved lafareto comparison yet. Preparing a snapshot is a separate research task; opening this page never starts calculations.</p>}
    {error && <p {...style('error')} role="alert">{error}</p>}
    {snapshot && <>
      <section {...style('distributions.panel')} className="salience-comparison-distributions" aria-label="Comparison distributions">
        {snapshot.variants.map(variant => <Distribution key={variant.id} variant={variant} maximum={maximum} />)}
      </section>
      <section {...style('games.panel')} className="salience-comparison-games" aria-label="Game comparison">
        <div {...style('games.filters')} className="salience-comparison-filters">
          <label>Sort <select {...style('games.sort')} value={filters.sort} onChange={e => change('sort',e.target.value)}>
            <option value="difference">Largest change</option><option value="date">Date</option>
            <option value="current">Current score</option><option value="equal">Whole game · equal</option>
            <option value="opening">First 15 moves · equal</option><option value="delta_equal">Whole-game difference</option>
            <option value="delta_opening">First-15 difference</option></select></label>
          <label>Order <select {...style('games.direction')} value={filters.direction} onChange={e => change('direction',e.target.value)}>
            <option value="desc">Descending</option><option value="asc">Ascending</option></select></label>
          <label>Mode <select {...style('games.mode')} value={filters.mode} onChange={e => change('mode',e.target.value)}>
            {['all','bullet','blitz','rapid'].map(mode => <option key={mode} value={mode}>{mode}</option>)}</select></label>
          <span {...style('games.count')} role="status">{loading ? 'Loading saved games…' : `${count(page.total)} games`}</span>
        </div>
        <div className="salience-table-scroll" onKeyDown={keyDown} aria-busy={loading}>
          <table {...style('games.table')} className="salience-comparison-table"><thead {...style('games.head')}><tr>
            {['Game / date','Opponent','Mode / result','Current','Equal · all','Δ all','Equal · first 15','Δ first 15','Review'].map(t => <th key={t}>{t}</th>)}
          </tr></thead><tbody>{page.games.map((game,index) => <tr {...style('games.row')} key={game.game_link}
            data-highlighted={highlight===index} onMouseEnter={() => setHighlight(index)}>
            <td><button {...style('games.open')} data-game-button type="button" disabled={loading}
              onFocus={() => setHighlight(index)} onClick={() => setSelected(game.game_link)}>
              {date(game.played_at)}<small>{game.is_white ? 'White' : 'Black'} · {game.positions} half-moves</small></button></td>
            <td>{game.opponent}</td><td>{game.mode} · {game.result===1?'Win':game.result===.5?'Draw':'Loss'}</td>
            <td>{score(game.current)}</td><td>{score(game.equal)}</td><td>{difference(game.delta_equal)}</td>
            <td>{score(game.opening)}</td><td>{difference(game.delta_opening)}</td><td>{game.review?.judgement || 'Not reviewed'}</td>
          </tr>)}</tbody></table>
        </div>
        <nav {...style('games.pagination')} className="salience-comparison-pagination" aria-label="Game pages">
          <button {...style('games.previous')} disabled={loading || filters.offset===0} onClick={() => setFilters(p => ({...p,offset:Math.max(0,p.offset-p.limit)}))}>Previous</button>
          <span>{page.total ? `${filters.offset+1}–${Math.min(filters.offset+filters.limit,page.total)} of ${count(page.total)}` : 'No games'}</span>
          <button {...style('games.next')} disabled={loading || filters.offset+filters.limit>=page.total} onClick={() => setFilters(p => ({...p,offset:p.offset+p.limit}))}>Next</button>
        </nav>
      </section>
    </>}
    {selected && <PlayerGameReviewModal gameId={selected} playerName="lafareto" onClose={() => setSelected(null)} loadGame={loadGame}
      renderComparison={({data,positionIndex}) => <SalienceComparisonReview key={`${id}:${selected}`} snapshot={id} gameId={selected}
        data={data} positionIndex={positionIndex} onSaved={() => setRevision(v => v+1)} />} />}
  </main>
}

export default function SalienceComparison() {
  return <div {...style('page')} className="page-frame"><SideRail /><div className="home-shell"><Header /><SalienceComparisonContent /></div></div>
}
