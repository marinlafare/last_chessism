import { useRef, useState } from 'react'
import GraphBoard, { graphStyle as s } from './GraphBoard'
import PlayerGameReviewModal from '../players/PlayerGameReviewModal'
import { useGraphRead } from './useGraphRead'
import './graphExplorer.css'
import './plyMap.css'

const num = n => Number(n || 0).toLocaleString()
const initial = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1'
const styles = name => s(`ply.${name}`)
const title = node => node.kind === 'root' ? 'Initial position' : node.kind === 'position'
  ? `${node.label} · ply ${node.ply}` : node.kind === 'ending' ? 'Game endings' : 'Continuations beyond move 15'

function Detail({ snapshot, selected, players, close }) {
  const [offset, setOffset] = useState(0), [review, setReview] = useState(null), [preview, setPreview] = useState(null)
  const request = useGraphRead('ply-map/detail', { snapshot, key: selected.key, players: players.map(p => p.player_id).join(','), offset })
  const data = request.data, first = data?.rows?.[0]?.path?.[0]
  const position = preview || first
  return <section {...styles('detail')} className="ply-map-detail">
    <header><h4 {...styles('detailTitle')}>{selected.source ? selected.label || 'Direct move' : title(selected)}</h4>
      <button {...styles('detailClose')} onClick={close} aria-label="Close selected path">×</button></header>
    {request.loading && <p {...styles('status')} role="status">Loading captured games…</p>}
    {request.error && <p {...styles('error')} role="alert">{request.error} <button {...styles('retry')} onClick={request.retry}>Retry</button></p>}
    {data && <>
      <p {...styles('detailCount')}>{num(data.total)} matching games · newest first</p>
      <div className="ply-map-detail-columns">
        <div {...styles('paths')}>
          {data.rows.map(game => <article {...styles('path')} key={game.game_id}>
            <button {...styles('game')} onClick={() => setReview(game)}>{game.white_name} — {game.black_name}</button>
            <span {...styles('pathReason')}>{game.reason === 'ending' ? 'Actual game ending' : game.reason === 'continuation' ? 'Continues beyond move 15' : 'Shared position'}</span>
            <div {...styles('moves')}>{game.path.map(p => <button {...styles('move')} key={p.ply}
              onPointerMove={() => setPreview(p)} onFocus={() => setPreview(p)} onClick={() => setPreview(p)}>
              {Math.ceil(p.ply / 2)}{p.ply % 2 ? '.' : '…'} {p.san}</button>)}</div>
          </article>)}
          {!data.rows.length && <p {...styles('empty')}>No games for the selected players here.</p>}
        </div>
        <div {...styles('preview')}><GraphBoard preview fen={position?.fen || (selected.kind === 'root' ? initial : null)} /></div>
      </div>
      <footer><button {...styles('detailPrevious')} disabled={!offset} onClick={() => { setOffset(n => n - 10); setPreview(null) }}>Previous</button>
        <button {...styles('detailNext')} disabled={!data.has_more} onClick={() => { setOffset(n => n + 10); setPreview(null) }}>Next</button></footer>
    </>}
    {review && <PlayerGameReviewModal gameId={review.game_id} playerName={review.white_name}
      onClose={() => setReview(null)} onBack={() => setReview(null)} />}
  </section>
}

function PlayerPicker({ snapshot, selected, change }) {
  const [search, setSearch] = useState(''), [query, setQuery] = useState(''), [open, setOpen] = useState(false)
  const request = useGraphRead('ply-map/players', { snapshot, search: query }, open)
  return <div {...styles('players')} className="ply-map-players">
    <form onSubmit={event => { event.preventDefault(); setQuery(search.trim()); setOpen(true) }}>
      <label {...styles('searchLabel')}>Highlight players<input {...styles('search')} value={search}
        onChange={event => setSearch(event.target.value)} maxLength={64} placeholder="Player name" /></label>
      <button {...styles('searchButton')} type="submit">Find players</button>
      {!!selected.length && <button {...styles('clear')} type="button" onClick={() => change([])}>Clear highlights</button>}
    </form>
    <div {...styles('legend')} className="ply-map-legend">
      <span {...styles('neutral')}>All imported games</span>
      {selected.map((p, i) => <button {...styles(`player${i}`)} className={`ply-player-${i}`} key={p.player_id}
        onClick={() => change(selected.filter(v => v.player_id !== p.player_id))} title="Remove highlight">{p.name} ×</button>)}
    </div>
    {open && <div {...styles('playerResults')} className="ply-map-player-results">
      <button {...styles('searchClose')} aria-label="Close player search" onClick={() => setOpen(false)}>×</button>
      {request.loading && <span {...styles('status')}>Loading players…</span>}
      {request.error && <span {...styles('error')}>{request.error}</span>}
      {request.data?.rows.map(p => <button {...styles('playerResult')} key={p.player_id}
        disabled={selected.length >= 8 || selected.some(v => v.player_id === p.player_id)}
        onClick={() => { change([...selected, p]); setOpen(false) }}>{p.name} · {num(p.games)} games</button>)}
      {request.data?.rows.length === 0 && <span {...styles('empty')}>No imported player matches.</span>}
      {selected.length >= 8 && <span {...styles('limit')}>Eight player colors are selected.</span>}
    </div>}
  </div>
}

export default function PlyMapExplorer() {
  const [players, setPlayers] = useState([]), [first, setFirst] = useState(0), [offset, setOffset] = useState(0)
  const [selected, setSelected] = useState(null)
  const pinned = useRef(null), cached = useRef(null)
  const request = useGraphRead('ply-map', { ...(pinned.current ? { snapshot: pinned.current } : {}), first, last: Math.min(31, first + 6), offset, players: players.map(p => p.player_id).join(',') })
  if (request.data) {
    pinned.current = request.data.snapshot?.id || null
    cached.current = { first, offset, data: request.data }
  }
  // Keep the SVG mounted while requesting new highlight counts: no layout or
  // scroll reset when another player is selected.
  const data = request.data || (cached.current?.first === first && cached.current?.offset === offset ? cached.current.data : null)
  const snapshot = data?.snapshot
  const nodes = data?.nodes || [], edges = data?.connections || [], byKey = new Map(nodes.map(n => [n.key, n]))
  const originY = Math.min(...nodes.map(n => n.y), 1000000)
  const maxY = Math.max(10, ...nodes.map(n => n.y - originY)), height = maxY * 30 + 90
  const point = node => [80 + (node.x - first) * 210, 60 + (node.y - originY) * 30]
  const colors = item => players.map((p, i) => item.players?.[String(p.player_id)] ? i : -1).filter(i => i >= 0)
  const choose = item => setSelected({ ...item, snapshot: snapshot.id })
  const range = change => { setFirst(change); setOffset(0); setSelected(null) }
  return <div {...styles('panel')} className="ply-map">
    <div {...styles('toolbar')} className="ply-map-toolbar">
      <strong {...styles('title')}>Positions by move</strong>
      <button {...styles('refresh')} disabled={request.loading} onClick={() => { pinned.current = null; cached.current = null; setSelected(null); request.retry() }}>Refresh saved map</button>
    </div>
    {request.loading && <p {...styles('status')} role="status">Loading saved map…</p>}
    {request.error && <p {...styles('error')} role="alert">{request.error} <button {...styles('retry')} onClick={request.retry}>Retry</button></p>}
    {data && !snapshot && <p {...styles('empty')}>No collapsed map has been published yet. Use Run CUDA summaries on Analyze positions to prepare it.</p>}
    {snapshot && <>
      <p {...styles('coverage')}>{num(snapshot.games)} imported games · first 15 full moves · saved {new Date(snapshot.created_at).toLocaleString()}</p>
      {data.status.pending && <p {...styles('stale')}>Showing the last complete map. {data.status.syncing ? 'Neo4j synchronization is still in progress.' : 'Run CUDA summaries to include newer imported games.'}</p>}
      <PlayerPicker snapshot={snapshot.id} selected={players} change={setPlayers} />
      <p {...styles('explanation')}>Read left to right: each column is one half-move. Circles are shared positions; rectangles bundle unique continuations or endings. Dashed links hide unique positions. Click a node or link to explore its games. Player colors do not change the layout.</p>
      <div {...styles('range')} className="ply-map-range">
        <button {...styles('earlier')} disabled={!first} onClick={() => range(Math.max(0, first - 6))}>Earlier moves</button>
        <span {...styles('rangeLabel')}>Plies {first}–{Math.min(31, first + 6)}</span>
        <button {...styles('later')} disabled={first >= 30} onClick={() => range(Math.min(30, first + 6))}>Later moves</button>
      </div>
      <div {...styles('canvas')} className="ply-map-canvas" tabIndex={0} aria-label="Saved chronological position map">
        <svg width="1500" height={height} role="group" aria-label="Layered positions and compressed continuations">
          {Array.from({ length: 7 }, (_, i) => first + i).filter(p => p <= 31).map(p => <text {...styles('column')} key={p}
            x={80 + (p - first) * 210} y="22" textAnchor="middle">{p === 0 ? 'Start' : p === 31 ? 'Beyond 15' : `${Math.ceil(p / 2)}${p % 2 ? '. White' : '… Black'}`}</text>)}
          {edges.map(edge => {
            const a = byKey.get(edge.source), b = byKey.get(edge.target)
            if (!a || !b) return null
            const [x, y] = point(a), [tx, ty] = point(b), path = `M${x},${y} C${x + 100},${y} ${tx - 100},${ty} ${tx},${ty}`
            const highlighted = colors(edge)
            return <g key={edge.key} role="button" tabIndex={0} aria-label={`${edge.label || 'Direct move'}: ${num(edge.games)} games`}
              className="ply-map-link" onClick={() => choose(edge)} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); choose(edge) } }}>
              <path {...styles('edge')} d={path} fill="none" stroke="currentColor" strokeWidth="2" strokeDasharray={edge.hidden ? '5 4' : undefined} />
              {highlighted.map((i, index) => <path {...styles(`player${i}`)} className={`ply-player-${i}`} key={i} d={path} fill="none" stroke="currentColor" strokeWidth="2"
                transform={`translate(0,${index * 2})`} strokeDasharray={edge.hidden ? '5 4' : undefined} />)}
              <path d={path} fill="none" stroke="transparent" strokeWidth="12" />
              <title>{edge.hidden ? `${edge.hidden} hidden positions · ` : ''}{num(edge.games)} games</title>
            </g>
          })}
          {nodes.map(node => {
            const [x, y] = point(node), highlighted = colors(node), virtual = ['ending', 'continuation'].includes(node.kind)
            return <g {...styles('node')} key={node.key} transform={`translate(${x},${y})`} role="button" tabIndex={0}
              data-active={selected?.key === node.key} aria-label={`${title(node)}: ${num(node.games)} games`} className="ply-map-node"
              onClick={() => choose(node)} onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); choose(node) } }}>
              {virtual ? <rect {...styles('bundle')} x="-8" y="-8" width="16" height="16" rx="3" fill="currentColor" /> : <circle {...styles('neutral')} r="7" fill="currentColor" />}
              {highlighted.map((i, index) => <circle {...styles(`player${i}`)} className={`ply-player-${i}`} key={i} r={10 + index * 3} fill="none" stroke="currentColor" strokeWidth="2" />)}
              <text {...styles('nodeLabel')} x="15" y="4">{virtual ? node.kind === 'ending' ? 'Ends' : 'Continues' : node.label} · {num(node.games)}</text>
              <title>{title(node)} · {num(node.games)} distinct games{players.length ? ` · ${num(node.selected_games)} selected-player games` : ''}</title>
            </g>
          })}
        </svg>
      </div>
      {(data.has_more || offset > 0 || data.boundary_connections > 0) && <p {...styles('limit')}>Showing this saved section of the map. {num(data.boundary_connections)} connections lead outside the visible section; change moves or pages to explore them.</p>}
      <div {...styles('pagination')} className="ply-map-range">
        <button {...styles('previous')} disabled={!offset} onClick={() => setOffset(n => n - 300)}>Previous positions</button>
        <button {...styles('next')} disabled={!data.has_more} onClick={() => setOffset(n => n + 300)}>More positions</button>
      </div>
    </>}
    {selected && <Detail key={`${selected.snapshot}:${selected.key}:${players.map(p => p.player_id)}`} snapshot={selected.snapshot}
      selected={selected} players={players} close={() => setSelected(null)} />}
  </div>
}
