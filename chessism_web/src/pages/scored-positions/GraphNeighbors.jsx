import { useId, useState } from 'react'
import { graphStyle as s } from './GraphBoard'

export default function GraphNeighbors({ data, onSelect, onGames, onPage }) {
  const marker = useId().replaceAll(':', '')
  const [highlight, setHighlight] = useState(null)
  const { incoming, outgoing } = data
  const max = Math.max(1, ...[...incoming.rows, ...outgoing.rows].map(row => row.appearances))
  const height = Math.max(240, Math.max(incoming.rows.length, outgoing.rows.length) * 76)
  const center = height / 2
  const y = (index, count) => (index + 0.5) * height / count
  return <section {...s('neighborhood')} className="common-graph-neighborhood" aria-label="Position connections">
    <div className="common-graph-directions">
      <span {...s('incomingTitle')}>Previous positions</span><span {...s('outgoingTitle')}>Next positions</span>
    </div>
    <svg {...s('network')} className="common-graph-network" viewBox={`0 0 600 ${height}`} role="group" aria-label="Recorded moves into and out of the selected position">
      <defs><marker id={marker} viewBox="0 0 10 10" refX="9" refY="5" markerWidth="5" markerHeight="5" orient="auto-start-reverse">
        <path d="M0 0L10 5L0 10Z" fill="context-stroke" /></marker></defs>
      {['incoming', 'outgoing'].flatMap(direction => data[direction].rows.map((row, index) => {
        const left = direction === 'incoming', targetY = y(index, data[direction].rows.length)
        const x1 = left ? 162 : 370, x2 = left ? 230 : 438
        const y1 = left ? targetY : center, y2 = left ? center : targetY
        return <path {...s('edge')} key={`${direction}-${row.move_key}`} className="common-graph-edge"
          strokeWidth={1 + 4 * Math.sqrt(row.appearances / max)} markerEnd={`url(#${marker})`}
          d={`M${x1} ${y1} C${(x1+x2)/2} ${y1},${(x1+x2)/2} ${y2},${x2} ${y2}`} />
      }))}
      <foreignObject x="230" y={center - 32} width="140" height="64">
        <div {...s('centerNode')} className="common-graph-center">Selected position<br />
          {data.position.fen_id ? `FEN ${data.position.fen_id}` : 'Starting position'}</div>
      </foreignObject>
      {['incoming', 'outgoing'].flatMap(direction => data[direction].rows.map((row, index) => {
        const id = `${direction}-${row.move_key}`, active = highlight === id
        return <foreignObject key={id} x={direction === 'incoming' ? 0 : 440}
          y={y(index, data[direction].rows.length) - 34} width="160" height="68">
          <div className="common-graph-node-group">
            <button {...s('node')} type="button" className="common-graph-node" data-active={active}
              title={`${row.move}: ${row.appearances.toLocaleString()} appearances in ${row.games.toLocaleString()} games`}
              onPointerMove={event => { setHighlight(id); event.currentTarget.focus({ preventScroll: true }) }} onFocus={() => setHighlight(id)}
              onClick={() => onSelect(row.position.key)}>
              <strong {...s('nodeMove')}>{row.move}</strong><span {...s('nodeCount')}>{row.appearances.toLocaleString()}</span>
            </button>
            <button {...s('moveGames')} className="common-graph-move-games" type="button"
              aria-label={`See games with ${row.move}`} onClick={() => onGames(row.move_key, row.move)}>See games</button>
          </div>
        </foreignObject>
      }))}
    </svg>
    <div className="common-graph-directions">
      {['incoming', 'outgoing'].map(direction => <div key={direction} className="common-graph-page">
        <button {...s(`${direction}Previous`)} type="button" disabled={!data[direction].offset}
          aria-label={`Previous ${direction} moves`} onClick={() => onPage(direction, -6)}>‹</button>
        {!data[direction].rows.length && <span {...s('empty')}>{direction === 'incoming' ? 'No earlier moves' : 'No later moves'}</span>}
        <button {...s(`${direction}Next`)} type="button" disabled={!data[direction].has_more || data[direction].offset >= 996}
          aria-label={`More ${direction} moves`} onClick={() => onPage(direction, 6)}>›</button>
      </div>)}
    </div>
  </section>
}
