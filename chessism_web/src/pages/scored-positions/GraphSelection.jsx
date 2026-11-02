import { useState } from 'react'
import GraphBoard, { graphEvaluation, graphStyle as s } from './GraphBoard'
import GraphNeighbors from './GraphNeighbors'
import { useGraphRead } from './useGraphRead'
const number = n => Number(n || 0).toLocaleString()

export default function GraphSelection({ filter, selected, onSelect, onBack, canBack, onReset, onGames }) {
  const [pages, setPages] = useState({ incoming: 0, outgoing: 0 }), [flipped, setFlipped] = useState(false)
  const request = useGraphRead('position', { ...filter, key: selected, ...pages }), data = request.data
  return <aside className="opening-map-details">
    <section {...s('selection')} className="common-graph-selection">
      <div className="common-graph-page"><button {...s('back')} type="button" disabled={!canBack} onClick={onBack}>Back</button>
        <button {...s('reset')} type="button" onClick={onReset}>Starting position</button></div>
      {request.loading&&<p {...s('status')} role="status">Loading position…</p>}
      {request.error&&<p {...s('error')} role="alert">{request.error} <button {...s('retry')} type="button" onClick={request.retry}>Retry</button></p>}
      {data&&<>
        <h3 {...s('positionTitle')}>{data.position.fen_id?`FEN ${data.position.fen_id}`:'Starting position'}</h3>
        <button {...s('seeGames')} type="button" disabled={data.position.key==='chess:initial'&&!data.games} onClick={()=>onGames(data.position)}>See games</button>
        <GraphBoard fen={data.position.fen} orientation={(filter.color==='black')!==flipped?'black':'white'} />
        <button {...s('flip')} type="button" onClick={()=>setFlipped(value=>!value)}>Flip board</button>
        <p {...s('evaluation')}>{graphEvaluation(data.position)}</p>
        <div {...s('counts')} className="common-graph-counts"><span {...s('appearances')}>{number(data.appearances)} appearances</span>
          <span {...s('distinctGames')}>{number(data.games)} distinct games</span></div>
        <small {...s('mapDetailScope')}>Within the first 15 full moves of matching imported games.</small>
        {data.position.key==='chess:initial'&&<small {...s('rootNote')}>The shared start is not counted as a played move. Counts here represent returns to it. Click a move below to explore.</small>}
      </>}
    </section>
    {data&&<GraphNeighbors data={data} onSelect={onSelect} onGames={(moveKey,move)=>onGames(data.position,moveKey,move)}
      onPage={(direction,delta)=>setPages(current=>({...current,[direction]:current[direction]+delta}))} />}
  </aside>
}
