import { memo } from 'react'
import { formatNumber } from '../../utils/formatters'
import {
  AnalyticsCard,
  EmptyAnalytics,
  TimeContextNotice,
  WeekdayHourHeatmap,
} from './PlayerAnalyticsShared'

const cp = (value) => {
  const parsed = Number(value || 0)
  return `${parsed > 0 ? '+' : ''}${parsed.toFixed(1)} cp`
}

const CpByWeekday = memo(function CpByWeekday({ rows = [] }) {
  const maximum = Math.max(1, ...rows.flatMap((row) => [Number(row.total_cp_gain || 0), Number(row.total_cp_loss || 0)]))
  return (
    <div className="hero-cp-weekdays">
      {rows.map((row) => (
        <div className="hero-cp-weekday" key={row.weekday}>
          <strong>{String(row.label).slice(0, 3)}</strong>
          <div>
            <span title={`Gained ${cp(row.total_cp_gain)}`}><i className="gain" style={{ width: `${(Number(row.total_cp_gain || 0) / maximum) * 100}%` }} /></span>
            <span title={`Lost ${cp(row.total_cp_loss)}`}><i className="loss" style={{ width: `${(Number(row.total_cp_loss || 0) / maximum) * 100}%` }} /></span>
          </div>
          <small>{cp(row.net_cp_change)}</small>
        </div>
      ))}
    </div>
  )
})

const CpByHour = memo(function CpByHour({ rows = [] }) {
  const maximum = Math.max(1, ...rows.map((row) => Math.abs(Number(row.net_cp_change || 0))))
  return (
    <div className="hero-hour-bars signed">
      {rows.map((row) => {
        const net = Number(row.net_cp_change || 0)
        return (
          <div key={row.hour} title={`${String(row.hour).padStart(2, '0')}:00 · net ${cp(net)}`}>
            <i className={net < 0 ? 'negative' : ''} style={{ height: `${Math.max(2, (Math.abs(net) / maximum) * 100)}%` }} />
            <small>{row.hour}</small>
          </div>
        )
      })}
    </div>
  )
})

function GameInspection({ gameId, setGameId, onLoad, loading, error, data }) {
  const game = data?.game
  return (
    <AnalyticsCard title="Game Evaluation Sequence" subtitle="Raw White score, player score, gain and loss after every move" metric="Game ID" className="hero-wide-card">
      <div className="hero-inspector-controls">
        <label><span>Chess.com game ID</span><input className="text-input" inputMode="numeric" value={gameId} onChange={(event) => setGameId(event.target.value.replace(/\D/g, ''))} /></label>
        <button className="btn btn-primary btn-inline" type="button" disabled={!gameId || loading} onClick={onLoad}>{loading ? 'Loading…' : 'Inspect game'}</button>
      </div>
      {error ? <p className="player-analysis-error">{error}</p> : null}
      {game ? (
        <>
          <div className="hero-game-meta">
            <span>{game.color}</span><span>{game.mode}</span><span>vs {game.opponent}</span>
            <span>{formatNumber(game.analyzed_positions)} / {formatNumber(game.total_positions)} positions</span>
          </div>
          <div className="hero-position-table-wrap">
            <table className="hero-position-table">
              <thead><tr><th>Ply</th><th>Move</th><th>Player score</th><th>Change</th><th>Gain</th><th>Loss</th><th>Source</th></tr></thead>
              <tbody>
                {game.positions.map((position) => (
                  <tr key={position.ply} className={position.mover_is_player ? 'player-move' : ''}>
                    <td>{position.ply}</td><td>{position.move || '—'}</td>
                    <td>{position.score_kind === 'cp' ? cp(position.player_score) : `${position.player_score} (${position.score_kind})`}</td>
                    <td>{position.cp_change === null ? 'N/A' : cp(position.cp_change)}</td>
                    <td>{position.cp_gain === null ? 'N/A' : cp(position.cp_gain)}</td>
                    <td>{position.cp_loss === null ? 'N/A' : cp(position.cp_loss)}</td>
                    <td>{position.analysis_source || 'stockfish'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      ) : <EmptyAnalytics>Enter a game ID belonging to this player.</EmptyAnalytics>}
    </AnalyticsCard>
  )
}

function HourInspection({ inspector, onSelectGame }) {
  const { date, setDate, hour, setHour, onLoad, onLoadMore, loading, error, data } = inspector
  return (
    <AnalyticsCard title="Evaluation Sequences by Hour" subtitle="Games initialized during one resolved local hour" metric="Paginated games" className="hero-wide-card">
      <div className="hero-inspector-controls">
        <label><span>Date</span><input className="text-input" type="date" value={date} onChange={(event) => setDate(event.target.value)} /></label>
        <label><span>Hour</span><select className="text-input" value={hour} onChange={(event) => setHour(event.target.value)}>{Array.from({ length: 24 }, (_, value) => <option value={value} key={value}>{String(value).padStart(2, '0')}:00</option>)}</select></label>
        <button className="btn btn-primary btn-inline" type="button" disabled={!date || loading} onClick={onLoad}>{loading ? 'Loading…' : 'Inspect hour'}</button>
      </div>
      {error ? <p className="player-analysis-error">{error}</p> : null}
      {data ? (
        data.games.length ? (
          <div className="hero-hour-games">
            {data.games.map((game) => (
              <button type="button" key={game.game_id} onClick={() => onSelectGame(game.game_id)}>
                <strong>{game.game_id}</strong>
                <span>{game.mode} · {game.color} · vs {game.opponent}</span>
                <small>{formatNumber(game.positions.length)} scored positions · {game.played_at_local}</small>
              </button>
            ))}
            {data.pagination?.has_more ? <button className="btn btn-secondary" type="button" disabled={loading} onClick={onLoadMore}>Load more games</button> : null}
          </div>
        ) : <EmptyAnalytics>No games started during this hour.</EmptyAnalytics>
      ) : <EmptyAnalytics>Choose a local date and hour to query detailed games.</EmptyAnalytics>}
    </AnalyticsCard>
  )
}

export default function PlayerMeasuresPanel({ quality, gameInspector, hourInspector }) {
  const coverage = quality?.coverage || {}
  return (
    <>
      <TimeContextNotice context={quality?.time_context} />
      <div className="player-analysis-coverage">
        <div><span>Eligible games</span><strong>{formatNumber(coverage.eligible_games || 0)}</strong></div>
        <div><span>Excluded games</span><strong>{formatNumber(coverage.excluded_games || 0)}</strong></div>
        <div><span>Scored positions</span><strong>{formatNumber(coverage.scored_positions || 0)}</strong></div>
        <div><span>Perspective</span><strong>Player</strong></div>
      </div>
      <div className="player-chart-grid hero-analytics-grid">
        <AnalyticsCard title="CP Gain and Loss by Weekday" subtitle="Signed changes from the selected player’s perspective" metric="Gain − loss"><CpByWeekday rows={quality?.weekdays} /></AnalyticsCard>
        <AnalyticsCard title="Net CP Change by Hour" subtitle="Positive improvements and negative deterioration" metric="0–23"><CpByHour rows={quality?.hours} /></AnalyticsCard>
        <AnalyticsCard title="Weekday × Hour CP Balance" subtitle="Green is net gain; red is net loss" metric="7 × 24" className="hero-wide-card">
          <WeekdayHourHeatmap
            cells={quality?.weekday_hours}
            value={(cell) => cell.net_cp_change}
            title={(cell) => `gain ${cp(cell.total_cp_gain)} · loss ${cp(cell.total_cp_loss)} · net ${cp(cell.net_cp_change)}`}
            signed
          />
        </AnalyticsCard>
        <GameInspection {...gameInspector} />
        <HourInspection inspector={hourInspector} onSelectGame={gameInspector.selectGame} />
      </div>
    </>
  )
}
