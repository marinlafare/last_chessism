import { useState } from 'react'
import { pageStyleTarget } from '../../../appearance/styleCore.js'

// These are per-use choices, never modifications to the saved formula.
export default function MatrixParametersForm({ definition, catalog, onApply, onDirty, disabled = false }) {
  const isScores = definition.row_type === 'move_scores'
  const scoredDistribution = definition.row_type === 'score_distribution' && definition.config.summary_input_version >= 2
  const [filters, setFilters] = useState({ players: '', modes: ['bullet', 'blitz', 'rapid'],
    date_from: '', date_to: '', min_moves: 0, max_rows: null, analyzed_only: isScores || scoredDistribution })
  const [moves, setMoves] = useState({ all_moves: true, first_move: 1, last_move: 40, score_perspective: 'player' })
  const rowType = catalog?.row_types.find((row) => row.key === definition.row_type)
  const capabilities = rowType?.filters || {}
  const updateFilter = (key, value) => { onDirty?.(); setFilters((current) => ({ ...current, [key]: value })) }
  const updateMoves = (key, value) => { onDirty?.(); setMoves((current) => ({ ...current, [key]: value })) }

  const submit = (event) => {
    event.preventDefault()
    if (!rowType) return
    onApply({
      filters: {
        ...filters,
        players: filters.players.split(',').map((value) => value.trim()).filter(Boolean),
        min_moves: Number(filters.min_moves || 0),
        max_rows: filters.max_rows === null ? null : Number(filters.max_rows),
        date_from: filters.date_from || null, date_to: filters.date_to || null,
      },
      ...(isScores ? { move_scores: { ...moves,
        first_move: moves.all_moves ? 1 : Number(moves.first_move),
        last_move: moves.all_moves ? null : Number(moves.last_move),
      } } : {}),
    })
  }

  return <section {...pageStyleTarget('panel')} className="matrix-panel">
    <div className="section-head"><div><p {...pageStyleTarget('text')} className="eyebrow">ALGORITHM INPUT · DATA SELECTION</p><h2 {...pageStyleTarget('title')}>{definition.name}</h2></div></div>
    <p {...pageStyleTarget('text')} className="matrix-sources">Choose the data for this use of the formula. These choices do not change the saved columns or create another formula.</p>
    <form onSubmit={submit}>
      <fieldset className="matrix-runtime-fields" disabled={!rowType || disabled}>
        <div className="matrix-filter-grid">
          <label {...pageStyleTarget('label')} className="matrix-wide"><span>Players · comma separated</span><input {...pageStyleTarget('field')} value={filters.players} onChange={(event) => updateFilter('players', event.target.value)} disabled={!capabilities.players} placeholder="hikaru · blank means all players" /></label>
          <label {...pageStyleTarget('label')}><span>From</span><input {...pageStyleTarget('field')} type="date" value={filters.date_from} onChange={(event) => updateFilter('date_from', event.target.value)} disabled={!capabilities.dates} /></label>
          <label {...pageStyleTarget('label')}><span>To</span><input {...pageStyleTarget('field')} type="date" min={filters.date_from || undefined} value={filters.date_to} onChange={(event) => updateFilter('date_to', event.target.value)} disabled={!capabilities.dates} /></label>
          <label {...pageStyleTarget('label')}><span>Minimum game length</span>
            <select {...pageStyleTarget('field')} value={Number(filters.min_moves) > 0 ? 'minimum' : 'any'} onChange={(event) => updateFilter('min_moves', event.target.value === 'any' ? 0 : 1)} disabled={!capabilities.minimum_moves}>
              <option value="any">Any length · no minimum</option><option value="minimum">At least this many full moves</option>
            </select>
            {Number(filters.min_moves) > 0 ? <input {...pageStyleTarget('field')} aria-label="Minimum full moves" type="number" min="1" max="10000" required value={filters.min_moves} onChange={(event) => updateFilter('min_moves', event.target.value)} /> : null}
          </label>
          <label {...pageStyleTarget('label')}><span>{isScores ? 'Games / rows to include' : 'Output rows'}</span>
            <select {...pageStyleTarget('field')} value={filters.max_rows === null ? 'all' : 'limited'} onChange={(event) => updateFilter('max_rows', event.target.value === 'all' ? null : 1000)}>
              <option value="all">{isScores ? 'All matching games' : 'All matching rows'}</option><option value="limited">Maximum row count</option>
            </select>
            {filters.max_rows !== null ? <input {...pageStyleTarget('field')} aria-label="Maximum output rows" type="number" min="1" max={catalog?.limits.maximum_rows || 5000000} required value={filters.max_rows} onChange={(event) => updateFilter('max_rows', event.target.value)} /> : null}
          </label>
          {isScores ? <>
            <label {...pageStyleTarget('label')}><span>Moves to include</span><select {...pageStyleTarget('field')} value={moves.all_moves ? 'all' : 'range'} onChange={(event) => updateMoves('all_moves', event.target.value === 'all')}>
              <option value="all">All available moves</option><option value="range">Specific move range</option>
            </select></label>
            <label {...pageStyleTarget('label')}><span>Score perspective</span><select {...pageStyleTarget('field')} value={moves.score_perspective} onChange={(event) => updateMoves('score_perspective', event.target.value)}>
              <option value="player">Selected player · positive favors player</option><option value="white">White · positive favors White</option>
            </select></label>
            {!moves.all_moves ? <>
              <label {...pageStyleTarget('label')}><span>First move</span><input {...pageStyleTarget('field')} type="number" min="1" max={catalog?.limits.maximum_score_move || 500} required value={moves.first_move} onChange={(event) => updateMoves('first_move', event.target.value)} /></label>
              <label {...pageStyleTarget('label')}><span>Last move · n</span><input {...pageStyleTarget('field')} type="number" min={Number(moves.first_move) || 1} max={catalog?.limits.maximum_score_move || 500} required value={moves.last_move} onChange={(event) => updateMoves('last_move', event.target.value)} /></label>
            </> : null}
          </> : null}
        </div>
        <div className="matrix-filter-actions">
          {['bullet', 'blitz', 'rapid'].map((mode) => <button {...pageStyleTarget('button')} className={filters.modes.includes(mode) ? 'active' : ''} type="button" key={mode} disabled={!capabilities.modes}
            onClick={() => updateFilter('modes', filters.modes.includes(mode) ? filters.modes.filter((item) => item !== mode) : [...filters.modes, mode])}>{mode}</button>)}
          <label {...pageStyleTarget('label')}><input {...pageStyleTarget('field')} type="checkbox" checked={scoredDistribution || filters.analyzed_only} disabled={scoredDistribution || !capabilities.analyzed_only} onChange={(event) => updateFilter('analyzed_only', event.target.checked)} />{scoredDistribution ? 'Scored positions only' : isScores ? moves.all_moves ? 'Fully scored games' : 'Fully scored games + complete selected range' : 'Analyzed only'}</label>
        </div>
        {isScores ? <p {...pageStyleTarget('text')} className="matrix-sources">One row per player/game; selecting both participants produces two rows. With all moves, shorter games remain eligible and have N/A after their final move. Scores are in centipawns; stored mate encodings are not ordinary centipawn values.</p> : null}
        <p {...pageStyleTarget('text')} className="matrix-sources">Applying only sets this algorithm’s input instructions. Preview and calculation are separate actions. The current CPU runner requires a fixed move range and an explicit row limit; all-moves and unlimited-row selections are preview-only.</p>
        <button {...pageStyleTarget('button')} className="btn btn-primary btn-inline" type="submit">Apply data selection</button>
      </fieldset>
    </form>
  </section>
}
