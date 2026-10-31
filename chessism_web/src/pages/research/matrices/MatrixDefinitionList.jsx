import { pageStyleTarget } from '../../../appearance/styleCore.js'
import MatrixPreviewButton from './MatrixPreviewButton'
import MoveScoreFields from './MoveScoreFields'
import { formatNumber } from '../../../utils/formatters'

export default function MatrixDefinitionList({ definitions, onPreview, onUse, onDelete, onSelect, selectedId, disabled = false }) {
  if (!definitions.length) return <p {...pageStyleTarget('text')} className="matrix-empty">No formulas saved yet. Saving stores instructions only.</p>
  return <div className="matrix-artifact-list">{definitions.map((definition) => {
    const { config } = definition
    const template = config.definition_kind === 'template'
    const builtin = Boolean(definition.builtin_key)
    const dynamicMoves = config.move_scores?.all_moves
    const filters = config.filters || {}
    if (builtin) return <article {...pageStyleTarget('card')} className="matrix-artifact matrix-default-formula" aria-label={definition.name} key={definition.id}>
      <strong {...pageStyleTarget('value')} className="matrix-default-name">{definition.name}</strong>
      {onSelect ? <button {...pageStyleTarget('button')} type="button" className="matrix-score-schema matrix-formula-choice"
        disabled={disabled} aria-pressed={selectedId === definition.id} aria-label={`Select ${definition.name}`}
        onClick={() => onSelect(definition)}>
        <code>{definition.row_type === 'move_scores' ? 'player | game | result | move_score_1 | … | move_score_n' : [...config.feature_columns, ...config.label_columns].join(' | ')}</code>
      </button> : definition.row_type === 'move_scores' ? <MoveScoreFields /> : <div {...pageStyleTarget('text')} className="matrix-score-schema" aria-label="Formula columns">
        <code>{[...config.feature_columns, ...config.label_columns].join(' | ')}</code>
      </div>}
      <MatrixPreviewButton name={definition.name} live disabled={disabled} onClick={() => onPreview(definition)} />
      {definition.row_type === 'accuracy_positions' ? <p {...pageStyleTarget('text')} className="matrix-default-description">Fully scored games · each game once · both colors’ moves in order · White-perspective scores. Game and ply are structural keys. Repeated positions are retained; a row limit never splits a selected game. Preview shows a short excerpt only. Initial +15 CP belongs to the algorithm, not an extra column. Formula only: no accuracy algorithm runs.</p> : null}
      {definition.row_type === 'blunder_moves' ? <p {...pageStyleTarget('text')} className="matrix-default-description">Fully analyzed games · before/after evaluations · game-time ratings. Initial prior: 1 means the first White move uses an assumed +15 CP before-evaluation; otherwise 0. Game outcomes belong to a separate training dataset. The rating-aware model is not fitted or active.</p> : null}
      {definition.row_type === 'score_distribution' ? <p {...pageStyleTarget('text')} className="matrix-default-description">Scored positions only · both colors’ moves from each player’s perspective · repeated occurrences retained · centipawn, mate and tablebase evaluations kept separate.</p> : null}
    </article>
    return <article {...pageStyleTarget('card')} className="matrix-artifact" key={definition.id}>
      <div className="matrix-artifact-head">
        <div><strong {...pageStyleTarget('value')}>{definition.name}</strong><span>{definition.row_type.replaceAll('_', ' ')} · {new Date(definition.created_at).toLocaleString()}</span></div>
        <div className="matrix-artifact-tools"><MatrixPreviewButton name={definition.name} live disabled={disabled} onClick={() => onPreview(definition)} /><span className="matrix-state complete">{template ? 'reusable formula' : 'saved scoped formula'}</span></div>
      </div>
      <div className="matrix-artifact-result">
        <span>{definition.feature_count} {dynamicMoves ? 'base features + move scores' : 'features'} · {definition.label_count} labels</span>
        <span>{template ? 'Data scope belongs to the algorithm' : filters.max_rows === null ? 'All matching rows · no limit' : 'up to ' + formatNumber(filters.max_rows) + ' rows when run'}</span>
        <span>{formatNumber(definition.definition_bytes)} bytes of instructions · no matrix files</span>
      </div>
      {template ? <p {...pageStyleTarget('text')} className="matrix-definition-scope">Only the column structure is saved. Preview shows an automatic sample; it does not select the data for an algorithm.</p> :
        <p {...pageStyleTarget('text')} className="matrix-definition-scope">
          {(filters.players || []).join(', ') || 'All players'} · {(filters.modes || []).join(' / ') || 'All modes'} · {filters.date_from || 'Any start date'} → {filters.date_to || 'Any end date'}
        </p>}
      <details className="matrix-definition-columns">
        <summary>Columns ({definition.feature_count + definition.label_count}{dynamicMoves ? ' + move_score_1 … move_score_n' : ''})</summary>
        <span>Features: {config.feature_columns.join(', ')}{dynamicMoves ? ', move_score_1 … move_score_n' : ''}</span>
        <span>Labels: {config.label_columns.join(', ') || 'none'}</span>
      </details>
      {definition.row_type === 'move_scores' && !template ? <p {...pageStyleTarget('text')} className="matrix-definition-scope">
        {dynamicMoves ? 'All available moves' : 'Moves ' + config.move_scores.first_move + '–' + config.move_scores.last_move} · {config.move_scores.score_perspective === 'player' ? "selected player's" : 'White'} perspective · result: w / d / l · {filters.analyzed_only ? dynamicMoves ? 'fully scored games · shorter games included' : 'fully scored games and complete move range' : 'missing scores allowed'} · date order
      </p> : null}
      <div className="matrix-definition-actions">
        {onSelect ? <button {...pageStyleTarget('button')} className="matrix-inspect-button" type="button" disabled={disabled} aria-pressed={selectedId === definition.id} onClick={() => onSelect(definition)}>Use in algorithm</button> : null}
        <button {...pageStyleTarget('button')} className="matrix-inspect-button" type="button" disabled={disabled} onClick={() => onPreview(definition)}>preview dataframe</button>
        {definition.row_type !== 'move_scores' ? onUse ? <button {...pageStyleTarget('button')} className="matrix-inspect-button" type="button" disabled={disabled} onClick={() => onUse(definition)}>use columns as starting point</button> : <a {...pageStyleTarget('link')} className="matrix-inspect-button" href={'/research/matrices?definition=' + encodeURIComponent(definition.id)}>use columns as starting point</a> : null}
        {onDelete ? <button {...pageStyleTarget('button')} className="matrix-delete" type="button" disabled={disabled} onClick={() => onDelete(definition)}>delete formula</button> : null}
      </div>
    </article>
  })}</div>
}
