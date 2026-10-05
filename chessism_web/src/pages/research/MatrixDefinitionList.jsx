import MatrixPreviewButton from './MatrixPreviewButton'
import { formatNumber } from '../../utils/formatters'

export default function MatrixDefinitionList({ definitions, onPreview, onUse, onDelete }) {
  if (!definitions.length) return <p className="matrix-empty">No definitions saved yet. Saving stores instructions only.</p>
  return <div className="matrix-artifact-list">{definitions.map((definition) => (
    <article className="matrix-artifact" key={definition.id}>
      <div className="matrix-artifact-head">
        <div><strong>{definition.name}</strong><span>{definition.row_type.replaceAll('_', ' ')} · {new Date(definition.created_at).toLocaleString()}</span></div>
        <div className="matrix-artifact-tools"><MatrixPreviewButton name={definition.name} live onClick={() => onPreview(definition)} /><span className="matrix-state complete">saved definition</span></div>
      </div>
      <div className="matrix-artifact-result">
        <span>{definition.feature_count} features · {definition.label_count} labels</span>
        <span>up to {formatNumber(definition.config.filters.max_rows)} rows when run</span>
        <span>{formatNumber(definition.definition_bytes)} bytes of instructions · no matrix files</span>
      </div>
      <p className="matrix-definition-scope">
        {(definition.config.filters.players || []).join(', ') || 'All players'} · {(definition.config.filters.modes || []).join(' / ') || 'All modes'} · {definition.config.filters.date_from || 'Any start date'} → {definition.config.filters.date_to || 'Any end date'}
      </p>
      <div className="matrix-definition-columns"><span>Features: {definition.config.feature_columns.join(', ')}</span><span>Labels: {definition.config.label_columns.join(', ') || 'none'}</span></div>
      <div className="matrix-definition-actions">
        <button className="matrix-inspect-button" type="button" onClick={() => onUse(definition)}>use as starting point</button>
        <button className="matrix-delete" type="button" onClick={() => onDelete(definition)}>delete definition</button>
      </div>
    </article>
  ))}</div>
}
