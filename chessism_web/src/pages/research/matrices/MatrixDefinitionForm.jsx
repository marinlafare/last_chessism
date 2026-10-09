import { formatNumber } from '../../../utils/formatters'

export default function MatrixDefinitionForm({ model }) {
  const { catalog, rowTypes, rowType, form, working, saved, estimate, changeName, changeRowType,
    toggleMode, toggleColumn, updateFilter, estimateCurrentScope, showDraftPreview, save } = model
  const { name, rowTypeKey, features, labels, filters } = form
  return <>
    <section className="matrix-panel">
      <div className="section-head"><div><p className="eyebrow">1 · ROW UNIT</p><h2>Choose what one row represents</h2></div></div>
      <div className="matrix-row-types">
        {rowTypes.map((item) => (
          <button className={item.key === rowTypeKey ? 'active' : ''} type="button" key={item.key} onClick={() => changeRowType(item.key)}>
            <strong>{item.label}</strong><span>{item.description}</span>
          </button>
        ))}
      </div>
      {rowType ? <p className="matrix-sources">Sources: {rowType.sources.join(' · ')}</p> : null}
    </section>

    <section className="matrix-panel">
      <div className="section-head"><div><p className="eyebrow">2 · SCOPE</p><h2>Define the source filters</h2></div></div>
      <div className="matrix-filter-grid">
        <label className="matrix-wide"><span>Players · comma separated</span><input value={filters.players} onChange={(event) => updateFilter('players', event.target.value)} disabled={!rowType?.filters.players} placeholder="hikaru, lafareto" /></label>
        <label><span>From</span><input type="date" value={filters.date_from} onChange={(event) => updateFilter('date_from', event.target.value)} disabled={!rowType?.filters.dates} /></label>
        <label><span>To</span><input type="date" value={filters.date_to} onChange={(event) => updateFilter('date_to', event.target.value)} disabled={!rowType?.filters.dates} /></label>
        <label><span>Minimum full moves</span><input type="number" min="0" value={filters.min_moves} onChange={(event) => updateFilter('min_moves', event.target.value)} disabled={!rowType?.filters.minimum_moves} /></label>
        <label><span>Maximum output rows</span><input type="number" min="1" max={catalog?.limits.maximum_rows || 5000000} value={filters.max_rows} onChange={(event) => updateFilter('max_rows', event.target.value)} /></label>
      </div>
      <div className="matrix-filter-actions">
        {['bullet', 'blitz', 'rapid'].map((mode) => (
          <button className={filters.modes.includes(mode) ? 'active' : ''} type="button" key={mode} onClick={() => toggleMode(mode)} disabled={!rowType?.filters.modes}>{mode}</button>
        ))}
        <label><input type="checkbox" checked={filters.analyzed_only} onChange={(event) => updateFilter('analyzed_only', event.target.checked)} disabled={!rowType?.filters.analyzed_only} /> analyzed only</label>
      </div>
    </section>

    <section className="matrix-panel">
      <div className="section-head"><div><p className="eyebrow">3 · COLUMNS</p><h2>Assign features and labels</h2></div></div>
      <div className="matrix-column-head"><span>Column</span><span>Feature</span><span>Label</span></div>
      <div className="matrix-columns">
        {(rowType?.columns || []).map((column) => (
          <div className="matrix-column" key={column.key} title={column.description || column.source}>
            <div><strong>{column.label}</strong><span>{column.source} · {column.encoding} · {column.storage_dtype}</span></div>
            <input type="checkbox" checked={features.includes(column.key)} onChange={() => toggleColumn(column.key, 'feature')} aria-label={`${column.label} as feature`} />
            <input type="checkbox" checked={labels.includes(column.key)} onChange={() => toggleColumn(column.key, 'label')} disabled={!column.label_allowed} aria-label={`${column.label} as label`} />
          </div>
        ))}
      </div>
    </section>

    <section className="matrix-panel matrix-build-panel">
      <div className="section-head"><div><p className="eyebrow">4 · DEFINITION</p><h2>Preview and save instructions</h2></div></div>
      <div className="matrix-build-row matrix-definition-build-row">
        <label><span>Definition name</span><input value={name} maxLength="100" onChange={(event) => changeName(event.target.value)} /></label>
        <button className="btn btn-secondary btn-inline" type="button" onClick={estimateCurrentScope} disabled={working || !features.length}>{working === 'estimate' ? 'Estimating' : 'Estimate'}</button>
        <button className="btn btn-secondary btn-inline" type="button" onClick={showDraftPreview} disabled={working || !features.length}>Live preview</button>
        <button className="btn btn-primary btn-inline" type="button" onClick={save} disabled={working || saved.loading || !features.length}>{working === 'save' ? 'Saving' : 'Save definition'}</button>
      </div>
      {estimate ? (
        <div className="matrix-estimate">
          <article><span>Matching rows</span><strong>{formatNumber(estimate.matching_rows)}{estimate.matching_rows_is_lower_bound ? '+' : ''}</strong></article>
          <article><span>Rows if run now</span><strong>{formatNumber(estimate.selected_rows)}{estimate.selected_rows_is_lower_bound ? '+' : ''}</strong></article>
          <article><span>Configured row limit</span><strong>{formatNumber(estimate.row_limit)}</strong></article>
          <article><span>Instructions only</span><strong>{formatNumber(estimate.definition_bytes)} B</strong></article>
          <p>Live estimate, not a frozen dataset. “+” means a lower bound; counting is capped to avoid scanning the entire database. Saving does not require an estimate or matching rows.</p>
        </div>
      ) : null}
    </section>

  </>
}
