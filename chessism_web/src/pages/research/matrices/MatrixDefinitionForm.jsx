import { pageStyleTarget } from '../../../appearance/styleCore.js'
import { formatNumber } from '../../../utils/formatters'

export default function MatrixDefinitionForm({ model }) {
  const { catalog, rowTypes, rowType, form, working, saved, estimate, changeName, changeRowType,
    toggleMode, toggleColumn, updateFilter, estimateCurrentScope, showDraftPreview, save } = model
  const { name, rowTypeKey, features, labels, filters } = form
  return <>
    <section {...pageStyleTarget('panel')} className="matrix-panel">
      <div className="section-head"><div><p {...pageStyleTarget('text')} className="eyebrow">1 · ROW UNIT</p><h2 {...pageStyleTarget('title')}>Choose what one row represents</h2></div></div>
      <div className="matrix-row-types">
        {rowTypes.map((item) => (
          <button {...pageStyleTarget('button')} className={item.key === rowTypeKey ? 'active' : ''} type="button" key={item.key} onClick={() => changeRowType(item.key)}>
            <strong {...pageStyleTarget('value')}>{item.label}</strong><span>{item.description}</span>
          </button>
        ))}
      </div>
      {rowType ? <p {...pageStyleTarget('text')} className="matrix-sources">Sources: {rowType.sources.join(' · ')}</p> : null}
    </section>

    <section {...pageStyleTarget('panel')} className="matrix-panel">
      <div className="section-head"><div><p {...pageStyleTarget('text')} className="eyebrow">2 · SCOPE</p><h2 {...pageStyleTarget('title')}>Define the source filters</h2></div></div>
      <div className="matrix-filter-grid">
        <label {...pageStyleTarget('label')} className="matrix-wide"><span>Players · comma separated</span><input {...pageStyleTarget('field')} value={filters.players} onChange={(event) => updateFilter('players', event.target.value)} disabled={!rowType?.filters.players} placeholder="hikaru, lafareto" /></label>
        <label {...pageStyleTarget('label')}><span>From</span><input {...pageStyleTarget('field')} type="date" value={filters.date_from} onChange={(event) => updateFilter('date_from', event.target.value)} disabled={!rowType?.filters.dates} /></label>
        <label {...pageStyleTarget('label')}><span>To</span><input {...pageStyleTarget('field')} type="date" value={filters.date_to} onChange={(event) => updateFilter('date_to', event.target.value)} disabled={!rowType?.filters.dates} /></label>
        <label {...pageStyleTarget('label')}><span>Minimum full moves</span><input {...pageStyleTarget('field')} type="number" min="0" value={filters.min_moves} onChange={(event) => updateFilter('min_moves', event.target.value)} disabled={!rowType?.filters.minimum_moves} /></label>
        <label {...pageStyleTarget('label')}><span>Maximum output rows</span><input {...pageStyleTarget('field')} type="number" min="1" max={catalog?.limits.maximum_rows || 5000000} value={filters.max_rows} onChange={(event) => updateFilter('max_rows', event.target.value)} /></label>
      </div>
      <div className="matrix-filter-actions">
        {['bullet', 'blitz', 'rapid'].map((mode) => (
          <button {...pageStyleTarget('button')} className={filters.modes.includes(mode) ? 'active' : ''} type="button" key={mode} onClick={() => toggleMode(mode)} disabled={!rowType?.filters.modes}>{mode}</button>
        ))}
        <label {...pageStyleTarget('label')}><input {...pageStyleTarget('field')} type="checkbox" checked={filters.analyzed_only} onChange={(event) => updateFilter('analyzed_only', event.target.checked)} disabled={!rowType?.filters.analyzed_only} /> analyzed only</label>
      </div>
    </section>

    <section {...pageStyleTarget('panel')} className="matrix-panel">
      <div className="section-head"><div><p {...pageStyleTarget('text')} className="eyebrow">3 · COLUMNS</p><h2 {...pageStyleTarget('title')}>Assign features and labels</h2></div></div>
      <div className="matrix-column-head"><span>Column</span><span>Feature</span><span>Label</span></div>
      <div className="matrix-columns">
        {(rowType?.columns || []).map((column) => (
          <div className="matrix-column" key={column.key} title={column.description || column.source}>
            <div><strong {...pageStyleTarget('value')}>{column.label}</strong><span>{column.source} · {column.encoding} · {column.storage_dtype}</span></div>
            <input {...pageStyleTarget('field')} type="checkbox" checked={features.includes(column.key)} onChange={() => toggleColumn(column.key, 'feature')} aria-label={`${column.label} as feature`} />
            <input {...pageStyleTarget('field')} type="checkbox" checked={labels.includes(column.key)} onChange={() => toggleColumn(column.key, 'label')} disabled={!column.label_allowed} aria-label={`${column.label} as label`} />
          </div>
        ))}
      </div>
    </section>

    <section {...pageStyleTarget('panel')} className="matrix-panel matrix-build-panel">
      <div className="section-head"><div><p {...pageStyleTarget('text')} className="eyebrow">4 · DEFINITION</p><h2 {...pageStyleTarget('title')}>Preview and save instructions</h2></div></div>
      <div className="matrix-build-row matrix-definition-build-row">
        <label {...pageStyleTarget('label')}><span>Definition name</span><input {...pageStyleTarget('field')} value={name} maxLength="100" onChange={(event) => changeName(event.target.value)} /></label>
        <button {...pageStyleTarget('button')} className="btn btn-secondary btn-inline" type="button" onClick={estimateCurrentScope} disabled={working || !features.length}>{working === 'estimate' ? 'Estimating' : 'Estimate'}</button>
        <button {...pageStyleTarget('button')} className="btn btn-secondary btn-inline" type="button" onClick={showDraftPreview} disabled={working || !features.length}>Live preview</button>
        <button {...pageStyleTarget('button')} className="btn btn-primary btn-inline" type="button" onClick={save} disabled={working || saved.loading || !features.length}>{working === 'save' ? 'Saving' : 'Save definition'}</button>
      </div>
      {estimate ? (
        <div className="matrix-estimate">
          <article {...pageStyleTarget('card')}><span>Matching rows</span><strong {...pageStyleTarget('value')}>{formatNumber(estimate.matching_rows)}{estimate.matching_rows_is_lower_bound ? '+' : ''}</strong></article>
          <article {...pageStyleTarget('card')}><span>Rows if run now</span><strong {...pageStyleTarget('value')}>{formatNumber(estimate.selected_rows)}{estimate.selected_rows_is_lower_bound ? '+' : ''}</strong></article>
          <article {...pageStyleTarget('card')}><span>Configured row limit</span><strong {...pageStyleTarget('value')}>{formatNumber(estimate.row_limit)}</strong></article>
          <article {...pageStyleTarget('card')}><span>Instructions only</span><strong {...pageStyleTarget('value')}>{formatNumber(estimate.definition_bytes)} B</strong></article>
          <p {...pageStyleTarget('text')}>Live estimate, not a frozen dataset. “+” means a lower bound; counting is capped to avoid scanning the entire database. Saving does not require an estimate or matching rows.</p>
        </div>
      ) : null}
    </section>

  </>
}
