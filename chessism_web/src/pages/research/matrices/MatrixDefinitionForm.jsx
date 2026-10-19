import { pageStyleTarget } from '../../../appearance/styleCore.js'

export default function MatrixDefinitionForm({ model }) {
  const { catalog, rowTypes, rowType, form, working, changeName, changeRowType, toggleColumn, save } = model
  const { name, rowTypeKey, features, labels } = form
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
      <div className="section-head"><div><p {...pageStyleTarget('text')} className="eyebrow">2 · COLUMNS</p><h2 {...pageStyleTarget('title')}>Assign features and labels</h2></div></div>
        <div className="matrix-column-head"><span>Column</span><span>Feature</span><span>Label</span></div>
        <div className="matrix-columns">
          {(rowType?.columns || []).map((column) => (
            <div className="matrix-column" key={column.key} title={column.description || column.source}>
              <div><strong {...pageStyleTarget('value')}>{column.label}</strong><span>{column.source} · {column.encoding} · {column.storage_dtype}</span></div>
              <input {...pageStyleTarget('field')} type="checkbox" checked={features.includes(column.key)} onChange={() => toggleColumn(column.key, 'feature')} aria-label={column.label + ' as feature'} />
              <input {...pageStyleTarget('field')} type="checkbox" checked={labels.includes(column.key)} onChange={() => toggleColumn(column.key, 'label')} disabled={!column.label_allowed} aria-label={column.label + ' as label'} />
            </div>
          ))}
        </div>
      <p {...pageStyleTarget('text')} className="matrix-sources">Only the column structure is saved here. In Algorithm creation, preview the formula or select it to choose players, game modes, dates, game count and move range.</p>
    </section>

    <section {...pageStyleTarget('panel')} className="matrix-panel matrix-build-panel">
      <div className="section-head"><div><p {...pageStyleTarget('text')} className="eyebrow">3 · FORMULA</p><h2 {...pageStyleTarget('title')}>Save reusable columns</h2></div></div>
      <div className="matrix-build-row matrix-definition-build-row">
        <label {...pageStyleTarget('label')}><span>Formula name</span><input {...pageStyleTarget('field')} value={name} maxLength="100" onChange={(event) => changeName(event.target.value)} /></label>
        <button {...pageStyleTarget('button')} className="btn btn-primary btn-inline" type="button" onClick={save} disabled={working || !catalog || !features.length}>{working === 'save' ? 'Saving' : 'Save to collection'}</button>
      </div>
      <p {...pageStyleTarget('text')} className="matrix-sources">No games are selected or queried when saving. The same formula can be used for different players and move ranges without making another copy.</p>
    </section>
  </>
}
