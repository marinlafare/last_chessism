import { bytes, number, numericColumns } from './algorithmForm'

export default function AlgorithmForm({ model }) {
  const { form, inputs, catalog, working, update, estimate } = model
  if (!form) {
    return (
      <section className="algorithm-panel">
        <h2>1. Input matrix</h2>
        <p>{working ? 'Loading matrix definitions…' : 'Save a matrix definition first.'}</p>
        <a href="/research/matrices">Open matrices constructor</a>
      </section>
    )
  }
  const matrix = inputs.find((item) => item.id === form.matrix_definition_id)
  const numeric = numericColumns(matrix, catalog)
  const axes = [...form.columns, ...(form.rating_difference ? ['rating_difference'] : [])]
  const maxColumns = model.algorithmCatalog?.max_columns || 12
  const valid = form.columns.length >= 2 && form.x && form.y && form.name.trim()
  const canDerive = form.columns.includes('rating') && form.columns.includes('opponent_rating')
  const rowLimit = Math.min(matrix?.config.filters.max_rows || 1000000, 1000000)
  const toggle = (key) => update({
    columns: form.columns.includes(key)
      ? form.columns.filter((item) => item !== key)
      : [...form.columns, key],
  })

  return (
    <form onSubmit={(event) => { event.preventDefault(); model.save() }}>
      <fieldset className="algorithm-panel" disabled={Boolean(working)}>
        <h2>1. Input matrix</h2>
        <div className="algorithm-fields">
          <label>
            Saved instructions
            <select value={form.matrix_definition_id} onChange={(event) => model.selectInput(event.target.value)}>
              {inputs.map((item) => <option key={item.id} value={item.id}>{item.name}</option>)}
            </select>
          </label>
          <div className="algorithm-actions">
            <button type="button" onClick={() => model.setPreview(matrix)}>Preview live rows</button>
            {model.inputMore && <button type="button" onClick={model.moreInputs}>Load more matrices</button>}
          </div>
        </div>
        <p>
          Source: {matrix?.row_type} · {matrix?.config.filters.modes?.join(', ') || 'all game types'} ·
          ordered source rows, not random selection. The source recipe is frozen when you save;
          each run reads current database values.
        </p>
      </fieldset>

      <fieldset className="algorithm-panel" disabled={Boolean(working)}>
        <h2>2. Calculation</h2>
        <h3>{model.algorithmCatalog?.operations?.[0]?.name || 'Feature relationships'}</h3>
        <p>
          Column summaries, Pearson correlation heatmap and a scatter plot.
          Select 2–{maxColumns} numeric columns. Category codes are excluded.
        </p>
        <div className="algorithm-checks">
          {numeric.map((column) => (
            <label key={column.key}>
              <input type="checkbox" checked={form.columns.includes(column.key)}
                disabled={!form.columns.includes(column.key) && form.columns.length >= maxColumns}
                onChange={() => toggle(column.key)} />
              {column.label || column.key}
            </label>
          ))}
        </div>
        {numeric.length < 2 && (
          <p role="alert">
            This matrix needs at least two numeric columns. Create another definition in the matrix constructor.
          </p>
        )}
        <label className="algorithm-check">
          <input type="checkbox" checked={form.rating_difference} disabled={!canDerive}
            onChange={(event) => update({ rating_difference: event.target.checked })} />
          Add rating_difference = opponent_rating − rating
        </label>
        <div className="algorithm-fields">
          {['x', 'y'].map((axis) => (
            <label key={axis}>
              Scatter {axis.toUpperCase()}
              <select value={form[axis]} onChange={(event) => update({ [axis]: event.target.value })}>
                {axes.map((key) => <option key={key} value={key}>{key}</option>)}
              </select>
            </label>
          ))}
        </div>
      </fieldset>

      <fieldset className="algorithm-panel" disabled={Boolean(working)}>
        <h2>3. Data preparation</h2>
        <div className="algorithm-fields">
          <label>
            Missing values
            <select value={form.missing} onChange={(event) => update({ missing: event.target.value })}>
              <option value="drop_rows">Exclude incomplete rows</option>
              <option value="column_mean">Replace with column mean</option>
            </select>
          </label>
          <label>
            Scatter scaling
            <select value={form.scaling} onChange={(event) => update({ scaling: event.target.value })}>
              <option value="none">Original units</option>
              <option value="standardize">Standardize (z-score)</option>
            </select>
          </label>
          <label>
            Maximum source rows
            <input type="number" min="2" max={rowLimit} value={form.max_rows} required
              onChange={(event) => update({ max_rows: Number(event.target.value) })} />
          </label>
          <label>
            Scatter sampling seed
            <input type="number" min="0" max="4294967295" value={form.seed} required
              onChange={(event) => update({ seed: Number(event.target.value) })} />
          </label>
        </div>
        <p>
          Missing values are never silently replaced by zero. Standardization changes scatter units,
          not Pearson correlations. Summaries retain the original units.
        </p>
      </fieldset>

      <fieldset className="algorithm-panel" disabled={Boolean(working)}>
        <h2>4. Save instructions</h2>
        <label>
          Algorithm name
          <input required maxLength="100" value={form.name}
            onChange={(event) => update({ name: event.target.value })} />
        </label>
        <div className="algorithm-actions">
          <button type="button" disabled={!valid} onClick={model.preflight}>Check scope</button>
          <button type="submit" disabled={!valid}>Save algorithm</button>
        </div>
        <p>
          CPU reference · one run at a time · up to 1 million rows · temporary arrays removed after
          each run. CUDA and TensorFlow execution are not enabled in this version.
        </p>
        {estimate && (
          <div role="status" className="algorithm-estimate">
            {estimate.estimate.selected_rows_is_lower_bound ? 'At least ' : ''}
            {number(estimate.estimate.selected_rows)} selected rows · temporary allowance{' '}
            {bytes(estimate.resources.temporary_bytes_upper_estimate)} ·{' '}
            {estimate.resources.safe_to_run ? 'disk reserve protected' : 'insufficient free space'}.
            No matrix has been created.
          </div>
        )}
      </fieldset>
    </form>
  )
}
