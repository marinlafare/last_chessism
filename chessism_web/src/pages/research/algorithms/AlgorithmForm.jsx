import { pageStyleTarget } from '../../../appearance/styleCore.js'
import { bytes, number } from './algorithmForm'
import { inputColumns } from './builderForm'
import BuilderSteps from './BuilderSteps'
import BuilderOutputs from './BuilderOutputs'

export default function AlgorithmForm({ model }) {
  const { form, catalog, matrix, working, update, estimate, schemas } = model
  if (!form) return (
    <section {...pageStyleTarget('panel')} className="algorithm-panel">
      <h2 {...pageStyleTarget('title')}>1. Input matrix</h2>
      <p {...pageStyleTarget('text')}>{working ? 'Loading matrix definitions…' : 'Select a formula above, or open a saved algorithm below.'}</p>
    </section>
  )
  const available = inputColumns(matrix, catalog)
  const template = matrix?.config.definition_kind === 'template'
  const previewOnly = matrix?.config.move_scores?.all_moves || matrix?.config.filters?.max_rows === null
  const valid = Boolean(matrix) && !template && !previewOnly && form.name.trim() && form.columns.length && form.steps.length && form.outputs.length
  const toggle = (key) => update({
    columns: form.columns.includes(key) ? form.columns.filter((item) => item !== key) : [...form.columns, key],
  })
  return (
    <form onSubmit={(event) => { event.preventDefault(); if (valid) model.save() }}>
      <fieldset className="algorithm-panel" disabled={Boolean(working)}>
        <h2 {...pageStyleTarget('title')}>Create an algorithm</h2>
        <p {...pageStyleTarget('text')}>Build your own calculation from steps and formulas. Open a saved algorithm below to make a new revision.</p>
        <label {...pageStyleTarget('label')}>Open saved algorithm
          <select {...pageStyleTarget('field')} value="" onChange={(event) => {
            const definition = model.definitions.find((item) => item.id === event.target.value)
            if (definition) model.openDefinition(definition)
          }}>
            <option value="">Choose saved instructions…</option>
            {model.definitions.map((item) => <option key={item.id} value={item.id}>
              {item.name} · revision {item.config.revision || 1}
            </option>)}
          </select>
        </label>
        <div className="algorithm-actions">
          <button {...pageStyleTarget('button')} type="button" onClick={model.reset}>Create blank algorithm</button>
          <button {...pageStyleTarget('button')} type="button" onClick={() => model.template('relationships')}>Feature relationships template</button>
          <button {...pageStyleTarget('button')} type="button" onClick={() => model.template('duration')}>Duration by result example</button>
        </div>
        {form.parent_definition_id && <p {...pageStyleTarget('text')} className="algorithm-estimate">Editing a new revision. The original instructions and past results will not change.</p>}
      </fieldset>

      <fieldset className="algorithm-panel" disabled={Boolean(working)}>
        <h2 {...pageStyleTarget('title')}>1. Input matrix</h2>
        <div className="algorithm-fields">
          <p {...pageStyleTarget('text')}>{matrix?.name || 'No input selected'} · <a {...pageStyleTarget('link')} href="#matrix-formulas" onClick={(event) => {
            event.preventDefault(); document.getElementById('matrix-formulas')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
          }}>Change formula</a></p>
          <div className="algorithm-actions">
            <button {...pageStyleTarget('button')} type="button" disabled={!matrix || template} onClick={model.previewInput}>Preview selected data</button>
          </div>
        </div>
        <div className="algorithm-checks">
          {available.map((column) => <label {...pageStyleTarget('label')} key={column.key}>
            <input {...pageStyleTarget('field')} type="checkbox" checked={form.columns.includes(column.key)}
              disabled={!form.columns.includes(column.key) && form.columns.length >= 12}
              onChange={() => toggle(column.key)} />
            {column.key} <small {...pageStyleTarget('text')}>({column.data_type})</small>
          </label>)}
        </div>
        <p {...pageStyleTarget('text')}>
          Select up to 12 input columns. Text columns are valid grouping keys, not numeric measurements.
          Source: {matrix?.config.row_type} · {template ? 'scope chosen when used' : matrix?.config.filters?.modes?.join(', ') || 'all game types'}.
          Inputs are read from current database values only when you run or explicitly preview.
        </p>
        {template ? <p {...pageStyleTarget('text')} className="algorithm-warning">Choose and apply the data selection above. Formula samples do not configure a run.</p> : null}
        {!template && previewOnly ? <p {...pageStyleTarget('text')} className="algorithm-warning">This selection can be previewed. For the current bounded CPU runner, choose a fixed move range and an explicit input row limit. All-moves and unlimited-row execution are not enabled.</p> : null}
      </fieldset>

      <fieldset className="algorithm-panel" disabled={Boolean(working)}>
        <BuilderSteps form={form} schemas={schemas} update={update} />
      </fieldset>
      <fieldset className="algorithm-panel" disabled={Boolean(working)}>
        <BuilderOutputs form={form} schemas={schemas} update={update} />
      </fieldset>

      <fieldset className="algorithm-panel" disabled={Boolean(working)}>
        <h2 {...pageStyleTarget('title')}>4. Preparation and limits</h2>
        <div className="algorithm-fields">
          <label {...pageStyleTarget('label')}>Source missing values
            <select {...pageStyleTarget('field')} value={form.missing} onChange={(event) => update({ missing: event.target.value })}>
              <option value="keep">Keep N/A for explicit handling in steps</option>
              <option value="drop_rows">Exclude incomplete source rows</option>
              <option value="column_mean">Impute numeric column means</option>
            </select>
          </label>
          <label {...pageStyleTarget('label')}>Invalid formula results / division by zero
            <select {...pageStyleTarget('field')} value={form.invalid_values} onChange={(event) => update({ invalid_values: event.target.value })}>
              <option value="null">Return N/A</option><option value="error">Fail with an explanation</option>
            </select>
          </label>
          <label {...pageStyleTarget('label')}>Maximum source rows
            <input {...pageStyleTarget('field')} type="number" min="1" max={Math.min(matrix?.config.filters?.max_rows || 100000, 100000)}
              required value={form.max_rows} onChange={(event) => update({ max_rows: Number(event.target.value) })} />
          </label>
          <label {...pageStyleTarget('label')}>Scatter sampling seed
            <input {...pageStyleTarget('field')} type="number" min="0" max="4294967295" required value={form.seed}
              onChange={(event) => update({ seed: Number(event.target.value) })} />
          </label>
        </div>
        <p {...pageStyleTarget('text')}>
          One CPU worker · 100,000 source rows · 20 steps · 6 outputs · 10,000 groups maximum.
          Steps use the ordered source prefix, not a random selection. Rerunning after database changes can change results.
        </p>
      </fieldset>

      <fieldset className="algorithm-panel" disabled={Boolean(working)}>
        <h2 {...pageStyleTarget('title')}>5. Validate, test and save</h2>
        <label {...pageStyleTarget('label')}>Algorithm name
          <input {...pageStyleTarget('field')} required maxLength="100" value={form.name} onChange={(event) => update({ name: event.target.value })} />
        </label>
        <div className="algorithm-actions">
          <button {...pageStyleTarget('button')} type="button" disabled={!valid} onClick={model.validate}>Validate steps</button>
          <button {...pageStyleTarget('button')} type="button" disabled={!valid} onClick={model.preflight}>Check scope</button>
          <button {...pageStyleTarget('button')} type="button" disabled={!valid} onClick={model.sample}>Test on up to 500 rows</button>
          <button {...pageStyleTarget('button')} type="submit" disabled={!valid}>{form.parent_definition_id ? 'Save new revision' : 'Save algorithm'}</button>
        </div>
        {model.validation && <p {...pageStyleTarget('text')} className="algorithm-estimate" role="status">Valid: formulas, column types, dependencies and output definitions checked. No data was extracted.</p>}
        {estimate && <div className="algorithm-estimate" role="status">
          {estimate.estimate.selected_rows_is_lower_bound ? 'At least ' : ''}
          {number(estimate.estimate.selected_rows)} selected rows · input memory estimate{' '}
          {bytes(estimate.resources.working_input_bytes_estimate)} · intermediate allowance 256 MiB ·{' '}
          {estimate.resources.safe_to_run ? 'disk reserve protected' : 'insufficient free space'}.
        </div>}
        <p {...pageStyleTarget('text')}>
          Saving stores instructions only. Test explicitly queues a small run and shows intermediate previews.
          For the full calculation, press Run on the saved algorithm. Working inputs are discarded;
          compact results and instructions enter your next manual database backup.
        </p>
      </fieldset>
    </form>
  )
}
