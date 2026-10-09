import { nextStepId, stepDefaults } from './builderForm'

const OPERATIONS = ['filter', 'calculate', 'transform', 'aggregate', 'correlate', 'select', 'sort']
const METRICS = ['count', 'sum', 'mean', 'weighted_mean', 'min', 'max', 'std']

export function ColumnChoice({ label, value = '', columns, onChange }) {
  return <label>{label}<select value={value} onChange={(event) => onChange(event.target.value)}>
    <option value="">Choose a column</option>
    {value && !columns.includes(value) && <option value={value}>Missing: {value}</option>}
    {columns.map((key) => <option key={key} value={key}>{key}</option>)}
  </select></label>
}

export function ColumnChecks({ label, columns, selected = [], onChange }) {
  return <div><p>{label}</p><div className="algorithm-checks">
    {columns.map((key) => <label key={key}>
      <input type="checkbox" checked={selected.includes(key)} onChange={() => onChange(selected.includes(key) ? selected.filter((item) => item !== key) : [...selected, key])} />{key}
    </label>)}
    {selected.filter((key) => !columns.includes(key)).map((key) => <span className="algorithm-warning" key={key}>Missing: {key}</span>)}
  </div></div>
}

function AggregationEditor({ step, schema, onChange }) {
  const numeric = Object.keys(schema).filter((key) => schema[key] === 'number')
  const update = (index, patch) => onChange({ metrics: step.metrics.map((metric, i) => i === index ? { ...metric, ...patch } : metric) })
  return <>
    <ColumnChecks label="Group by (leave empty for one summary row)" columns={Object.keys(schema)} selected={step.group_by} onChange={(group_by) => onChange({ group_by })} />
    {step.metrics.map((metric, index) => <div className="algorithm-metric-editor" key={index}>
      <label>Result column<input value={metric.name} onChange={(event) => update(index, { name: event.target.value })} /></label>
      <label>Aggregation<select value={metric.op} onChange={(event) => update(index, { op: event.target.value })}>
        {METRICS.map((key) => <option key={key}>{key}</option>)}
      </select></label>
      {metric.op !== 'count' && <ColumnChoice label="Value column" value={metric.column} columns={numeric} onChange={(column) => update(index, { column })} />}
      {metric.op === 'weighted_mean' && <ColumnChoice label="Weight column" value={metric.weight} columns={numeric} onChange={(weight) => update(index, { weight })} />}
      <button type="button" onClick={() => onChange({ metrics: step.metrics.filter((_, i) => i !== index) })}>Remove metric</button>
    </div>)}
    <button type="button" disabled={step.metrics.length >= 48} onClick={() => onChange({ metrics: [...step.metrics, { name: `metric_${step.metrics.length + 1}`, op: 'count' }] })}>Add metric</button>
  </>
}

export default function BuilderSteps({ form, schemas, update }) {
  const change = (index, patch) => update({ steps: form.steps.map((step, i) => i === index ? { ...step, ...patch } : step) })
  const move = (index, direction) => {
    const steps = [...form.steps]
    ;[steps[index], steps[index + direction]] = [steps[index + direction], steps[index]]
    update({ steps })
  }
  const append = () => {
    const input = form.steps.at(-1)?.id || 'input'
    update({ steps: [...form.steps, stepDefaults('filter', nextStepId(form.steps), input, schemas[input])] })
  }
  return <>
    <h2>2. Build the calculation</h2>
    <p>Add steps and write formulas. Every input must refer to the source matrix or an earlier step. Reordering does not silently change dependencies.</p>
    {!form.steps.length && <p>No steps yet. Add your first step or choose a template.</p>}
    {form.steps.map((step, index) => {
      const sources = ['input', ...form.steps.slice(0, index).map((item) => item.id)]
      const schema = schemas[step.input] || {}
      const numeric = Object.keys(schema).filter((key) => schema[key] === 'number')
      return <article className="algorithm-card algorithm-step" key={step.id}>
        <div className="algorithm-heading"><h3>Step {index + 1} · {step.id}</h3><div className="algorithm-actions">
          <button type="button" aria-label={`Move ${step.id} up`} disabled={!index} onClick={() => move(index, -1)}>↑</button>
          <button type="button" aria-label={`Move ${step.id} down`} disabled={index === form.steps.length - 1} onClick={() => move(index, 1)}>↓</button>
          <button type="button" disabled={form.steps.length >= 20} onClick={() => {
            const steps = [...form.steps]
            steps.splice(index + 1, 0, { ...structuredClone(step), id: nextStepId(steps), name: `${step.name} copy` })
            update({ steps })
          }}>Duplicate step</button>
          <button type="button" onClick={() => update({ steps: form.steps.filter((_, i) => i !== index) })}>Remove step</button>
        </div></div>
        <div className="algorithm-fields">
          <label>Step name<input value={step.name} maxLength="100" onChange={(event) => change(index, { name: event.target.value })} /></label>
          <label>Input<select value={step.input} onChange={(event) => change(index, { input: event.target.value })}>
            {!sources.includes(step.input) && <option value={step.input}>Invalid dependency: {step.input}</option>}
            {sources.map((key) => <option key={key}>{key}</option>)}
          </select></label>
          <label>Operation<select value={step.op} onChange={(event) => change(index, stepDefaults(event.target.value, step.id, step.input, schema))}>
            {OPERATIONS.map((key) => <option key={key}>{key}</option>)}
          </select></label>
        </div>
        {step.op === 'calculate' && <label>New column name<input value={step.column} onChange={(event) => change(index, { column: event.target.value })} /></label>}
        {['filter', 'calculate'].includes(step.op) && <label>Formula<textarea spellCheck="false" maxLength="500" rows="2" value={step.expression} onChange={(event) => change(index, { expression: event.target.value })} /></label>}
        {['transform', 'correlate', 'select'].includes(step.op) && <ColumnChecks label="Columns" columns={step.op === 'select' ? Object.keys(schema) : numeric} selected={step.columns} onChange={(columns) => change(index, { columns })} />}
        {step.op === 'transform' && <label>Method<select value={step.method} onChange={(event) => change(index, { method: event.target.value })}>
          <option value="standardize">Standardize: (value − mean) / std</option><option value="normalize">Normalize to 0–1</option>
        </select></label>}
        {step.op === 'aggregate' && <AggregationEditor step={step} schema={schema} onChange={(patch) => change(index, patch)} />}
        {step.op === 'sort' && <div className="algorithm-fields">
          <ColumnChoice label="Sort column" value={step.column} columns={Object.keys(schema)} onChange={(column) => change(index, { column })} />
          <label>Direction<select value={step.descending ? 'desc' : 'asc'} onChange={(event) => change(index, { descending: event.target.value === 'desc' })}><option value="asc">Ascending</option><option value="desc">Descending</option></select></label>
        </div>}
        <p className="algorithm-column-hint">Available: {Object.keys(schema).join(', ') || 'fix the input dependency first'}</p>
      </article>
    })}
    <button type="button" disabled={form.steps.length >= 20} onClick={append}>Add step</button>
    <details className="algorithm-formula-help"><summary>Formula reference</summary>
      <p>Use column names, numbers, text in quotes, + − * / ** %, comparisons, and / or / not. Python statements, imports and attribute access are not supported.</p>
      <code>safe_divide(elapsed_seconds, moves)</code><br /><code>where(rating &gt; 2000, 1, 0)</code><br /><code>floor(rating / 200) * 200</code>
      <p>Functions: abs, sqrt, log, exp, floor, ceil, clip(value, min, max), safe_divide(a, b), where(condition, yes, no), is_missing(value), fill_missing(value, replacement).</p>
      <p>utc_day(started_at), utc_month(started_at), utc_year(started_at) turn epoch seconds into UTC grouping keys. Null/non-finite comparisons are false. Constant-column normalization produces zero for finite values.</p>
    </details>
  </>
}
