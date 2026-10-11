import { pageStyleTarget } from '../../../appearance/styleCore.js'
import { ColumnChecks, ColumnChoice } from './BuilderSteps'

export default function BuilderOutputs({ form, schemas, update }) {
  const change = (index, patch) => update({ outputs: form.outputs.map((item, i) => i === index ? { ...item, ...patch } : item) })
  const sources = ['input', ...form.steps.map((step) => step.id)]
  return <>
    <h2 {...pageStyleTarget('title')}>3. Choose outputs</h2>
    <p {...pageStyleTarget('text')}>Outputs may use different steps. Tables show up to 100 rows; scatter plots sample up to 1,000 points; line/bar charts show up to 200 points. Summaries use the complete selected scope.</p>
    {form.outputs.map((output, index) => {
      const schema = schemas[output.input] || {}
      const columns = Object.keys(schema)
      const numeric = columns.filter((key) => schema[key] === 'number')
      return <article {...pageStyleTarget('card')} className="algorithm-card" key={index}>
        <div className="algorithm-fields">
          <label {...pageStyleTarget('label')}>Output title<input {...pageStyleTarget('field')} value={output.name} maxLength="100" onChange={(event) => change(index, { name: event.target.value })} /></label>
          <label {...pageStyleTarget('label')}>Input step<select {...pageStyleTarget('field')} value={output.input} onChange={(event) => change(index, { input: event.target.value, columns: [] })}>
            {!sources.includes(output.input) && <option value={output.input}>Missing: {output.input}</option>}
            {sources.map((key) => <option key={key}>{key}</option>)}
          </select></label>
          <label {...pageStyleTarget('label')}>Display<select {...pageStyleTarget('field')} value={output.type} onChange={(event) => change(index, { type: event.target.value })}>
            {['table', 'scatter', 'line', 'bar', 'heatmap', 'scalar'].map((key) => <option key={key}>{key}</option>)}
          </select></label>
        </div>
        {['scatter', 'line', 'bar'].includes(output.type) && <div className="algorithm-fields">
          <ColumnChoice label="X" value={output.x} columns={output.type === 'scatter' ? numeric : columns} onChange={(x) => change(index, { x })} />
          <ColumnChoice label="Y" value={output.y} columns={numeric} onChange={(y) => change(index, { y })} />
        </div>}
        {output.type === 'scalar' && <ColumnChoice label="Value (requires exactly one row)" value={output.column} columns={numeric} onChange={(column) => change(index, { column })} />}
        {output.type === 'heatmap' && <p {...pageStyleTarget('text')}>Choose a correlation step as this output’s input.</p>}
        {output.type === 'table' && <ColumnChecks label="Table columns (empty means all)" columns={columns} selected={output.columns} onChange={(selected) => change(index, { columns: selected })} />}
        <button {...pageStyleTarget('button')} type="button" onClick={() => update({ outputs: form.outputs.filter((_, i) => i !== index) })}>Remove output</button>
      </article>
    })}
    <button {...pageStyleTarget('button')} type="button" disabled={form.outputs.length >= 6} onClick={() => update({ outputs: [...form.outputs, { name: 'Result table', type: 'table', input: form.steps.at(-1)?.id || 'input', columns: [] }] })}>Add output</button>
  </>
}
