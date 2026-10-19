import { pageStyleTarget } from '../../../appearance/styleCore.js'
import MatrixDefinitionList from '../matrices/MatrixDefinitionList'

export default function MatrixFormulaBrowser({ model }) {
  const groups = [
    ['Default', model.formulaInputs.filter((item) => item.builtin_key)],
    ['Your formulas', model.formulaInputs.filter((item) => !item.builtin_key && item.collection !== 'foreign')],
    ['Foreign formulas', model.formulaInputs.filter((item) => item.collection === 'foreign')],
  ]
  return <section {...pageStyleTarget('panel')} className="algorithm-panel" id="matrix-formulas">
    <div className="section-head"><h2 {...pageStyleTarget('title')}>Input formulas</h2>
      <a {...pageStyleTarget('link')} href="/research/matrices">Create formula</a>
    </div>
    <p {...pageStyleTarget('text')}>Select a formula to configure its data. The eye only opens a small sample; neither action starts a calculation.</p>
    {model.working === 'loading' ? <p role="status">Loading formulas…</p> : null}
    {groups.map(([title, definitions]) => <details {...pageStyleTarget('panel')} className="matrix-panel matrix-defaults" key={title}>
      <summary {...pageStyleTarget('title')}>{title}</summary>
      <div className="matrix-defaults-content">
        <MatrixDefinitionList definitions={definitions} onPreview={model.setPreview}
          onSelect={(definition) => model.selectInput(definition.id)} selectedId={model.sourceMatrix?.id}
          onDelete={model.removeInput} disabled={Boolean(model.working)} />
      </div>
    </details>)}
    {model.inputMore ? <button {...pageStyleTarget('button')} type="button" disabled={Boolean(model.working)} onClick={model.moreInputs}>Load more formulas</button> : null}
  </section>
}
