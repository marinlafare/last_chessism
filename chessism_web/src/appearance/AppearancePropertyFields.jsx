import { useAppearance } from './AppearanceProvider'
import { normalizeColor, replaceRgb, validValue } from './styleCore'
import { restoreSavedStyle } from './editorTargets'

export default function AppearancePropertyFields({ editable, related = false }) {
  const ctx = useAppearance(), { saved, draft, setDraft } = ctx
  const { scope, id, element, spec: target } = editable
  const matches = row => row.scope === scope && row.target === id
  const set = (property, value) => setDraft(rows => [
    ...rows.filter(row => !(matches(row) && row.property === property)),
    { scope, target: id, property, value },
  ])
  return target.properties.map(property => {
    const spec = saved.registry.properties[property]
    const override = draft.find(row => matches(row) && row.property === property)
    const lastSaved = saved.overrides.find(row => matches(row) && row.property === property)
    const label = target.propertyLabels?.[property] || spec.label
    const computed = getComputedStyle(element).getPropertyValue(spec.css).trim() || String(spec.default ?? '')
    const value = override?.value ?? (spec.kind === 'color' ? normalizeColor(computed)
      : spec.kind === 'number' ? parseFloat(computed) || 0 : computed.split(',')[0].replaceAll('"', '').trim())
    const inputId = `appearance-${related ? 'related-' : ''}${property}`
    const restoreLabel = `Restore saved ${label.toLowerCase()}`
    return <fieldset className="appearance-property" key={property} disabled={ctx.busy}>
      <label htmlFor={inputId}>{label}</label>
      {spec.kind === 'color' ? <>
        <input id={inputId} type="color" value={normalizeColor(value).slice(0, 7)} onChange={e => set(property, replaceRgb(value, e.target.value))} />
        <label className="appearance-opacity">Opacity {Math.round(parseInt(normalizeColor(value).slice(7), 16) / 255 * 100)}%
          <input aria-label={`${label} opacity`} type="range" min="0" max="100"
            value={Math.round(parseInt(normalizeColor(value).slice(7), 16) / 255 * 100)}
            onChange={e => set(property, normalizeColor(value).slice(0, 7) + Math.round(Number(e.target.value) * 2.55).toString(16).padStart(2, '0'))} />
        </label>
      </> : spec.kind === 'number' ? <input id={inputId} type="number" min={spec.min} max={spec.max} value={value}
        onChange={e => { const number = Number(e.target.value); if (validValue(spec, number)) set(property, number) }} />
        : <select id={inputId} value={spec.options.includes(value) ? value : ''} onChange={e => set(property, e.target.value)}>
          <option value="" disabled>Inherited</option>{spec.options.map(option => <option key={option}>{option}</option>)}
        </select>}
      <button type="button" className="appearance-restore-property" title={restoreLabel} aria-label={restoreLabel}
        disabled={override?.value === lastSaved?.value}
        onClick={() => setDraft(rows => restoreSavedStyle(rows, saved.overrides, scope, id, property))}>
        <span aria-hidden="true">↶</span>
      </button>
    </fieldset>
  })
}
