import { useAppearance } from './AppearanceProvider'
import { fontFamilyCss, normalizeColor, replaceRgb, validValue } from './styleCore'
import { restoreSavedStyle } from './editorTargets'
import { changeStyle, MARGINS, numericStyleValue } from './spacing'
import { supportsCornerRadius, computedCornerRadius } from './cornerRadius'

export default function AppearancePropertyFields({ editable, related = false, idSuffix = '' }) {
  const ctx = useAppearance(), { saved, draft, setDraft } = ctx
  const { scope, id, element, spec: target } = editable
  const matches = row => row.scope === scope && row.target === id
  const set = (property, value) => setDraft(rows => changeStyle(rows, scope, id, property, value))
  const computedStyle = getComputedStyle(element)
  // Keep old palette keys valid for saved presets without exposing unused controls.
  return target.properties.filter(property => !target.hiddenProperties?.includes(property)
    && (property !== 'radius' || supportsCornerRadius(element, computedStyle))).map(property => {
    const spec = saved.registry.properties[property]
    const override = draft.find(row => matches(row) && row.property === property)
    const label = target.propertyLabels?.[property] || spec.label
    const opaqueColor = target.opaqueColors?.includes(property)
    const computed = (property === 'radius' ? computedCornerRadius(element, computedStyle)
      : computedStyle.getPropertyValue(spec.css)).trim() || String(spec.default ?? '')
    const value = override?.value ?? (spec.kind === 'color' ? normalizeColor(computed)
      : spec.kind === 'number' ? numericStyleValue(spec, computed) : computed.split(',')[0].replaceAll('"', '').trim())
    const restoreProperties = property === 'margin' ? MARGINS : [property]
    const unchanged = restoreProperties.every(key => draft.find(row => matches(row) && row.property === key)?.value
      === saved.overrides.find(row => matches(row) && row.property === key)?.value)
    const inputId = `appearance-${related ? 'related-' : ''}${idSuffix}${property}`
    const restoreLabel = `Restore saved ${label.toLowerCase()}`
    return <fieldset className="appearance-property" key={property} disabled={ctx.busy}>
      <label htmlFor={inputId}>{label}</label>
      {spec.kind === 'color' ? <>
        <input id={inputId} type="color" value={normalizeColor(value).slice(0, 7)} onChange={e => set(property, opaqueColor ? e.target.value : replaceRgb(value, e.target.value))} />
        {!opaqueColor && <label className="appearance-opacity">Opacity {Math.round(parseInt(normalizeColor(value).slice(7), 16) / 255 * 100)}%
          <input aria-label={`${label} opacity`} type="range" min="0" max="100"
            value={Math.round(parseInt(normalizeColor(value).slice(7), 16) / 255 * 100)}
            onChange={e => set(property, normalizeColor(value).slice(0, 7) + Math.round(Number(e.target.value) * 2.55).toString(16).padStart(2, '0'))} />
        </label>}
      </> : spec.kind === 'number' ? <input id={inputId} type="number" min={spec.min} max={spec.max} step={spec.step ?? 1} value={value} placeholder={property === 'radius' ? 'Mixed / relative' : 'Mixed / auto'}
        onChange={e => { const number = Number(e.target.value); if (validValue(spec, number)) set(property, number) }} />
        : <select id={inputId} value={spec.options.includes(value) ? value : ''} onChange={e => set(property, e.target.value)}>
          <option value="" disabled>Inherited</option>{spec.options.map(option => <option key={option}>{option}</option>)}
        </select>}
      <button type="button" className="appearance-restore-property" title={restoreLabel} aria-label={restoreLabel}
        disabled={unchanged}
        onClick={() => setDraft(rows => restoreProperties.reduce((current, key) => restoreSavedStyle(current, saved.overrides, scope, id, key), rows))}>
        <span aria-hidden="true">↶</span>
      </button>
      {property === 'fontFamily' ? <div className="appearance-font-preview"
        aria-label={`Font preview: ${value || 'system-ui'}`} style={{ fontFamily: fontFamilyCss(value) }}>
        <span>Chessism · Aa Bb Cc Ññ</span>
        <span>0123456789 · 1.5M · 100%</span>
      </div> : null}
    </fieldset>
  })
}
