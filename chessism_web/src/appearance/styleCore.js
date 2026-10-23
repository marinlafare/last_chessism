import { scopeForPath } from './pageScopes.js'
import { chartInteractionCss } from './chartInteractionCss.js'

export const styleTarget = (id, scope = 'scored_positions') => ({ 'data-style-scope': scope, 'data-style-id': id })
// Evaluated during rendering, including portal content. No DOM scanning or
// identities derived from text, array indices, or CSS class names at runtime.
export const pageStyleTarget = id => styleTarget(id, scopeForPath(typeof window==='undefined'?'/':window.location.pathname))
export const registryScope = (registry, scope) => registry?.scopes?.[scope]
  || (scope===registry?.scope ? {label:'Scored Positions',targets:registry.targets} : null)
export const registryTarget = (registry, scope, id) => {
  const target = registryScope(registry,scope)?.targets?.[id]
  return target ? { ...target, properties: [...new Set([...target.properties, ...(registry.universalProperties || [])])] } : null
}
export const overrideKey = row => `${row.scope}@${row.target}@${row.property}`
export const sameOverrides = (a, b) => JSON.stringify([...a].sort((x,y)=>overrideKey(x).localeCompare(overrideKey(y)))) === JSON.stringify([...b].sort((x,y)=>overrideKey(x).localeCompare(overrideKey(y))))

// Split formerly shared controls without resetting existing presets. Seed only
// missing values when a saved snapshot is accepted, never while editing a draft.
// The next normal save persists the independent rows; reads do not change the DB.
export function seedIndependentStyles(registry, rows) {
  const result = [...rows], existing = new Set(rows.map(overrideKey))
  // Resolve chains in one pass over the growing result: a newly seeded source
  // can seed its descendants too, so reloading does not introduce new styles.
  const descendants = new Map()
  for (const [scope, spec] of Object.entries(registry?.scopes || {})) {
    for (const [target, definition] of Object.entries(spec.targets || {})) {
      if (!definition.initialStyleFrom) continue
      const key = `${scope}@${definition.initialStyleFrom}`
      descendants.set(key, [...(descendants.get(key) || []), { target, definition: registryTarget(registry, scope, target) }])
    }
  }
  for (const source of result) for (const { target, definition } of descendants.get(`${source.scope}@${source.target}`) || []) {
    if (!definition.properties.includes(source.property)) continue
    const row = { ...source, target }, key = overrideKey(row)
    if (!existing.has(key)) { result.push(row); existing.add(key) }
  }
  return result
}

export function validValue(spec, value) {
  if (spec.kind === 'color') return typeof value === 'string' && /^#[\da-f]{6}([\da-f]{2})?$/i.test(value)
  if (spec.kind === 'number') return typeof value === 'number' && Number.isFinite(value) && value >= spec.min && value <= spec.max
  return spec.options?.includes(value)
}

export function fontFamilyCss(family) {
  // CSS generic families are keywords, not quoted names of installed fonts.
  if (!family || family === 'system-ui') return 'system-ui, sans-serif'
  const fallback = ['IBM Plex Mono', 'JetBrains Mono'].includes(family)
    ? 'ui-monospace, monospace'
    : ['Lora', 'Playfair Display', 'Source Serif 4'].includes(family) ? 'serif' : 'system-ui, sans-serif'
  return `${JSON.stringify(family)}, ${fallback}`
}

export function appearanceCss(registry, rows) {
  if (!registry) return ''
  // Longhand sides always override the all-sides baseline, including after
  // database reloads (row order is not guaranteed).
  const ordered = [...rows.filter(row => row.property === 'margin'), ...rows.filter(row => row.property !== 'margin')]
  return ordered.flatMap(row => {
    const spec = registry.properties[row.property]
    const target = registryTarget(registry,row.scope,row.target)
    if (!target?.properties.includes(row.property) || !spec || !validValue(spec,row.value)) return []
    // IDs, property names and suffixes come from the server-owned registry only.
    if (!/^[\w.-]+$/.test(row.target) || !/^[\w-]+$/.test(row.scope)) return []
    const value = spec.kind === 'number' ? `${row.value}${spec.unit || ''}` : spec.kind === 'choice' && row.property === 'fontFamily' ? fontFamilyCss(row.value) : row.value
    const selector = `[data-style-scope="${row.scope}"][data-style-id="${row.target}"][data-style-id][data-style-id][data-style-id]`
    // Only opted-in toggles retain their hover colors while pressed. A
    // fixed attribute selector also keeps the rule independent of row order.
    const pressed = ((target.pressedUsesHoverColor && row.property === 'hoverColor')
      || (target.pressedUsesHoverBackground && row.property === 'hoverBackground'))
      ? `,${selector}[aria-pressed="true"]` : ''
    const state = spec.state === ':hover' && target.hoverState ? target.hoverState : spec.state || ''
    // Padding belongs inside the chart's measured box, not outside a 100%-wide
    // SVG, so it cannot push the chart outside its parent container.
    const chartBox = row.property === 'padding' && target.chartInteraction ? 'box-sizing:border-box;' : ''
    return [`${selector}${state}${pressed}{${spec.css}:${value};${row.property==='borderWidth'?'border-style:solid;':''}${chartBox}}`,
      ...(row.property === 'radius' ? [`${selector}:where(rect){rx:${value};ry:${value};}`] : []),
      ...chartInteractionCss(selector, target, row.property)]
  }).join('\n')
}

export function normalizeColor(value) {
  if (/^#[\da-f]{6}([\da-f]{2})?$/i.test(value)) return value.length===7?`${value}ff`:value
  if (value === 'transparent') return '#00000000'
  const parts = String(value).match(/[\d.]+/g)?.map(Number)
  if (!parts || parts.length < 3) return '#000000ff'
  return '#' + [...parts.slice(0,3), (parts[3] ?? 1)*255].map(n=>Math.round(Math.max(0,Math.min(255,n))).toString(16).padStart(2,'0')).join('')
}

export function replaceRgb(current, rgb) {
  const alpha = normalizeColor(current).slice(7)
  return rgb + (alpha === '00' ? 'ff' : alpha)
}
