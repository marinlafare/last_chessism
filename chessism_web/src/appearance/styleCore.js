import { scopeForPath } from './pageScopes.js'

export const styleTarget = (id, scope = 'scored_positions') => ({ 'data-style-scope': scope, 'data-style-id': id })
// Evaluated during rendering, including portal content. No DOM scanning or
// identities derived from text, array indices, or CSS class names at runtime.
export const pageStyleTarget = id => styleTarget(id, scopeForPath(typeof window==='undefined'?'/':window.location.pathname))
export const registryScope = (registry, scope) => registry?.scopes?.[scope]
  || (scope===registry?.scope ? {label:'Scored Positions',targets:registry.targets} : null)
export const registryTarget = (registry, scope, id) => registryScope(registry,scope)?.targets?.[id]
export const overrideKey = row => `${row.scope}@${row.target}@${row.property}`
export const sameOverrides = (a, b) => JSON.stringify([...a].sort((x,y)=>overrideKey(x).localeCompare(overrideKey(y)))) === JSON.stringify([...b].sort((x,y)=>overrideKey(x).localeCompare(overrideKey(y))))

export function validValue(spec, value) {
  if (spec.kind === 'color') return typeof value === 'string' && /^#[\da-f]{6}([\da-f]{2})?$/i.test(value)
  if (spec.kind === 'number') return typeof value === 'number' && Number.isFinite(value) && value >= spec.min && value <= spec.max
  return spec.options?.includes(value)
}

export function appearanceCss(registry, rows) {
  if (!registry) return ''
  return rows.flatMap(row => {
    const spec = registry.properties[row.property]
    if (!registryTarget(registry,row.scope,row.target)?.properties.includes(row.property) || !spec || !validValue(spec,row.value)) return []
    // IDs, property names and suffixes come from the server-owned registry only.
    if (!/^[\w.-]+$/.test(row.target) || !/^[\w-]+$/.test(row.scope)) return []
    const value = spec.kind === 'number' ? `${row.value}${spec.unit || ''}` : spec.kind === 'choice' && row.property === 'fontFamily' ? JSON.stringify(row.value) : row.value
    return [`[data-style-scope="${row.scope}"][data-style-id="${row.target}"][data-style-id][data-style-id][data-style-id]${spec.state || ''}{${spec.css}:${value};${row.property==='borderWidth'?'border-style:solid;':''}}`]
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
