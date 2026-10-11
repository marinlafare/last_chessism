export const styleTarget = id => ({ 'data-style-scope': 'scored_positions', 'data-style-id': id })
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
    if (row.scope !== registry.scope || !registry.targets[row.target]?.properties.includes(row.property) || !spec || !validValue(spec,row.value)) return []
    // IDs, property names and suffixes come from the server-owned registry only.
    if (!/^[\w.-]+$/.test(row.target) || !/^[\w-]+$/.test(row.scope)) return []
    const value = spec.kind === 'number' ? `${row.value}${spec.unit || ''}` : spec.kind === 'choice' && row.property === 'fontFamily' ? JSON.stringify(row.value) : row.value
    return [`[data-style-scope="${row.scope}"][data-style-id="${row.target}"][data-style-id]${spec.state || ''}{${spec.css}:${value};${row.property==='borderWidth'?'border-style:solid;':''}}`]
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
