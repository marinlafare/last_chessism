export const MARGINS = ['margin', 'marginTop', 'marginRight', 'marginBottom', 'marginLeft']

export function changeStyle(rows, scope, target, property, value) {
  const replaced = property === 'margin' ? MARGINS : [property]
  return [...rows.filter(row => !(row.scope === scope && row.target === target && replaced.includes(row.property))),
    { scope, target, property, value }]
}

export function numericStyleValue(spec, computed) {
  if (spec.css === 'border-radius') {
    const corners = computed.split(/\s+/)
    if (computed.includes('%') || corners.some(corner => corner !== corners[0])) return ''
  }
  if (spec.css === 'margin') {
    const sides = computed.split(/\s+/)
    if (sides.includes('auto') || sides.some(side => side !== sides[0])) return ''
  }
  if (computed === 'auto') return ''
  return parseFloat(computed) || 0
}
