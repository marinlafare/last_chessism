// Only server-owned selectors are used. Emit paint rules only for an explicit
// override: existing palettes, hover feedback and saved presets stay unchanged.
export function chartInteractionCss(selector, target, property) {
  const hoverVariables = {
    barHoverColor: '--bar-hover-color', barHoverBorder: '--bar-hover-border',
    scoredBarHoverColor: '--scored-bar-hover-color', scoredBarHoverBorder: '--scored-bar-hover-border',
  }
  if (!['stroke', 'strokeWidth', ...Object.keys(hoverVariables)].includes(property)) return []
  const hover = Boolean(hoverVariables[property])
  return (target.chartInteraction || []).flatMap(paint => {
    const fillHover = property === (paint.hoverColorProperty || 'barHoverColor')
    const borderHover = property === (paint.hoverBorderProperty || 'barHoverBorder')
    if (hover && !fillHover && !borderHover) return []
    const hoverValue = `var(${hoverVariables[property]})`
    let declaration = ''
    if (paint.kind === 'html') {
      if (property === 'stroke' || property === 'strokeWidth') declaration = 'box-shadow:inset 0 0 0 var(--pie-outline-width,1px) var(--pie-outline-color,transparent);'
      if (fillHover) declaration = `background-color:${hoverValue}!important;filter:none;`
      if (borderHover) declaration = `box-shadow:inset 0 0 0 var(--pie-outline-width,1px) ${hoverValue};filter:none;`
    } else {
      if (property === 'stroke') declaration = 'stroke:var(--pie-outline-color);'
      if (property === 'strokeWidth') declaration = 'stroke-width:var(--pie-outline-width);'
      if (fillHover && paint.kind !== 'outline') {
        declaration = paint.kind === 'line' ? `stroke:${hoverValue};` : `fill:${hoverValue};filter:none;`
      }
      if (borderHover) {
        declaration = paint.kind === 'line' ? `filter:drop-shadow(0 0 1px ${hoverValue});`
          : `stroke:${hoverValue};stroke-width:var(--pie-outline-width,1.5px);filter:none;`
      }
    }
    if (!declaration) return []
    return (hover ? paint.hover : paint.marks).map(pattern => {
      const scoped = pattern.includes('&') ? pattern.replaceAll('&', selector) : `${selector} ${pattern}`
      return `${scoped}{${declaration}}`
    })
  })
}
