const HTML = 'http://www.w3.org/1999/xhtml'
const SVG = 'http://www.w3.org/2000/svg'
const TABLE_INTERNALS = new Set(['table-row', 'table-row-group', 'table-header-group', 'table-footer-group', 'table-column', 'table-column-group'])

// Capability detection only; target identities and permitted values still come
// from the server registry. Do not offer a control that cannot affect the box.
export function supportsCornerRadius(element, computed) {
  if (element.namespaceURI === SVG) return ['svg', 'rect'].includes(element.localName)
  if (element.namespaceURI !== HTML) return false
  if (['option', 'optgroup'].includes(element.localName) || computed.display === 'contents') return false
  if (TABLE_INTERNALS.has(computed.display)) return false
  return !(['table', 'inline-table', 'table-cell'].includes(computed.display) && computed.borderCollapse === 'collapse')
}

export function computedCornerRadius(element, computed) {
  if (element.namespaceURI !== SVG || element.localName !== 'rect') return computed.borderRadius
  // SVG rectangles use rx/ry, not CSS border-radius.
  const x = computed.getPropertyValue('rx'), y = computed.getPropertyValue('ry')
  if (x === 'auto' && y === 'auto') return '0px'
  if (x === 'auto') return y
  if (y === 'auto' || x === y) return x
  return `${x} ${y}`
}
