import { registryTarget } from './styleCore.js'

// A slice edits its own fill and its registered circle's outline in one popup.
// Keep border overrides on the SVG parent so sibling slices share one border.
export function editorTargets(registry, selection) {
  if (!selection) return []
  const spec = registryTarget(registry, selection.scope, selection.id)
  if (!spec) return []
  const targets = [{ ...selection, spec }]
  if (!spec.editorParent) return targets
  let parent = selection.element.parentElement
  while (parent) {
    if (parent.dataset.styleScope === selection.scope && parent.dataset.styleId === spec.editorParent) {
      const parentSpec = registryTarget(registry, selection.scope, spec.editorParent)
      if (parentSpec) targets.push({ element: parent, scope: selection.scope, id: spec.editorParent, spec: parentSpec })
      break
    }
    parent = parent.parentElement
  }
  return targets
}

export function restoreSavedStyle(draft, saved, scope, target, property = null) {
  const matches = row => row.scope === scope && row.target === target && (property === null || row.property === property)
  return [...draft.filter(row => !matches(row)), ...saved.filter(matches)]
}
