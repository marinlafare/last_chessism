import { registryTarget } from './styleCore.js'

// Expose a slice's own colors and borders alongside the registered chart's
// shared styling, without changing other charts on the page.
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
