import { useCallback, useEffect, useId, useMemo, useRef, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'
import DatabaseTableCard from './DatabaseTableCard'
import DatabaseTablePreview from './DatabaseTablePreview'
import DatabaseRelationships from './DatabaseRelationships'
import DatabaseRegions from './DatabaseRegions'
import { useDatabaseLayout } from './useDatabaseLayout'
import { centerTable, fitBounds, fitTables, inViewport, normalizeLayout, relatedTables, relationshipPath, searchTables, workspaceBounds, zoomAt } from './workspaceGeometry'
import { buildDatabaseHierarchy, regionsForPositions, tableGroup, TABLE_GROUPS } from './workspaceHierarchy'

export default function DatabaseWorkspace({ catalog, accountId }) {
  const { tables, relationships } = catalog
  const { layout, setLayout, storageError, notice, hasSavedLayout, saveLayout, loadLayout } = useDatabaseLayout(tables, accountId)
  const [selected, setSelected] = useState(null), [query, setQuery] = useState(''), [searchOpen, setSearchOpen] = useState(false)
  const [size, setSize] = useState({ width: 1000, height: 600, measured: false }), [dragging, setDragging] = useState(false)
  const [preview, setPreview] = useState(null)
  const canvas = useRef(null), gesture = useRef(null), latest = useRef(layout)
  latest.current = layout
  const marker = `database-arrow-${useId().replaceAll(':', '')}`
  const matches = useMemo(() => searchTables(tables, query), [tables, query])
  const tableIndex = useMemo(() => new Map(tables.map(t => [t.id, t])), [tables])
  const related = useMemo(() => relatedTables(selected, relationships), [selected, relationships])
  const links = useMemo(() => relationships.filter(r => r.source_table === selected || r.target_table === selected), [selected, relationships])
  const expanded = useMemo(() => new Set(layout.expanded), [layout.expanded])
  const hierarchy = useMemo(() => buildDatabaseHierarchy(tables), [tables])
  const groups = useMemo(() => new Map(tables.map(table => [table.id, tableGroup(table)])), [tables])
  const regions = useMemo(() => regionsForPositions(tables, layout.positions), [tables, layout.positions])
  const field = useMemo(() => workspaceBounds(hierarchy.positions, layout.positions, layout.expanded), [hierarchy.positions, layout.positions, layout.expanded])
  const shown = tables.filter(t => layout.positions[t.id] && (dragging === t.id || inViewport(layout.positions[t.id], layout.view, size, expanded.has(t.id))))

  useEffect(() => {
    const node = canvas.current
    const observer = new ResizeObserver(([entry]) => setSize({ width: entry.contentRect.width, height: entry.contentRect.height, measured: true }))
    observer.observe(node)
    const wheel = event => {
      if (gesture.current) { event.preventDefault(); return }
      event.preventDefault()
      const rect = node.getBoundingClientRect()
      const delta = event.deltaY * (event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? size.height : 1)
      const columns = event.target.closest?.('.database-columns')
      if (event.shiftKey && columns) { columns.scrollTop += delta; return }
      setLayout(current => ({ ...current, view: zoomAt(current.view, current.view.zoom * Math.exp(-Math.max(-300, Math.min(300, delta)) * 0.002),
        { x: event.clientX - rect.left, y: event.clientY - rect.top }) }))
    }
    node.addEventListener('wheel', wheel, { passive: false })
    return () => { observer.disconnect(); node.removeEventListener('wheel', wheel) }
  }, [setLayout, size.height])

  useEffect(() => {
    if (!size.measured || !layout.autoFrame) return
    setLayout(current => current.autoFrame ? { ...current, autoFrame: false,
      view: hierarchy.overviewBounds ? fitBounds(hierarchy.overviewBounds, size) : fitTables(current.positions, size),
    } : current)
  }, [size, layout.autoFrame, hierarchy, setLayout])

  const select = useCallback(id => setSelected(id), [])
  const showPreview = useCallback(table => setPreview(table), [])
  const focusTable = id => {
    if (!layout.positions[id]) return
    setSelected(id); setSearchOpen(false)
    setLayout(current => ({ ...current, view: centerTable(current.positions[id], size) }))
  }
  const changeZoom = factor => setLayout(current => ({ ...current,
    view: zoomAt(current.view, current.view.zoom * factor, { x: size.width / 2, y: size.height / 2 }) }))
  const fit = () => setLayout(current => ({ ...current, view: fitTables(current.positions, size, current.expanded) }))
  const focusGroup = id => {
    const positions = Object.fromEntries(tables.filter(table => groups.get(table.id).id === id && layout.positions[table.id])
      .map(table => [table.id, layout.positions[table.id]]))
    setSelected(null); setSearchOpen(false)
    setLayout(current => ({ ...current, view: fitTables(positions, size, current.expanded) }))
    canvas.current?.focus({ preventScroll: true })
  }
  const toggleTable = useCallback(id => setLayout(current => ({ ...current,
    expanded: current.expanded.includes(id) ? current.expanded.filter(value => value !== id) : [...current.expanded, id],
  })), [setLayout])
  const moveTable = useCallback((id, dx, dy) => setLayout(current => ({ ...current, positions: { ...current.positions,
    [id]: { x: Math.max(-100000, Math.min(100000, current.positions[id].x + dx)), y: Math.max(-100000, Math.min(100000, current.positions[id].y + dy)) },
  } })), [setLayout])

  const start = useCallback((event, id = null) => {
    if (![0, 1].includes(event.button) || gesture.current) return
    event.preventDefault()
    if (event.button === 1) id = null
    if (id) setSelected(id)
    // Capture on the header for a table drag so clicks/double-clicks still reach
    // that button. Middle-button gestures capture on the canvas and only pan.
    const node = id ? event.currentTarget : canvas.current
    node.focus({ preventScroll: true })
    gesture.current = { id, node, pointerId: event.pointerId, x: event.clientX, y: event.clientY, layout: latest.current }
    node.setPointerCapture(event.pointerId); setDragging(id || 'canvas')
    setSearchOpen(false)
  }, [])
  const move = event => {
    const active = gesture.current
    if (!active || active.pointerId !== event.pointerId) return
    const dx = event.clientX - active.x, dy = event.clientY - active.y
    const { view, positions } = active.layout
    setLayout(current => active.id ? { ...current, positions: { ...current.positions, [active.id]: {
      x: Math.max(-100000, Math.min(100000, positions[active.id].x + dx / view.zoom)),
      y: Math.max(-100000, Math.min(100000, positions[active.id].y + dy / view.zoom)),
    } } } : { ...current, view: { ...view, x: Math.max(-200000, Math.min(200000, view.x + dx)), y: Math.max(-200000, Math.min(200000, view.y + dy)) } })
  }
  const finish = useCallback((cancel = false) => {
    const active = gesture.current
    if (!active) return
    gesture.current = null; setDragging(false)
    if (cancel) setLayout(active.layout)
    if (active.node.hasPointerCapture(active.pointerId)) active.node.releasePointerCapture(active.pointerId)
  }, [setLayout])
  useEffect(() => {
    const blur = () => finish()
    window.addEventListener('blur', blur)
    return () => window.removeEventListener('blur', blur)
  }, [finish])

  return <>
    <div {...pageStyleTarget('toolbar')} className="database-tools">
      <div className="database-search">
        <label htmlFor="database-search">Find a table or column</label>
        <input {...pageStyleTarget('field')} id="database-search" type="search" autoComplete="off" value={query}
          onClick={() => setSearchOpen(true)}
          onChange={event => { setQuery(event.target.value); setSearchOpen(true) }} onFocus={() => setSearchOpen(true)}
          onKeyDown={event => { if (event.key === 'Escape') setSearchOpen(false); if (event.key === 'Enter' && matches[0]) focusTable(matches[0].id) }} />
        {searchOpen && query.trim() && <div {...pageStyleTarget('popup')} className="database-search-results" aria-label="Search results">
          <small>{matches.length} matching tables</small>
          {matches.slice(0, 30).map(table => <button {...pageStyleTarget('button')} key={table.id} type="button" onClick={() => focusTable(table.id)}>{table.id}</button>)}
          {matches.length > 30 && <small>Refine your search to see more.</small>}
        </div>}
      </div>
      <div className="database-zoom" aria-label="Canvas controls">
        <select {...pageStyleTarget('field')} aria-label="Go to table group" value="" onChange={event => focusGroup(event.target.value)}>
          <option value="">Go to group…</option>
          {TABLE_GROUPS.filter(group => Object.values(hierarchy.tableGroups).includes(group.id)).map(group => <option key={group.id} value={group.id}>{group.label}</option>)}
        </select>
        <button {...pageStyleTarget('button')} type="button" aria-label="Zoom out" onClick={() => changeZoom(1 / 1.2)}>−</button>
        <output aria-label="Canvas zoom">{Number((layout.view.zoom * 100).toFixed(layout.view.zoom < 0.01 ? 1 : 0))}%</output>
        <button {...pageStyleTarget('button')} type="button" aria-label="Zoom in" onClick={() => changeZoom(1.2)}>+</button>
        <button {...pageStyleTarget('button')} type="button" onClick={fit}>Fit all</button>
        <button {...pageStyleTarget('button')} type="button" onClick={() => setLayout(current => ({ ...current, view: fitBounds(field, size) }))}>Fit field</button>
        <button {...pageStyleTarget('button')} type="button" onClick={saveLayout}>Save layout</button>
        <button {...pageStyleTarget('button')} type="button" disabled={!hasSavedLayout} onClick={() => {
          if (loadLayout()) { setSelected(null); setSearchOpen(false) }
        }}>Load saved layout</button>
        <button {...pageStyleTarget('button')} type="button" onClick={() => {
          if (!window.confirm('Return to the game-centered default? Unsaved adjustments will be discarded. Your saved layout will stay available.')) return
          const fresh = normalizeLayout(null, tables)
          setSelected(null); setSearchOpen(false)
          setLayout({ ...fresh, autoFrame: false, view: hierarchy.overviewBounds ? fitBounds(hierarchy.overviewBounds, size) : fitTables(fresh.positions, size) })
        }}>Reset to default</button>
      </div>
    </div>
    <p {...pageStyleTarget('text')} className="database-hint">Left-drag a title to move its table · double-click or Enter to expand · hold the mouse wheel and drag to pan · wheel or + / − to zoom · Shift + wheel to scroll columns.</p>
    <div {...pageStyleTarget('canvas')} className={`database-canvas${dragging ? ' dragging' : ''}`} ref={canvas} tabIndex={0}
      role="region" aria-label="Database table canvas"
      onPointerDownCapture={event => { if (event.button === 1) start(event) }}
      onMouseDownCapture={event => { if (event.button === 1) event.preventDefault() }}
      onAuxClick={event => { if (event.button === 1) event.preventDefault() }}
      onPointerDown={event => {
        setSearchOpen(false)
        if (!event.target.closest('.database-table')) start(event)
      }} onPointerMove={move}
      onPointerUp={event => { if (gesture.current?.pointerId === event.pointerId) finish() }}
      onPointerCancel={event => { if (gesture.current?.pointerId === event.pointerId) finish(true) }}
      onLostPointerCapture={event => { if (gesture.current?.pointerId === event.pointerId) finish() }}
      onKeyDown={event => {
        if (event.key === 'Escape') { finish(true); setSelected(null); setSearchOpen(false); return }
        if (event.ctrlKey || event.metaKey || event.altKey || gesture.current) return
        if (event.target.closest('input, textarea, select, [contenteditable="true"]')) return
        if (['+', '=', '-'].includes(event.key)) { event.preventDefault(); changeZoom(event.key === '-' ? 1 / 1.2 : 1.2); return }
        if (event.target !== event.currentTarget) return
        const shift = { ArrowLeft: [70, 0], ArrowRight: [-70, 0], ArrowUp: [0, 70], ArrowDown: [0, -70] }[event.key]
        if (shift) { event.preventDefault(); setLayout(current => ({ ...current, view: { ...current.view, x: current.view.x + shift[0], y: current.view.y + shift[1] } })) }
        if (event.key === 'Home') { event.preventDefault(); fit() }
      }}>
      <div className="database-world" style={{ transform: `translate(${layout.view.x}px, ${layout.view.y}px) scale(${layout.view.zoom})` }}>
        <div className="database-field" aria-hidden="true" style={{ left: field.left, top: field.top, width: field.width, height: field.height }} />
        <DatabaseRegions regions={regions} />
        <svg className="database-connectors" aria-hidden="true">
          <defs><marker id={marker} markerWidth="9" markerHeight="9" refX="8" refY="4" orient="auto"><path d="M0 0 L8 4 L0 8" fill="none" stroke="currentColor" /></marker></defs>
          {links.map(link => layout.positions[link.source_table] && layout.positions[link.target_table] && <path key={link.id} data-relationship-id={link.id}
            d={relationshipPath(layout.positions[link.source_table], layout.positions[link.target_table], link.source_table === link.target_table)} markerEnd={`url(#${marker})`} />)}
        </svg>
        {shown.map(table => <DatabaseTableCard key={table.id} table={table} group={groups.get(table.id)} point={layout.positions[table.id]}
          selected={selected === table.id} related={related.has(table.id)} dimmed={Boolean(selected) && selected !== table.id && !related.has(table.id)}
          compact={layout.view.zoom < 0.45} expanded={expanded.has(table.id)}
          onSelect={select} onDrag={start} onMove={moveTable} onToggle={toggleTable} onPreview={showPreview} />)}
      </div>
      {!tables.length && <p className="database-empty">No accessible application tables or views found.</p>}
    </div>
    <p {...pageStyleTarget('text')} className="database-hint" role="status">{storageError || notice || 'The default hierarchy opens every time. Adjustments are temporary until you choose Save layout; use Load saved layout to reopen your personal arrangement.'}</p>
    {tableIndex.has(selected) && <DatabaseRelationships table={tableIndex.get(selected)} relationships={links} onSelect={focusTable} onClose={() => setSelected(null)} />}
    {preview && <DatabaseTablePreview key={preview.id} table={preview} onClose={() => setPreview(null)} />}
  </>
}
