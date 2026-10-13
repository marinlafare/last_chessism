import { memo } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore'

function DatabaseTableCard({ table, group, point, selected, related, dimmed, compact, expanded, onSelect, onDrag, onMove, onToggle, onPreview }) {
  return <article {...pageStyleTarget('table.card')} className={`database-table${expanded ? ' expanded' : ''}${selected ? ' selected' : ''}${related ? ' related' : ''}${dimmed ? ' dimmed' : ''}`}
    data-table-id={table.id} data-database-group={group.id} data-database-family={group.family}
    style={{ left: point.x, top: point.y }} aria-label={table.id}>
    <div className="database-table-titlebar"><button {...pageStyleTarget('table.header')} type="button" className="database-table-header"
      title={`${table.id} · ${group.label} · Left-drag to move · Double-click or Enter to ${expanded ? 'collapse' : 'expand'} · Arrow keys move the selected table`}
      aria-label={`Select ${table.id}`} aria-pressed={selected} aria-expanded={expanded}
      onPointerDown={event => onDrag(event, table.id)} onClick={() => onSelect(table.id)}
      onDoubleClick={event => { event.preventDefault(); onToggle(table.id) }}
      onKeyDown={event => {
        if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); onSelect(table.id); onToggle(table.id); return }
        const direction = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] }[event.key]
        if (!direction || !selected) return
        event.preventDefault(); event.stopPropagation()
        onMove(table.id, direction[0] * (event.shiftKey ? 50 : 10), direction[1] * (event.shiftKey ? 50 : 10))
      }}>
      <span><small>{table.schema}</small><strong>{table.name}</strong></span><span className="database-table-disclosure" aria-hidden="true">{expanded ? '▾' : '▸'}</span>
    </button>
    <button {...pageStyleTarget('button')} className="database-table-eye" type="button" aria-label={`Preview rows of ${table.id}`}
      title="Preview 50 sampled rows" onPointerDown={event => event.stopPropagation()}
      onDoubleClick={event => event.stopPropagation()} onClick={() => onPreview(table)}>
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" aria-hidden="true">
        <path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z" /><circle cx="12" cy="12" r="3" />
      </svg>
    </button></div>
    {expanded && <><div className="database-table-meta"><span>{table.kind}{table.is_partition ? ' · partition' : ''}</span>
      <span title="PostgreSQL statistics estimate, not an exact row count">{table.estimated_rows == null ? 'Rows: unknown' : `≈ ${table.estimated_rows.toLocaleString()} rows`}</span></div>
    <ul className="database-columns" tabIndex={0} aria-label={`Columns of ${table.id}`} title="Shift + wheel to scroll columns">
      {compact ? <li className="database-zoom-hint">Zoom in to read columns</li> : table.columns.map(column => <li {...pageStyleTarget('table.column')} key={column.name} title={`${column.name}: ${column.data_type} · ${column.nullable ? 'nullable' : 'not null'}`}>
        <span className="database-key-markers">
          {column.primary_key && <abbr title="Primary key">PK</abbr>}
          {column.foreign_key && <abbr title="Foreign key">FK</abbr>}
        </span>
        <span className="database-column-name">{column.name}</span>
        <span className="database-column-type">{column.data_type}{!column.nullable && <span title="Not null"> *</span>}</span>
      </li>)}
    </ul>
    <footer>{table.columns.length} columns · PK primary · FK foreign · * required</footer></>}
  </article>
}

export default memo(DatabaseTableCard)
