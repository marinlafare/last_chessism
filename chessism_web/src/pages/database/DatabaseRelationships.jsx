import { pageStyleTarget } from '../../appearance/styleCore'

export default function DatabaseRelationships({ table, relationships, onSelect, onClose }) {
  return <section {...pageStyleTarget('relationships.panel')} className="database-relationships" aria-label="Selected table relationships">
    <header><h2 {...pageStyleTarget('title')}>{table.id}</h2>
      <button {...pageStyleTarget('button')} type="button" onClick={onClose} aria-label="Clear table selection">×</button></header>
    <p {...pageStyleTarget('text')}>{relationships.length} declared foreign-key relationship{relationships.length === 1 ? '' : 's'}. Arrows point to the referenced table.</p>
    {!relationships.length && <p>No declared foreign keys. Similar column names are not treated as relationships.</p>}
    <ul>{relationships.map(relation => {
      const outgoing = relation.source_table === table.id
      const other = outgoing ? relation.target_table : relation.source_table
      return <li key={relation.id}>
        <span className="database-relationship-direction">{outgoing ? 'References' : 'Referenced by'}</span>
        <button {...pageStyleTarget('button')} type="button" onClick={() => onSelect(other)}>{other}</button>
        <code>{relation.source_columns.join(', ')} → {relation.target_columns.join(', ')}</code>
        <small>{relation.name}</small>
      </li>
    })}</ul>
  </section>
}
