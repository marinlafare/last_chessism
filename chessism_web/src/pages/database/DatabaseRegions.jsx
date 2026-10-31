import { pageStyleTarget } from '../../appearance/styleCore'
import { TABLE_GROUPS } from './workspaceHierarchy'

const groups = new Map(TABLE_GROUPS.map(group => [group.id, group]))

export default function DatabaseRegions({ regions }) {
  return regions.map(region => {
    const group = groups.get(region.id)
    if (!group) return null
    return <section key={region.id} {...pageStyleTarget('panel')} className="database-region" data-database-region={region.id}
      data-database-family={group.family} aria-label={group.label}
      style={{ left: region.x, top: region.y, width: region.width, height: region.height }}>
      <h2 {...pageStyleTarget('heading')}>{group.label}</h2>
    </section>
  })
}
