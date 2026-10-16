import { pageStyleTarget } from '../appearance/styleCore.js'
import SideRail from '../components/layout/SideRail'
import { useEffect, useState } from 'react'
import { fetchDatabaseSchema } from './database/databaseApi'
import DatabaseWorkspace from './database/DatabaseWorkspace'
import './database/database.css'

export default function Database({ account }) {
  const [catalog, setCatalog] = useState(null), [loading, setLoading] = useState(false), [error, setError] = useState('')
  const [revision, setRevision] = useState(0)
  const allowed = account?.role === 'admin'
  useEffect(() => {
    if (!allowed) return undefined
    const controller = new AbortController()
    setLoading(true); setError('')
    fetchDatabaseSchema({ signal: controller.signal }).then(data => {
      if (!controller.signal.aborted) setCatalog(data)
    }).catch(reason => {
      if (!controller.signal.aborted) setError(reason.message || 'Database structure unavailable.')
    }).finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [allowed, revision])
  return <div {...pageStyleTarget('page')} className="page-frame database-page">
    <SideRail />
    <main className="home-shell database-main" aria-label="Database">
      <header className="database-heading"><h1 {...pageStyleTarget('heading')}>Database</h1>
        {allowed && <><span {...pageStyleTarget('text')}>{catalog ? `${catalog.tables.length} tables / views · ${catalog.relationships.length} foreign keys` : 'Read-only workspace'}</span>
          <button {...pageStyleTarget('button')} className="btn btn-secondary" type="button" disabled={loading} onClick={() => setRevision(value => value + 1)}>{loading ? 'Loading structure…' : 'Refresh structure'}</button></>}
      </header>
      {!allowed ? <p role="alert">Superuser access is required.</p> : <>
        {error && <p {...pageStyleTarget('text')} className="database-error" role="alert">{error}{catalog ? ' Showing the previous structure.' : ''}</p>}
        {catalog ? <DatabaseWorkspace key={account.id} catalog={catalog} accountId={account.id} />
          : <p {...pageStyleTarget('text')} role="status">{loading ? 'Reading table and column metadata…' : 'Use Refresh structure to try again.'}</p>}
      </>}
    </main>
  </div>
}
