import { useEffect, useMemo, useRef, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import { formatNumber } from '../utils/formatters'
import {
  constructMatrix,
  deleteMatrixArtifact,
  estimateMatrix,
  fetchMatrixArtifacts,
  fetchMatrixCatalog,
} from './research/matrixApi'
import './research/matricesConstructor.css'

const EMPTY_FILTERS = {
  players: '',
  modes: ['bullet', 'blitz', 'rapid'],
  date_from: '',
  date_to: '',
  min_moves: 0,
  analyzed_only: false,
  max_rows: 100000,
}

const formatBytes = (value) => {
  const bytes = Number(value || 0)
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const power = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)))
  return `${(bytes / (1024 ** power)).toFixed(power > 1 ? 2 : 0)} ${units[power]}`
}

const artifactProgress = (artifact) => {
  const progress = artifact.progress || {}
  const total = Number(progress.total || artifact.estimate?.selected_rows || 0)
  const processed = Number(progress.processed || 0)
  return total ? Math.min(100, (processed / total) * 100) : 0
}

const profileRange = (profile) => {
  if (profile.data_type === 'category') return `${formatNumber(profile.category_count || 0)} categories`
  if (profile.minimum === null || profile.maximum === null) return 'no numeric values'
  const minimum = Number(profile.minimum)
  const maximum = Number(profile.maximum)
  return `${minimum.toLocaleString(undefined, { maximumFractionDigits: 3 })} → ${maximum.toLocaleString(undefined, { maximumFractionDigits: 3 })}`
}

function ArtifactInspection({ artifact }) {
  const profiles = [
    ...(artifact.result?.profiles?.features || []).map((profile) => ({ ...profile, role: 'feature' })),
    ...(artifact.result?.profiles?.labels || []).map((profile) => ({ ...profile, role: 'label' })),
  ]
  if (!profiles.length) return <p className="matrix-empty">This artifact predates snapshot profiling.</p>
  return (
    <div className="matrix-inspection">
      <div className="matrix-inspection-summary">
        <span>format v{artifact.result?.version || 1}</span>
        <span>{artifact.result?.storage_layout?.replaceAll('_', ' ') || 'legacy dense array'}</span>
        <span>{artifact.result?.snapshot_isolation || 'unknown isolation'}</span>
      </div>
      <div className="matrix-profile-head"><span>Column</span><span>Type</span><span>Missing</span><span>Profile</span></div>
      {profiles.map((profile) => (
        <div className="matrix-profile-row" key={`${profile.role}-${profile.key}`}>
          <span><strong>{profile.key}</strong><small>{profile.role}</small></span>
          <code>{profile.storage_dtype}</code>
          <span>{(Number(profile.missing_fraction || 0) * 100).toFixed(2)}%</span>
          <span>{profileRange(profile)}</span>
        </div>
      ))}
    </div>
  )
}

function ArtifactList({ artifacts, onDelete }) {
  const [expandedId, setExpandedId] = useState('')
  if (!artifacts.length) return <p className="matrix-empty">No matrix snapshots have been constructed.</p>
  return (
    <div className="matrix-artifact-list">
      {artifacts.map((artifact) => (
        <article className="matrix-artifact" key={artifact.id}>
          <div className="matrix-artifact-head">
            <div>
              <strong>{artifact.name}</strong>
              <span>{artifact.row_type.replaceAll('_', ' ')} · {artifact.created_at ? new Date(artifact.created_at).toLocaleString() : '-'}</span>
            </div>
            <span className={`matrix-state ${artifact.status}`}>{artifact.status}</span>
          </div>
          {['queued', 'running'].includes(artifact.status) ? (
            <div className="matrix-progress" aria-live="polite">
              <div><span>{artifact.progress?.detail || 'Waiting for the research worker.'}</span><b>{artifactProgress(artifact).toFixed(0)}%</b></div>
              <div className="matrix-progress-track"><i style={{ width: `${artifactProgress(artifact)}%` }} /></div>
            </div>
          ) : null}
          {artifact.status === 'complete' ? (
            <>
              <div className="matrix-artifact-result">
                <span>{formatNumber(artifact.row_count)} rows</span>
                <span>{artifact.feature_count} features</span>
                <span>{artifact.label_count} labels</span>
                <span>{formatBytes(artifact.size_bytes)}</span>
                <code title={artifact.artifact_path}>{artifact.artifact_path}</code>
              </div>
              <button
                className="matrix-inspect-button"
                type="button"
                onClick={() => setExpandedId((current) => current === artifact.id ? '' : artifact.id)}
              >
                {expandedId === artifact.id ? 'hide inspection' : 'inspect snapshot'}
              </button>
              {expandedId === artifact.id ? <ArtifactInspection artifact={artifact} /> : null}
            </>
          ) : null}
          {artifact.error ? <p className="matrix-error">{artifact.error}</p> : null}
          {!['queued', 'running'].includes(artifact.status) ? (
            <button className="matrix-delete" type="button" onClick={() => onDelete(artifact)}>delete snapshot</button>
          ) : null}
        </article>
      ))}
    </div>
  )
}

export default function MatricesConstructor() {
  const [catalog, setCatalog] = useState(null)
  const [rowTypeKey, setRowTypeKey] = useState('game_player')
  const [features, setFeatures] = useState([])
  const [labels, setLabels] = useState([])
  const [filters, setFilters] = useState(EMPTY_FILTERS)
  const [name, setName] = useState('Player-game research matrix')
  const [estimate, setEstimate] = useState(null)
  const [artifacts, setArtifacts] = useState([])
  const [working, setWorking] = useState('')
  const [error, setError] = useState('')
  const estimateRevision = useRef(0)
  const hasActiveArtifacts = artifacts.some((item) => ['queued', 'running'].includes(item.status))

  const rowTypes = catalog?.row_types || []
  const rowType = useMemo(
    () => rowTypes.find((item) => item.key === rowTypeKey),
    [rowTypes, rowTypeKey]
  )

  const loadArtifacts = async () => {
    const payload = await fetchMatrixArtifacts()
    setArtifacts(Array.isArray(payload.artifacts) ? payload.artifacts : [])
  }

  useEffect(() => {
    let active = true
    Promise.all([fetchMatrixCatalog(), fetchMatrixArtifacts()])
      .then(([catalogPayload, artifactsPayload]) => {
        if (!active) return
        setCatalog(catalogPayload)
        setArtifacts(Array.isArray(artifactsPayload.artifacts) ? artifactsPayload.artifacts : [])
        const initial = catalogPayload.row_types?.find((item) => item.key === 'game_player')
        setFeatures((initial?.columns || []).filter((item) => item.default).map((item) => item.key))
      })
      .catch((loadError) => { if (active) setError(loadError.message || 'Unable to load the matrix constructor.') })
    return () => { active = false }
  }, [])

  useEffect(() => {
    if (!hasActiveArtifacts) return undefined
    let active = true
    let timer
    const refresh = async () => {
      try {
        const response = await fetchMatrixArtifacts()
        if (active) setArtifacts(Array.isArray(response.artifacts) ? response.artifacts : [])
      } catch (loadError) {
        if (active) setError(loadError.message || 'Unable to refresh matrix jobs.')
      } finally {
        if (active) timer = window.setTimeout(refresh, 3000)
      }
    }
    timer = window.setTimeout(refresh, 3000)
    return () => { active = false; window.clearTimeout(timer) }
  }, [hasActiveArtifacts])

  const invalidate = () => { estimateRevision.current += 1; setEstimate(null) }
  const changeRowType = (key) => {
    const next = rowTypes.find((item) => item.key === key)
    setRowTypeKey(key)
    setFeatures((next?.columns || []).filter((item) => item.default).map((item) => item.key))
    setLabels([])
    setName(`${next?.label || 'Research'} matrix`)
    invalidate()
  }
  const toggleMode = (mode) => {
    setFilters((current) => ({
      ...current,
      modes: current.modes.includes(mode)
        ? current.modes.filter((item) => item !== mode)
        : [...current.modes, mode],
    }))
    invalidate()
  }
  const updateFilter = (key, value) => {
    setFilters((current) => ({ ...current, [key]: value }))
    invalidate()
  }
  const toggleColumn = (key, role) => {
    if (role === 'feature') {
      setFeatures((current) => current.includes(key) ? current.filter((item) => item !== key) : [...current, key])
      setLabels((current) => current.filter((item) => item !== key))
    } else {
      setLabels((current) => current.includes(key) ? current.filter((item) => item !== key) : [...current, key])
      setFeatures((current) => current.filter((item) => item !== key))
    }
    invalidate()
  }

  const payload = () => ({
    name,
    row_type: rowTypeKey,
    feature_columns: features,
    label_columns: labels,
    filters: {
      ...filters,
      players: filters.players.split(',').map((item) => item.trim()).filter(Boolean),
      min_moves: Number(filters.min_moves || 0),
      max_rows: Number(filters.max_rows || 100000),
      date_from: filters.date_from || null,
      date_to: filters.date_to || null,
    },
  })

  const preview = async () => {
    const revision = estimateRevision.current
    setWorking('estimate')
    setError('')
    try {
      const nextEstimate = await estimateMatrix(payload())
      if (revision === estimateRevision.current) setEstimate(nextEstimate)
    } catch (previewError) {
      if (revision === estimateRevision.current) setError(previewError.message || 'Unable to estimate this matrix.')
    } finally {
      setWorking('')
    }
  }

  const build = async () => {
    setWorking('build')
    setError('')
    try {
      await constructMatrix(payload())
      setEstimate(null)
      await loadArtifacts()
    } catch (buildError) {
      setError(buildError.message || 'Unable to queue this matrix.')
    } finally {
      setWorking('')
    }
  }

  const remove = async (artifact) => {
    if (!window.confirm(`Delete the matrix snapshot “${artifact.name}”?`)) return
    try {
      await deleteMatrixArtifact(artifact.id)
      await loadArtifacts()
    } catch (deleteError) {
      setError(deleteError.message || 'Unable to delete this snapshot.')
    }
  }

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="matrix-main">
          <section className="matrix-hero">
            <div>
              <p className="eyebrow">SUPERUSER RESEARCH</p>
              <h1>Matrices constructor</h1>
              <p>Compose an allowlisted database snapshot for later CUDA or TensorFlow work.</p>
            </div>
            <a className="matrix-back" href="/research">Research index</a>
          </section>
          {error ? <div className="status-banner warn">{error}</div> : null}

          <section className="matrix-panel">
            <div className="section-head"><div><p className="eyebrow">1 · ROW UNIT</p><h2>Choose what one row represents</h2></div></div>
            <div className="matrix-row-types">
              {rowTypes.map((item) => (
                <button className={item.key === rowTypeKey ? 'active' : ''} type="button" key={item.key} onClick={() => changeRowType(item.key)}>
                  <strong>{item.label}</strong><span>{item.description}</span>
                </button>
              ))}
            </div>
            {rowType ? <p className="matrix-sources">Sources: {rowType.sources.join(' · ')}</p> : null}
          </section>

          <section className="matrix-panel">
            <div className="section-head"><div><p className="eyebrow">2 · SCOPE</p><h2>Filter the source snapshot</h2></div></div>
            <div className="matrix-filter-grid">
              <label className="matrix-wide"><span>Players · comma separated</span><input value={filters.players} onChange={(event) => updateFilter('players', event.target.value)} disabled={!rowType?.filters.players} placeholder="hikaru, lafareto" /></label>
              <label><span>From</span><input type="date" value={filters.date_from} onChange={(event) => updateFilter('date_from', event.target.value)} disabled={!rowType?.filters.dates} /></label>
              <label><span>To</span><input type="date" value={filters.date_to} onChange={(event) => updateFilter('date_to', event.target.value)} disabled={!rowType?.filters.dates} /></label>
              <label><span>Minimum full moves</span><input type="number" min="0" value={filters.min_moves} onChange={(event) => updateFilter('min_moves', event.target.value)} disabled={!rowType?.filters.minimum_moves} /></label>
              <label><span>Maximum output rows</span><input type="number" min="1" max={catalog?.limits.maximum_rows || 5000000} value={filters.max_rows} onChange={(event) => updateFilter('max_rows', event.target.value)} /></label>
            </div>
            <div className="matrix-filter-actions">
              {['bullet', 'blitz', 'rapid'].map((mode) => (
                <button className={filters.modes.includes(mode) ? 'active' : ''} type="button" key={mode} onClick={() => toggleMode(mode)} disabled={!rowType?.filters.modes}>{mode}</button>
              ))}
              <label><input type="checkbox" checked={filters.analyzed_only} onChange={(event) => updateFilter('analyzed_only', event.target.checked)} disabled={!rowType?.filters.analyzed_only} /> analyzed only</label>
            </div>
          </section>

          <section className="matrix-panel">
            <div className="section-head"><div><p className="eyebrow">3 · COLUMNS</p><h2>Assign features and labels</h2></div></div>
            <div className="matrix-column-head"><span>Column</span><span>Feature</span><span>Label</span></div>
            <div className="matrix-columns">
              {(rowType?.columns || []).map((column) => (
                <div className="matrix-column" key={column.key} title={column.description || column.source}>
                  <div><strong>{column.label}</strong><span>{column.source} · {column.encoding} · {column.storage_dtype}</span></div>
                  <input type="checkbox" checked={features.includes(column.key)} onChange={() => toggleColumn(column.key, 'feature')} aria-label={`${column.label} as feature`} />
                  <input type="checkbox" checked={labels.includes(column.key)} onChange={() => toggleColumn(column.key, 'label')} disabled={!column.label_allowed} aria-label={`${column.label} as label`} />
                </div>
              ))}
            </div>
          </section>

          <section className="matrix-panel matrix-build-panel">
            <div className="section-head"><div><p className="eyebrow">4 · SNAPSHOT</p><h2>Estimate, then construct</h2></div></div>
            <div className="matrix-build-row">
              <label><span>Artifact name</span><input value={name} maxLength="100" onChange={(event) => { setName(event.target.value); invalidate() }} /></label>
              <button className="btn btn-secondary btn-inline" type="button" onClick={preview} disabled={working || !features.length}>{working === 'estimate' ? 'Estimating' : 'Estimate'}</button>
              <button className="btn btn-primary btn-inline" type="button" onClick={build} disabled={working || !estimate?.storage?.safe_to_build}>{working === 'build' ? 'Queueing' : 'Construct matrix'}</button>
            </div>
            {estimate ? (
              <div className="matrix-estimate">
                <article><span>Matching rows</span><strong>{formatNumber(estimate.matching_rows)}{estimate.matching_rows_is_lower_bound ? '+' : ''}</strong></article>
                <article><span>Snapshot rows</span><strong>{formatNumber(estimate.selected_rows)}</strong></article>
                <article><span>Estimated size</span><strong>{formatBytes(estimate.estimated_artifact_bytes)}</strong></article>
                <article><span>Protected free space</span><strong>{formatBytes(estimate.storage.available_for_matrix_bytes)}</strong></article>
                {estimate.truncated ? <p>The row cap truncates this snapshot.</p> : null}
                {!estimate.storage.safe_to_build ? <p className="matrix-error">This build would cross the 150 GB protected free-space floor.</p> : null}
              </div>
            ) : null}
          </section>

          <section className="matrix-panel">
            <div className="section-head"><div><p className="eyebrow">ARTIFACTS</p><h2>Immutable matrix snapshots</h2></div><button className="btn btn-secondary btn-inline" type="button" onClick={() => loadArtifacts().catch((loadError) => setError(loadError.message))}>refresh</button></div>
            <ArtifactList artifacts={artifacts} onDelete={remove} />
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}
