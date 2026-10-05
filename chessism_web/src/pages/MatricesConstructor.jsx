import { useEffect, useMemo, useRef, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import MatrixDataFrameModal from './research/MatrixDataFrameModal'
import MatrixDefinitionList from './research/MatrixDefinitionList'
import LegacyMatrixSnapshots from './research/LegacyMatrixSnapshots'
import { formatNumber } from '../utils/formatters'
import {
  saveMatrixDefinition,
  deleteMatrixDefinition,
  estimateMatrix,
  fetchMatrixDefinitions,
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


export default function MatricesConstructor() {
  const [catalog, setCatalog] = useState(null)
  const [rowTypeKey, setRowTypeKey] = useState('game_player')
  const [features, setFeatures] = useState([])
  const [labels, setLabels] = useState([])
  const [filters, setFilters] = useState(EMPTY_FILTERS)
  const [name, setName] = useState('Player-game research matrix')
  const [estimate, setEstimate] = useState(null)
  const [definitions, setDefinitions] = useState([])
  const [hasMore, setHasMore] = useState(false)
  const [legacyCount, setLegacyCount] = useState(0)
  const [notice, setNotice] = useState('')
  const [viewedArtifact, setViewedArtifact] = useState(null)
  const [working, setWorking] = useState('')
  const [error, setError] = useState('')
  const estimateRevision = useRef(0)

  const rowTypes = catalog?.row_types || []
  const rowType = useMemo(
    () => rowTypes.find((item) => item.key === rowTypeKey),
    [rowTypes, rowTypeKey]
  )

  const loadDefinitions = async (append = false) => {
    const payload = await fetchMatrixDefinitions({ offset: append ? definitions.length : 0 })
    setDefinitions((current) => append ? [...current, ...payload.definitions] : payload.definitions || [])
    setLegacyCount(payload.legacy_snapshot_count || 0)
    setHasMore(Boolean(payload.has_more))
  }

  useEffect(() => {
    let active = true
    Promise.all([fetchMatrixCatalog(), fetchMatrixDefinitions()])
      .then(([catalogPayload, definitionsPayload]) => {
        if (!active) return
        setCatalog(catalogPayload)
        setDefinitions(definitionsPayload.definitions || [])
        setLegacyCount(definitionsPayload.legacy_snapshot_count || 0)
        setHasMore(Boolean(definitionsPayload.has_more))
        const initial = catalogPayload.row_types?.find((item) => item.key === 'game_player')
        setFeatures((initial?.columns || []).filter((item) => item.default).map((item) => item.key))
      })
      .catch((loadError) => { if (active) setError(loadError.message || 'Unable to load the matrix constructor.') })
    return () => { active = false }
  }, [])

  const invalidate = () => { estimateRevision.current += 1; setEstimate(null); setNotice('') }
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

  const estimateCurrentScope = async () => {
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

  const save = async () => {
    setWorking('save')
    setError('')
    try {
      const definition = await saveMatrixDefinition(payload())
      setNotice(`Saved “${definition.name}”: instructions only. No matrix files or background job were created.`)
      await loadDefinitions()
    } catch (saveError) {
      setError(saveError.message || 'Unable to save this definition.')
    } finally {
      setWorking('')
    }
  }

  const remove = async (definition) => {
    if (!window.confirm(`Delete the instructions for “${definition.name}”? Existing snapshots and backups will be kept.`)) return
    try {
      await deleteMatrixDefinition(definition.id)
      await loadDefinitions()
    } catch (deleteError) {
      setError(deleteError.message || 'Unable to delete this definition.')
    }
  }

  const useDefinition = (definition) => {
    const config = definition.config
    setName(`${definition.name} copy`)
    setRowTypeKey(config.row_type)
    setFeatures(config.feature_columns)
    setLabels(config.label_columns)
    setFilters({ ...EMPTY_FILTERS, ...config.filters, players: config.filters.players.join(', '), date_from: config.filters.date_from || '', date_to: config.filters.date_to || '' })
    invalidate()
    setNotice('Definition loaded into the form. Save creates a separate definition; the original is unchanged.')
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }
  const showDraftPreview = () => setViewedArtifact({ id: null, storage_kind: 'definition', name,
    config: payload(), feature_count: features.length, label_count: labels.length })

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
              <p>Save reusable instructions for future algorithms. Preview a small live sample; no matrix files are created. Definitions are protected by the database backup.</p>
            </div>
            <a className="matrix-back" href="/research">Research index</a>
          </section>
          {error ? <div className="status-banner warn">{error}</div> : null}
          {notice ? <div className="status-banner" role="status">{notice}</div> : null}

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
            <div className="section-head"><div><p className="eyebrow">2 · SCOPE</p><h2>Define the source filters</h2></div></div>
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
            <div className="section-head"><div><p className="eyebrow">4 · DEFINITION</p><h2>Preview and save instructions</h2></div></div>
            <div className="matrix-build-row matrix-definition-build-row">
              <label><span>Definition name</span><input value={name} maxLength="100" onChange={(event) => { setName(event.target.value); invalidate() }} /></label>
              <button className="btn btn-secondary btn-inline" type="button" onClick={estimateCurrentScope} disabled={working || !features.length}>{working === 'estimate' ? 'Estimating' : 'Estimate'}</button>
              <button className="btn btn-secondary btn-inline" type="button" onClick={showDraftPreview} disabled={working || !features.length}>Live preview</button>
              <button className="btn btn-primary btn-inline" type="button" onClick={save} disabled={working || !features.length}>{working === 'save' ? 'Saving' : 'Save definition'}</button>
            </div>
            {estimate ? (
              <div className="matrix-estimate">
                <article><span>Matching rows</span><strong>{formatNumber(estimate.matching_rows)}{estimate.matching_rows_is_lower_bound ? '+' : ''}</strong></article>
                <article><span>Rows if run now</span><strong>{formatNumber(estimate.selected_rows)}{estimate.selected_rows_is_lower_bound ? '+' : ''}</strong></article>
                <article><span>Configured row limit</span><strong>{formatNumber(estimate.row_limit)}</strong></article>
                <article><span>Instructions only</span><strong>{formatNumber(estimate.definition_bytes)} B</strong></article>
                <p>Live estimate, not a frozen dataset. “+” means a lower bound; counting is capped to avoid scanning the entire database. Saving does not require an estimate or matching rows.</p>
              </div>
            ) : null}
          </section>

          <section className="matrix-panel">
            <div className="section-head"><div><p className="eyebrow">DEFINITIONS</p><h2>Saved matrix instructions</h2></div><button className="btn btn-secondary btn-inline" type="button" onClick={() => loadDefinitions().catch((failure) => setError(failure.message))}>refresh</button></div>
            <MatrixDefinitionList definitions={definitions} onDelete={remove} onPreview={setViewedArtifact} onUse={useDefinition} />
            {hasMore ? <button className="matrix-inspect-button" type="button" onClick={() => loadDefinitions(true).catch((failure) => setError(failure.message))}>load more definitions</button> : null}
          </section>
          <LegacyMatrixSnapshots count={legacyCount} onPreview={setViewedArtifact} onChanged={() => loadDefinitions().catch((failure) => setError(failure.message))} />
        </main>
        <Footer />
      </div>
      {viewedArtifact ? <MatrixDataFrameModal key={`${viewedArtifact.storage_kind}-${viewedArtifact.id || 'draft'}`} artifact={viewedArtifact} onClose={() => setViewedArtifact(null)} /> : null}
    </div>
  )
}
