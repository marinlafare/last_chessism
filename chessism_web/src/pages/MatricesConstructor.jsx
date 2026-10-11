import { pageStyleTarget } from '../appearance/styleCore.js'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import MatrixDataFrameModal from './research/matrices/MatrixDataFrameModal'
import MatrixDefinitionForm from './research/matrices/MatrixDefinitionForm'
import MatrixDefinitionList from './research/matrices/MatrixDefinitionList'
import LegacyMatrixSnapshots from './research/matrices/LegacyMatrixSnapshots'
import useMatrixConstructor from './research/matrices/useMatrixConstructor'
import './research/matrices/matricesConstructor.css'

export default function MatricesConstructor() {
  const model = useMatrixConstructor()
  const { error, notice, working, saved, viewedArtifact, setViewedArtifact, remove, useDefinition } = model
  return (
    <div {...pageStyleTarget('page')} className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="matrix-main">
          <section {...pageStyleTarget('panel')} className="matrix-hero">
            <div>
              <p {...pageStyleTarget('text')} className="eyebrow">SUPERUSER RESEARCH</p>
              <h1 {...pageStyleTarget('heading')}>Matrices constructor</h1>
              <p {...pageStyleTarget('text')}>Save reusable instructions for future algorithms. Preview a small live sample; no matrix files are created. Definitions are included in your next manual database backup.</p>
            </div>
            <a {...pageStyleTarget('link')} className="matrix-back" href="/research">Research index</a>
          </section>
          {error ? <div className="status-banner warn" role="alert">{error}</div> : null}
          {notice ? <div className="status-banner" role="status">{notice}</div> : null}
          <MatrixDefinitionForm model={model} />
          <section {...pageStyleTarget('definitions.panel')} className="matrix-panel" aria-busy={saved.loading}>
            <div className="section-head"><div><p {...pageStyleTarget('text')} className="eyebrow">DEFINITIONS</p><h2 {...pageStyleTarget('title')}>Saved matrix instructions</h2></div>
              <button {...pageStyleTarget('button')} className="btn btn-secondary btn-inline" type="button" disabled={Boolean(working) || saved.loading} onClick={() => saved.load()}>refresh</button>
            </div>
            {saved.loading ? <p {...pageStyleTarget('text')} className="matrix-empty" role="status">Loading definitions…</p> : null}
            {saved.definitions.length || !saved.loading ? <MatrixDefinitionList definitions={saved.definitions} disabled={Boolean(working)} onDelete={remove} onPreview={setViewedArtifact} onUse={useDefinition} /> : null}
            {saved.hasMore ? <button {...pageStyleTarget('button')} className="matrix-inspect-button" type="button" disabled={Boolean(working) || saved.loading} onClick={() => saved.load(true)}>load more definitions</button> : null}
          </section>
          <LegacyMatrixSnapshots count={saved.legacyCount} onPreview={setViewedArtifact} onChanged={() => saved.load()} />
        </main>
        <Footer />
      </div>
      {viewedArtifact ? <MatrixDataFrameModal key={`${viewedArtifact.storage_kind}-${viewedArtifact.id || 'draft'}`} artifact={viewedArtifact} onClose={() => setViewedArtifact(null)} /> : null}
    </div>
  )
}
