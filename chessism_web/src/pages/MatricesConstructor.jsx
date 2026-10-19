import { pageStyleTarget } from '../appearance/styleCore.js'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import MatrixDefinitionForm from './research/matrices/MatrixDefinitionForm'
import useMatrixConstructor from './research/matrices/useMatrixConstructor'
import './research/matrices/matricesConstructor.css'

export default function MatricesConstructor() {
  const model = useMatrixConstructor()
  const { error, notice, savedDefinition } = model
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
              <p {...pageStyleTarget('text')}>Build your own column formulas here. Browse saved and default formulas in Algorithm creation → Input formulas. Saving does not select games or create matrix files.</p>
            </div>
            <nav className="matrix-hero-actions" aria-label="Matrix pages">
              <a {...pageStyleTarget('link')} className="matrix-back" href="/research/algorithms">Algorithm creation</a>
              <a {...pageStyleTarget('link')} className="matrix-back" href="/research">Research index</a>
            </nav>
          </section>
          {error ? <div className="status-banner warn" role="alert">{error}</div> : null}
          {notice ? <div className="status-banner" role="status">{notice} {savedDefinition ? <a {...pageStyleTarget('link')} className="matrix-back" href={`/research/algorithms?preview=${encodeURIComponent(savedDefinition.id)}`}>Review saved formula →</a> : null}</div> : null}
          <MatrixDefinitionForm model={model} />
        </main>
        <Footer />
      </div>
    </div>
  )
}
