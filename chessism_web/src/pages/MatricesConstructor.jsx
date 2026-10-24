import { pageStyleTarget } from '../appearance/styleCore.js'
import Header from '../components/layout/Header'
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
          {error ? <div className="status-banner warn" role="alert">{error}</div> : null}
          {notice ? <div className="status-banner" role="status">{notice} {savedDefinition ? <a {...pageStyleTarget('link')} className="matrix-back" href={`/research/algorithms?preview=${encodeURIComponent(savedDefinition.id)}`}>Review saved formula →</a> : null}</div> : null}
          <MatrixDefinitionForm model={model} />
        </main>
      </div>
    </div>
  )
}
