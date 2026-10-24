import { pageStyleTarget } from '../appearance/styleCore.js'
import { useEffect, useRef } from 'react'
import Header from '../components/layout/Header'
import SideRail from '../components/layout/SideRail'
import MatrixDataFrameModal from './research/matrices/MatrixDataFrameModal'
import AlgorithmForm from './research/algorithms/AlgorithmForm'
import AlgorithmHistory from './research/algorithms/AlgorithmHistory'
import AlgorithmResults from './research/algorithms/AlgorithmResults'
import useAlgorithms from './research/algorithms/useAlgorithms'
import MatrixFormulaBrowser from './research/algorithms/MatrixFormulaBrowser'
import MatrixParametersForm from './research/matrices/MatrixParametersForm'
import './research/matrices/matricesConstructor.css'
import './research/algorithms/algorithms.css'

export default function Algorithms() {
  const model = useAlgorithms()
  const resultsRef = useRef(null)
  const inputRef = useRef(null)
  useEffect(() => {
    if (model.sourceMatrix?.config.definition_kind === 'template') {
      inputRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }
  }, [model.sourceMatrix?.id, model.inputRevision])
  useEffect(() => {
    if (model.result && resultsRef.current) {
      resultsRef.current.scrollIntoView({ behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'start' })
      resultsRef.current.focus({ preventScroll: true })
    }
  }, [model.result?.id])
  return <div {...pageStyleTarget('page')} className="page-frame"><SideRail /><div className="home-shell"><Header />
    <main className="algorithm-main">
      <section {...pageStyleTarget('panel')} className="algorithm-panel algorithm-heading"><div><p {...pageStyleTarget('text')} className="eyebrow">SUPERUSER RESEARCH</p><h1 {...pageStyleTarget('heading')}>Algorithm creation</h1><p {...pageStyleTarget('text')}>Use saved matrix instructions to investigate your chess data. Definitions and compact results are included in your manual database backup.</p></div><a {...pageStyleTarget('link')} href="/research">Research index</a></section>
      {model.error && <div className="status-banner warn" role="alert">{model.error}</div>}
      {model.notice && <div className="status-banner" role="status">{model.notice}</div>}
      <MatrixFormulaBrowser model={model} />
      {model.sourceMatrix?.config.definition_kind === 'template' ? <div ref={inputRef}>
        <MatrixParametersForm key={`${model.sourceMatrix.id}-${model.inputRevision}`} definition={model.sourceMatrix} catalog={model.catalog}
          onApply={model.applyInput} onDirty={model.dirtyInput} disabled={Boolean(model.working)} />
      </div> : null}
      <AlgorithmForm model={model} />
      <AlgorithmHistory model={model} />
      {model.result && <div ref={resultsRef} tabIndex="-1"><AlgorithmResults run={model.result} onClose={() => model.setResult(null)} /></div>}
    </main></div>
    {model.preview && <MatrixDataFrameModal artifact={model.preview} onClose={() => model.setPreview(null)} />}
  </div>
}
