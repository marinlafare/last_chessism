import { pageStyleTarget } from '../appearance/styleCore.js'
import { useEffect, useRef } from 'react'
import Header from '../components/layout/Header'
import SideRail from '../components/layout/SideRail'
import MatrixDataFrameModal from './research/matrices/MatrixDataFrameModal'
import AlgorithmForm from './research/algorithms/AlgorithmForm'
import AlgorithmHistory, { ActiveAlgorithmRuns, SavedAlgorithms } from './research/algorithms/AlgorithmHistory'
import CudaValidation from './research/algorithms/CudaValidation'
import BlunderTraining from './research/algorithms/BlunderTraining'
import BlunderValidation from './research/algorithms/BlunderValidation'
import BlunderV2 from './research/algorithms/BlunderV2'
import CanonicalBlunders from './research/algorithms/CanonicalBlunders'
import useAlgorithms from './research/algorithms/useAlgorithms'
import MatrixFormulaBrowser from './research/algorithms/MatrixFormulaBrowser'
import MatrixParametersForm from './research/matrices/MatrixParametersForm'
import './research/matrices/matricesConstructor.css'
import './research/algorithms/algorithms.css'

export default function Algorithms() {
  const model = useAlgorithms()
  const inputRef = useRef(null)
  useEffect(() => {
    if (model.sourceMatrix?.config.definition_kind === 'template') {
      inputRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }
  }, [model.sourceMatrix?.id, model.inputRevision])
  return <div {...pageStyleTarget('page')} className="page-frame"><SideRail /><div className="home-shell"><Header />
    <main className="algorithm-main">
      <section {...pageStyleTarget('panel')} className="algorithm-panel algorithm-heading"><div><p {...pageStyleTarget('text')} className="eyebrow">SUPERUSER RESEARCH</p><h1 {...pageStyleTarget('heading')}>Algorithm creation</h1><p {...pageStyleTarget('text')}>Use saved matrix instructions to investigate your chess data. Definitions and compact results are included in your manual database backup.</p></div><a {...pageStyleTarget('link')} href="/research">Research index</a></section>
      <CanonicalBlunders onBenchmark={model.benchmarkCanonical} disabled={Boolean(model.working) || model.activeRuns.length > 0} />
      <ActiveAlgorithmRuns model={model} />
      {model.error && <div className="status-banner warn" role="alert">{model.error}</div>}
      {model.notice && <div className="status-banner" role="status">{model.notice}</div>}
      <MatrixFormulaBrowser model={model} />
      <BlunderV2 onRun={model.trainBlundersV2} disabled={Boolean(model.working)} />
      <BlunderValidation onRun={model.validateBlunders} disabled={Boolean(model.working)} />
      <BlunderTraining onRun={model.trainBlunders} disabled={Boolean(model.working)} />
      <CudaValidation onRun={model.runCuda} disabled={Boolean(model.working)} />
      {model.sourceMatrix?.config.definition_kind === 'template' ? <div ref={inputRef}>
        <MatrixParametersForm key={`${model.sourceMatrix.id}-${model.inputRevision}`} definition={model.sourceMatrix} catalog={model.catalog}
          onApply={model.applyInput} onDirty={model.dirtyInput} disabled={Boolean(model.working)} />
      </div> : null}
      <AlgorithmForm model={model} />
      <SavedAlgorithms model={model} />
      <AlgorithmHistory model={model} />
    </main></div>
    {model.preview && <MatrixDataFrameModal artifact={model.preview} onClose={() => model.setPreview(null)} />}
  </div>
}
