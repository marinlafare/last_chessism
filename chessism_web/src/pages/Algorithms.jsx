import { useEffect, useRef } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import MatrixDataFrameModal from './research/matrices/MatrixDataFrameModal'
import AlgorithmForm from './research/algorithms/AlgorithmForm'
import AlgorithmHistory from './research/algorithms/AlgorithmHistory'
import AlgorithmResults from './research/algorithms/AlgorithmResults'
import useAlgorithms from './research/algorithms/useAlgorithms'
import './research/algorithms/algorithms.css'

export default function Algorithms() {
  const model = useAlgorithms()
  const resultsRef = useRef(null)
  useEffect(() => {
    if (model.result && resultsRef.current) {
      resultsRef.current.scrollIntoView({ behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth', block: 'start' })
      resultsRef.current.focus({ preventScroll: true })
    }
  }, [model.result?.id])
  return <div className="page-frame"><SideRail /><div className="home-shell"><Header />
    <main className="algorithm-main">
      <section className="algorithm-panel algorithm-heading"><div><p className="eyebrow">SUPERUSER RESEARCH</p><h1>Algorithm creation</h1><p>Use saved matrix instructions to investigate your chess data. Definitions and compact results are included in your manual database backup.</p></div><a href="/research">Research index</a></section>
      {model.error && <div className="status-banner warn" role="alert">{model.error}</div>}
      {model.notice && <div className="status-banner" role="status">{model.notice}</div>}
      <AlgorithmForm model={model} />
      <AlgorithmHistory model={model} />
      {model.result && <div ref={resultsRef} tabIndex="-1"><AlgorithmResults run={model.result} onClose={() => model.setResult(null)} /></div>}
    </main><Footer /></div>
    {model.preview && <MatrixDataFrameModal artifact={model.preview} onClose={() => model.setPreview(null)} />}
  </div>
}
