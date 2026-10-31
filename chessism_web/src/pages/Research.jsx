import { pageStyleTarget } from '../appearance/styleCore.js'
import Header from '../components/layout/Header'
import SideRail from '../components/layout/SideRail'
import './research/researchHub.css'

const RESEARCH_AREAS = [
  {
    id: 'coefficient',
    href: '/research/chessism-coefficient',
    eyebrow: 'ACCURACY MODEL',
    title: 'Accuracy coefficient',
    description: 'Fit and compare Chessism CP-to-outcome coefficients against the current Lichess constant.',
    action: 'Open coefficient research',
  },
  {
    id: 'matrices',
    href: '/research/matrices',
    eyebrow: 'DATASETS',
    title: 'Matrices constructor',
    description: 'Save matrix instructions and inspect a small live preview. Data is materialized only when an algorithm runs.',
    action: 'Open matrix constructor',
  },
  {
    id: 'algorithms',
    href: '/research/algorithms',
    eyebrow: 'CALCULATIONS',
    title: 'Algorithm creation',
    description: 'Browse matrix formulas, choose their data, and build calculations from editable steps with tables and charts.',
    action: 'Open algorithm creation',
  },
  {
    id: 'salience',
    href: '/research/salience-comparison',
    eyebrow: 'SIMILARITY',
    title: 'Salience comparison',
    description: 'Compare three salience formulas on lafareto’s saved games without changing official scores.',
    action: 'Open comparison',
  },
]

export default function Research() {
  return (
    <div {...pageStyleTarget('page')} className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="research-hub-main">
          <section {...pageStyleTarget('intro.panel')} className="research-hub-hero">
            <p {...pageStyleTarget('intro.eyebrow')} className="eyebrow">SUPERUSER RESEARCH</p>
            <h1 {...pageStyleTarget('intro.heading')}>Research</h1>
            <p {...pageStyleTarget('intro.text')}>Build reproducible experiments and datasets without changing production analysis.</p>
          </section>
          <section {...pageStyleTarget('areas.panel')} className="research-hub-grid" aria-label="Research areas">
            {RESEARCH_AREAS.map((area) => (
              <a {...pageStyleTarget(`area.${area.id}.panel`)} className="research-hub-card" href={area.href} key={area.href}>
                <span {...pageStyleTarget(`area.${area.id}.eyebrow`)} className="eyebrow">{area.eyebrow}</span>
                <h2 {...pageStyleTarget(`area.${area.id}.title`)}>{area.title}</h2>
                <p {...pageStyleTarget(`area.${area.id}.text`)}>{area.description}</p>
                <strong {...pageStyleTarget(`area.${area.id}.action`)}>{area.action} →</strong>
              </a>
            ))}
          </section>
        </main>
      </div>
    </div>
  )
}
