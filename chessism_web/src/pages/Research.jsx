import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import './research/researchHub.css'

const RESEARCH_AREAS = [
  {
    href: '/research/chessism-coefficient',
    eyebrow: 'ACCURACY MODEL',
    title: 'Accuracy coefficient',
    description: 'Fit and compare Chessism CP-to-outcome coefficients against the current Lichess constant.',
    action: 'Open coefficient research',
  },
  {
    href: '/research/matrices',
    eyebrow: 'DATASETS',
    title: 'Matrices constructor',
    description: 'Save matrix instructions and inspect a small live preview. Data is materialized only when an algorithm runs.',
    action: 'Open matrix constructor',
  },
  {
    href: '/research/salience',
    eyebrow: 'SIMILARITY',
    title: 'Game salience',
    description: 'Build and inspect corpus-wide position frequencies, move weights, and game salience for every tracked player.',
    action: 'Open salience research',
  },
]

export default function Research() {
  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="research-hub-main">
          <section className="research-hub-hero">
            <p className="eyebrow">SUPERUSER RESEARCH</p>
            <h1>Research</h1>
            <p>Build reproducible experiments and datasets without changing production analysis.</p>
          </section>
          <section className="research-hub-grid" aria-label="Research areas">
            {RESEARCH_AREAS.map((area) => (
              <a className="research-hub-card" href={area.href} key={area.href}>
                <span className="eyebrow">{area.eyebrow}</span>
                <h2>{area.title}</h2>
                <p>{area.description}</p>
                <strong>{area.action} →</strong>
              </a>
            ))}
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}
