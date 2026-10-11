import { pageStyleTarget } from '../appearance/styleCore.js'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import './secondary-character/secondaryCharacter.css'

function SecondaryCharacter() {
  return (
    <div {...pageStyleTarget('page')} className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main>
          <section {...pageStyleTarget('panel')} className="games-hero secondary-character-panel">
            <h1 {...pageStyleTarget('heading')}>Secondary Character</h1>
            <p {...pageStyleTarget('text')}>Placeholder page.</p>
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default SecondaryCharacter
