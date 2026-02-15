import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'

function Positions() {
  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main>
          <section className="games-hero">
            <h1>Positions</h1>
            <p>Placeholder page.</p>
          </section>
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default Positions
