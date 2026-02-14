import { useCallback } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import Hero from '../components/home/Hero'

function Home() {
  return (
    <div className="page-frame">
      <div className="left-rail">
        <div className="left-rail-menu">
          <img className="left-rail-icon" src="/chessism.jpg" alt="Chessism icon" />
          <a className="rail-btn" href="/games">
            Games
          </a>
          <a className="rail-btn" href="/players">
            Players
          </a>
        </div>
      </div>
      <div className="home-shell">
        <Header />
        <main>
          <Hero />
        </main>
        <Footer />
      </div>
    </div>
  )
}

export default Home
