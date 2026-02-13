import { useCallback } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import Hero from '../components/home/Hero'
import ProofPoints from '../components/home/ProofPoints'
import StatusPanel from '../components/home/StatusPanel'
import ExploreGrid from '../components/home/ExploreGrid'
import { usePoll } from '../hooks/usePoll'
import { fetchStatus } from '../services/statusService'

function Home() {
  const pollStatus = useCallback(({ signal }) => fetchStatus({ signal }), [])
  const { data, loading, error, lastSuccessAt } = usePoll(pollStatus, 8000)

  return (
    <div className="home-shell">
      <Header />
      <main>
        <Hero />
        <ProofPoints />
        <StatusPanel loading={loading} error={error} data={data} lastSuccessAt={lastSuccessAt} />
        <ExploreGrid />
      </main>
      <Footer />
    </div>
  )
}

export default Home
