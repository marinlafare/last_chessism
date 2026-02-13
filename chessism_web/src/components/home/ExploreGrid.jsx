import { API_BASE_URL, SHOW_ADMIN_LINK } from '../../config'

const baseExploreItems = [
  {
    title: 'Jobs',
    description: 'Create and track analysis runs.',
    href: '/api/docs#/Analysis/run_analysis_job_analysis_run_job_post',
    icon: 'J'
  },
  {
    title: 'Games',
    description: 'Review ingested games and metadata.',
    href: '/api/docs#/Games',
    icon: 'G'
  },
  {
    title: 'Players',
    description: 'Inspect player trends and performance.',
    href: '/api/docs#/Players',
    icon: 'P'
  },
  {
    title: 'Positions (FEN)',
    description: 'Browse critical and unscored positions.',
    href: '/api/docs#/FENs',
    icon: 'F'
  },
  {
    title: 'API Docs',
    description: 'Open Swagger and endpoint definitions.',
    href: '/api/docs',
    icon: 'A'
  }
]

const endpointLinks = [
  { method: 'POST', path: '/analysis/run_job' },
  { method: 'POST', path: '/analysis/run_player_job' },
  { method: 'GET', path: '/players/current_players' },
  { method: 'GET', path: '/fens/top' },
  { method: 'GET', path: '/fens/top_unscored' },
  { method: 'GET', path: '/games/{link}' }
]

function ExploreGrid() {
  const exploreItems = SHOW_ADMIN_LINK
    ? [
        ...baseExploreItems,
        { title: 'System / Admin', description: 'Operational controls and internals.', href: '/api/docs', icon: 'S' }
      ]
    : baseExploreItems

  return (
    <section className="explore" id="explore">
      <div className="section-head">
        <h2>Explore Chessism</h2>
        <p>Jump to the key surfaces for jobs, players, games, positions, and docs.</p>
      </div>

      <div className="explore-grid">
        {exploreItems.map((item) => (
          <a key={item.title} href={item.href} className="explore-card">
            <span className="explore-icon" aria-hidden="true">{item.icon}</span>
            <h3>{item.title}</h3>
            <p>{item.description}</p>
          </a>
        ))}
      </div>

      <div className="endpoint-hub" aria-label="Key endpoints">
        <h3>Key Endpoints</h3>
        <ul>
          {endpointLinks.map((endpoint) => (
            <li key={`${endpoint.method}-${endpoint.path}`}>
              <span>{endpoint.method}</span>
              <code>{`${API_BASE_URL}${endpoint.path}`}</code>
            </li>
          ))}
        </ul>
      </div>
    </section>
  )
}

export default ExploreGrid
