import { pageStyleTarget } from '../../appearance/styleCore.js'
import Header from '../../components/layout/Header'
import Footer from '../../components/layout/Footer'
import SideRail from '../../components/layout/SideRail'
import CloudAnalysisSection from './CloudAnalysisSection'
import LocalAnalysisSection from './LocalAnalysisSection'
import { formatNumber, formatTruncatedMillions } from './positionPageSupport'

export default function PositionsView({ page }) {
  const { coverageBarItems, coverageError } = page
  const coverageGroups = [
    { label: 'Game coverage', keys: ['games', 'games-analyzed'] },
    { label: 'Position coverage', keys: ['positions', 'analyzed'] },
  ]
  return (
    <div {...pageStyleTarget('page')} className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header eyebrow="AMALIZE POSITIONS" title="cChessism" />
        <main className="positions-main">
          <section className="coverage-summary-grid" aria-label="Game and position coverage">
            {coverageError ? <div className="status-banner warn">{coverageError}</div> : null}
            {coverageGroups.map(group => (
              <section {...pageStyleTarget('coverage.panel')} className="coverage-bar-chart" aria-label={group.label} key={group.label}>
                {coverageBarItems.filter(item => group.keys.includes(item.key)).map((item) => (
                  <div className={`coverage-bar-row coverage-bar-row-${item.key}`} key={item.key}>
                    <div className="coverage-bar-meta">
                      <span>{item.key === 'games-analyzed' ? 'Scored games' : item.label}</span>
                      <strong {...pageStyleTarget('value')}>
                        {item.ready
                          ? item.displayInMillions
                            ? formatTruncatedMillions(item.value)
                            : formatNumber(item.value)
                          : '-'}
                      </strong>
                    </div>
                    <div {...pageStyleTarget('chart.coverage')} className="coverage-bar-track" aria-hidden="true">
                      <div
                        className={`coverage-bar-fill coverage-bar-fill-${item.key}`}
                        style={{ width: `${item.percent}%` }}
                      />
                    </div>
                  </div>
                ))}
              </section>
            ))}
          </section>
          <LocalAnalysisSection page={page} />
          <CloudAnalysisSection collapsible onProgress={page.refreshCloudProgress} />
        </main>
        <Footer />
      </div>
    </div>
  )
}
