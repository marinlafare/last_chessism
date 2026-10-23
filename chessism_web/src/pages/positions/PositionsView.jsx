import { pageStyleTarget } from '../../appearance/styleCore.js'
import Header from '../../components/layout/Header'
import Footer from '../../components/layout/Footer'
import SideRail from '../../components/layout/SideRail'
import CloudAnalysisSection from './CloudAnalysisSection'
import LocalAnalysisSection from './LocalAnalysisSection'
import { AnalysisDisclosureGroup } from './AnalysisDisclosure'
import { formatNumber, formatTruncatedMillions } from './positionPageSupport'

export default function PositionsView({ page }) {
  const { coverageBarItems, coverageError } = page
  const coverageGroups = [
    { id: 'games', label: 'Game coverage', keys: ['games', 'games-analyzed'] },
    { id: 'positions', label: 'Position coverage', keys: ['positions', 'analyzed'] },
  ]
  return (
    <div {...pageStyleTarget('page')} className="page-frame">
      <SideRail />
      <div {...pageStyleTarget('layout.shell')} className="home-shell">
        <Header eyebrow={null} title="ANALYZE POSITIONS" />
        <main {...pageStyleTarget('layout.main')} className="positions-main">
          <section {...pageStyleTarget('layout.coverage')} className="coverage-summary-grid" aria-label="Game and position coverage">
            {coverageError ? <div className="status-banner warn">{coverageError}</div> : null}
            {coverageGroups.map(group => (
              <section {...pageStyleTarget(`coverage.${group.id}.panel`)} className="coverage-bar-chart" aria-label={group.label} key={group.label}>
                {coverageBarItems.filter(item => group.keys.includes(item.key)).map((item) => (
                  <div className={`coverage-bar-row coverage-bar-row-${item.key}`} key={item.key}>
                    <div className="coverage-bar-meta">
                      <span {...pageStyleTarget(`coverage.label.${item.key}`)}>
                        {item.key === 'games-analyzed' ? 'Scored games' : item.label}
                      </span>
                      <strong {...pageStyleTarget(`coverage.${item.key}.value`)}>
                        {item.ready
                          ? item.displayInMillions
                            ? formatTruncatedMillions(item.value)
                            : formatNumber(item.value)
                          : '-'}
                      </strong>
                    </div>
                    <div {...pageStyleTarget(`coverage.${item.key}.track`)} className="coverage-bar-track" aria-hidden="true">
                      <div
                        {...pageStyleTarget(`coverage.${item.key}.fill`)}
                        className={`coverage-bar-fill coverage-bar-fill-${item.key}`}
                        style={{ width: `${item.percent}%` }}
                      />
                    </div>
                  </div>
                ))}
              </section>
            ))}
          </section>
          <AnalysisDisclosureGroup>
            <LocalAnalysisSection page={page} />
            <CloudAnalysisSection collapsible onProgress={page.refreshCloudProgress} />
          </AnalysisDisclosureGroup>
        </main>
        <Footer />
      </div>
    </div>
  )
}
