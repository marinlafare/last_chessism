import Header from '../../components/layout/Header'
import Footer from '../../components/layout/Footer'
import SideRail from '../../components/layout/SideRail'
import CloudAnalysisSection from './CloudAnalysisSection'
import LocalAnalysisSection from './LocalAnalysisSection'
import { formatNumber, formatTruncatedMillions } from './positionPageSupport'

export default function PositionsView({ page }) {
  const { coverageBarItems, coverageError } = page
  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="positions-main">
          <section className="coverage-bar-chart" aria-label="Position coverage">
            {coverageError ? <div className="status-banner warn">{coverageError}</div> : null}
            {coverageBarItems.map((item) => (
              <div className={`coverage-bar-row coverage-bar-row-${item.key}`} key={item.key}>
                <div className="coverage-bar-meta">
                  <span>{item.label}</span>
                  <strong>
                    {item.ready
                      ? item.displayInMillions
                        ? formatTruncatedMillions(item.value)
                        : formatNumber(item.value)
                      : '-'}
                  </strong>
                </div>
                <div className="coverage-bar-track" aria-hidden="true">
                  <div
                    className={`coverage-bar-fill coverage-bar-fill-${item.key}`}
                    style={{ width: `${item.percent}%` }}
                  />
                </div>
              </div>
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
