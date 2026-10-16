export default function AppearanceChartDetails({ children }) {
  return <details className="appearance-details">
    <summary aria-label="Chart details" title="Chart details">ⓘ</summary>
    <div className="appearance-details-popover">{children}</div>
  </details>
}
