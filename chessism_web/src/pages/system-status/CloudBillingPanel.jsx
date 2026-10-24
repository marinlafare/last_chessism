import { pageStyleTarget } from '../../appearance/styleCore.js'
import { useEffect, useState } from 'react'
import { fetchCloudBilling } from './systemStatusApi'
import './cloudBilling.css'

const monthName = id => new Date(`${id}-01T00:00:00Z`).toLocaleDateString(undefined, { month: 'long', year: 'numeric', timeZone: 'UTC' })
const timestamp = value => value ? new Date(value).toLocaleString() : 'Not available'
const money = (value, currency) => new Intl.NumberFormat(undefined, {
  style: 'currency', currency, minimumFractionDigits: 2, maximumFractionDigits: 4,
}).format(Number(value))

function BillingData({ data }) {
  const [selected, setSelected] = useState('')
  const [currency, setCurrency] = useState('')
  const period = data.periods.find(item => item.id === selected) || data.periods[0]
  const costs = period.currencies.find(item => item.currency === currency) || period.currencies[0]
  return <>
    <div className="cloud-bill-toolbar">
      <div><h3 {...pageStyleTarget('title')}>Project usage costs</h3><p {...pageStyleTarget('text')}>{data.project}</p></div>
      <label {...pageStyleTarget('label')}>Usage month <select {...pageStyleTarget('field')} value={period.id} onChange={event => setSelected(event.target.value)}>
        {data.periods.map(item => <option key={item.id} value={item.id}>{monthName(item.id)}</option>)}
      </select></label>
      {period.currencies.length > 1 && <label {...pageStyleTarget('label')}>Currency <select {...pageStyleTarget('field')} value={costs.currency} onChange={event => setCurrency(event.target.value)}>
        {period.currencies.map(item => <option key={item.currency}>{item.currency}</option>)}
      </select></label>}
    </div>
    {data.stale && <p {...pageStyleTarget('text')} className="cloud-bill-warning" role="status">Showing an older snapshot. {data.warning}</p>}
    {!period.has_data ? <p {...pageStyleTarget('text')} className="cloud-bill-empty">No billing rows have been exported for this month yet. This does not confirm zero spending.</p> : <>
      <div className="cloud-bill-totals">
        {[['Cost before credits', costs.cost], ['Credits & discounts', costs.credits], ['Net cost', costs.net]].map(([label, value]) =>
          <article {...pageStyleTarget('card')} key={label}><h4 {...pageStyleTarget('title')}>{label}</h4><strong {...pageStyleTarget('value')}>{money(value, costs.currency)}</strong><span>{costs.currency}</span></article>)}
      </div>
      <div className="cloud-bill-tables">
        <div className="cloud-bill-table-wrap"><table {...pageStyleTarget('table')}>
          <caption>By Google Cloud service</caption>
          <thead><tr><th {...pageStyleTarget('table.header')} scope="col">Service</th><th {...pageStyleTarget('table.header')} scope="col">Cost</th><th {...pageStyleTarget('table.header')} scope="col">Credits</th><th {...pageStyleTarget('table.header')} scope="col">Net</th></tr></thead>
          <tbody>{costs.services.map(row => <tr key={row.service}><th {...pageStyleTarget('table.header')} scope="row">{row.service}</th>
            <td {...pageStyleTarget('table.cell')}>{money(row.cost, costs.currency)}</td><td {...pageStyleTarget('table.cell')}>{money(row.credits, costs.currency)}</td><td {...pageStyleTarget('table.cell')}>{money(row.net, costs.currency)}</td></tr>)}</tbody>
          <tfoot><tr><th {...pageStyleTarget('table.header')} scope="row">Total</th><td {...pageStyleTarget('table.cell')}>{money(costs.cost, costs.currency)}</td><td {...pageStyleTarget('table.cell')}>{money(costs.credits, costs.currency)}</td><td {...pageStyleTarget('table.cell')}>{money(costs.net, costs.currency)}</td></tr></tfoot>
        </table></div>
        <div className="cloud-bill-table-wrap"><table {...pageStyleTarget('table')}>
          <caption>Recent usage days · UTC</caption>
          <thead><tr><th {...pageStyleTarget('table.header')} scope="col">Day</th><th {...pageStyleTarget('table.header')} scope="col">Net cost</th></tr></thead>
          <tbody>{costs.days.slice(0, 10).map(row => <tr key={row.day}><th {...pageStyleTarget('table.header')} scope="row">{row.day}</th><td {...pageStyleTarget('table.cell')}>{money(row.net, costs.currency)}</td></tr>)}</tbody>
        </table></div>
      </div>
    </>}
    <dl className="cloud-bill-freshness">
      <div><dt>Snapshot read</dt><dd>{timestamp(data.fetched_at)}</dd></div>
      <div><dt>Latest included export</dt><dd>{timestamp(period.exported_at)}</dd></div>
      <div><dt>Latest usage end</dt><dd>{timestamp(period.usage_through)}</dd></div>
      <div><dt>Next refresh available</dt><dd>{timestamp(data.next_refresh_at)}</dd></div>
    </dl>
    <p {...pageStyleTarget('text')} className="cloud-bill-note">Google billing exports are delayed; recent work may be missing. These are project costs by usage date (UTC),
      including exported credits—not a final invoice or a live running-job quote. Only this project is included.</p>
    <p {...pageStyleTarget('text')} className="cloud-bill-note">Reads are cached for one hour, with a 100 MiB query scan limit. No background billing polling.
      {' '}<a {...pageStyleTarget('link')} href="https://console.cloud.google.com/billing?project=chessism-production" target="_blank" rel="noreferrer">Open Google Cloud Billing ↗</a></p>
  </>
}

export default function CloudBillingPanel() {
  const [open, setOpen] = useState(false)
  const [revision, setRevision] = useState(0)
  const [state, setState] = useState({ data: null, loading: false, error: null })
  useEffect(() => {
    if (!open) return
    const abort = new AbortController()
    setState(previous => ({ ...previous, loading: true, error: null }))
    fetchCloudBilling({ signal: abort.signal }).then(data => {
      if (!abort.signal.aborted) setState({ data, loading: false, error: null })
    }).catch(error => {
      if (!abort.signal.aborted) setState(previous => ({ ...previous, loading: false, error: error.message }))
    })
    return () => abort.abort()
  }, [open, revision])
  return <section {...pageStyleTarget('billing.panel')} className={`cloud-bill${open ? ' is-open' : ''}`} aria-label="Google Cloud billing">
    <h2 {...pageStyleTarget('title')}><button {...pageStyleTarget('button')} type="button" id="cloud-bill-toggle" className="cloud-bill-toggle" aria-expanded={open}
      aria-controls="cloud-bill-content" onClick={() => setOpen(value => !value)}>
      <span>gcloud bill</span><svg viewBox="0 0 20 20" aria-hidden="true"><path d="m5 7.5 5 5 5-5" /></svg>
    </button></h2>
    <div id="cloud-bill-content" className="cloud-bill-content" role="region" aria-labelledby="cloud-bill-toggle" hidden={!open}>
      <div className="cloud-bill-actions"><p {...pageStyleTarget('text')} aria-live="polite">{state.loading ? 'Reading billing data…' : 'Google Cloud billing export'}</p>
        <button {...pageStyleTarget('button')} type="button" disabled={state.loading} onClick={() => setRevision(value => value + 1)}>Check for updates</button></div>
      {state.error && <p {...pageStyleTarget('text')} className="cloud-bill-warning" role="alert">{state.data ? 'Could not update; showing the previous snapshot. ' : ''}{state.error}</p>}
      {state.data && <BillingData data={state.data} />}
    </div>
  </section>
}
