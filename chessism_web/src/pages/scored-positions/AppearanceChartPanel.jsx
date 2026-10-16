import { useEffect, useMemo, useRef, useState } from 'react'
import { fetchFenAppearances } from './scoredPositionsApi'
import AppearanceCircles from './AppearanceCircles'
import CommonnessChart from './CommonnessChart'
import PendingRepetitionChart from './PendingRepetitionChart'
import { allAppearanceHistogram } from './pendingRepetitions'
import { styleTarget } from '../../appearance/styleCore'

const TABS = [{ id: 'uniqueness', label: 'Uniqueness' }, { id: 'commonness', label: 'Communess' },
  { id: 'unscored', label: 'Unscored FENs' }]

export default function AppearanceChartPanel({ reloadToken }) {
  const [active, setActive] = useState('uniqueness')
  const [data, setData] = useState(null)
  const [error, setError] = useState('')
  const buttons = useRef([])
  useEffect(() => {
    const controller = new AbortController()
    fetchFenAppearances({ signal: controller.signal }).then(payload => {
      if (controller.signal.aborted) return
      setData(payload); setError('')
    }).catch(err => { if (!controller.signal.aborted) setError(err.message || 'Appearance summary unavailable.') })
    return () => controller.abort()
  }, [reloadToken])
  const pending = useMemo(() => data && ({ ...data, rows: (data.rows || []).map(row => ({
    repetitions: row.appearances, fen_count: row.unscored_fens,
  })) }), [data])
  const view = useMemo(() => allAppearanceHistogram(data?.rows), [data])
  const onTabKey = (event, index) => {
    let next
    if (event.key === 'ArrowRight') next = (index + 1) % TABS.length
    else if (event.key === 'ArrowLeft') next = (index + TABS.length - 1) % TABS.length
    else if (event.key === 'Home') next = 0
    else if (event.key === 'End') next = TABS.length - 1
    else return
    event.preventDefault(); setActive(TABS[next].id); buttons.current[next]?.focus()
  }
  return <section {...styleTarget('appearances.panel')} className="scored-positions-panel appearance-panel" aria-label="FEN appearances">
    <div className="appearance-tabs" role="tablist" aria-label="FEN appearance scope">
      {TABS.map((tab, index) => <button {...styleTarget('appearance.tab')} key={tab.id} ref={node => { buttons.current[index] = node }}
        type="button" role="tab" id={`appearance-tab-${tab.id}`} aria-controls={`appearance-panel-${tab.id}`}
        aria-selected={active === tab.id} tabIndex={active === tab.id ? 0 : -1}
        onClick={() => setActive(tab.id)} onKeyDown={event => onTabKey(event, index)}>{tab.label}</button>)}
    </div>
    {TABS.slice(0, 2).map(tab => <div key={tab.id} id={`appearance-panel-${tab.id}`} role="tabpanel"
      aria-labelledby={`appearance-tab-${tab.id}`} hidden={active !== tab.id} tabIndex={0}>
      {tab.id === 'uniqueness'
        ? <AppearanceCircles data={data} error={error} view={view} active={active === tab.id} />
        : <CommonnessChart data={data?.commonness} error={error} active={active === tab.id} />}
    </div>)}
    <div id="appearance-panel-unscored" role="tabpanel" aria-labelledby="appearance-tab-unscored" hidden={active !== 'unscored'} tabIndex={0}>
      <PendingRepetitionChart data={pending} error={error} />
    </div>
  </section>
}
