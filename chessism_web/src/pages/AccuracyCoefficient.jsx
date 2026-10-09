import { useEffect, useMemo, useState } from 'react'
import Header from '../components/layout/Header'
import Footer from '../components/layout/Footer'
import SideRail from '../components/layout/SideRail'
import { formatNumber } from '../utils/formatters'
import {
  fetchCoefficientExperiment,
  fetchCoefficientExperiments,
  previewCoefficientDataset,
  recordCoefficientDecision,
  startCoefficientExperiment,
} from './research/coefficientResearchApi'
import './research/coefficientResearch.css'

const DEFAULT_FORM = {
  modes: ['bullet', 'blitz', 'rapid'],
  max_rating_gap: 200,
  opening_moves_excluded: 10,
  minimum_games_per_fit: 500,
  cp_ceiling: 1000,
  split_seed: 20260930,
}

const MODE_LABELS = ['bullet', 'blitz', 'rapid']
const DECISION_LABELS = {
  keep_lichess: 'Keep Lichess only',
  alongside: 'Run alongside Lichess',
  replace: 'Replace Lichess',
}

const coefficientText = (value) => (
  Number.isFinite(Number(value)) ? Number(value).toFixed(8) : '-'
)

function CoefficientPlot({ fits, mode }) {
  const points = fits.filter((fit) => (
    fit.status === 'fitted' &&
    fit.mode === mode &&
    fit.rating_bin?.key !== 'all'
  ))
  if (!points.length) return <p className="research-empty">No fitted rating bins for this mode.</p>

  const width = 900
  const height = 260
  const inset = { left: 64, right: 24, top: 24, bottom: 48 }
  const coefficients = [0.00368208, ...points.map((point) => Number(point.coefficient))]
  const minimum = Math.min(...coefficients) * 0.9
  const maximum = Math.max(...coefficients) * 1.1
  const range = Math.max(0.0001, maximum - minimum)
  const x = (index) => inset.left + (
    (index / Math.max(1, points.length - 1)) * (width - inset.left - inset.right)
  )
  const y = (value) => inset.top + (
    ((maximum - value) / range) * (height - inset.top - inset.bottom)
  )
  const line = points.map((point, index) => `${x(index)},${y(Number(point.coefficient))}`).join(' ')

  return (
    <div className="coefficient-plot-wrap">
      <svg className="coefficient-plot" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Coefficient by rating bin">
        <line className="coefficient-baseline" x1={inset.left} x2={width - inset.right} y1={y(0.00368208)} y2={y(0.00368208)} />
        <text className="coefficient-baseline-label" x={width - inset.right} y={y(0.00368208) - 7} textAnchor="end">LICHESS 0.00368208</text>
        <polyline className="coefficient-line" points={line} />
        {points.map((point, index) => (
          <g key={point.rating_bin.key}>
            <circle className="coefficient-point" cx={x(index)} cy={y(Number(point.coefficient))} r="5" />
            <text className="coefficient-x-label" x={x(index)} y={height - 18} textAnchor="middle">{point.rating_bin.label}</text>
            <title>{`${point.rating_bin.label}: ${coefficientText(point.coefficient)}`}</title>
          </g>
        ))}
      </svg>
    </div>
  )
}

function DatasetTable({ overview }) {
  return (
    <div className="research-table-wrap">
      <table className="research-table">
        <thead>
          <tr>
            <th>Rating bin</th>
            <th>Games</th>
            <th>Candidate positions</th>
            {MODE_LABELS.map((mode) => <th key={mode}>{mode}</th>)}
          </tr>
        </thead>
        <tbody>
          {(overview?.bins || []).map((bin) => (
            <tr key={bin.key}>
              <td>{bin.label}</td>
              <td>{formatNumber(bin.games)}</td>
              <td>{formatNumber(bin.candidate_positions)}</td>
              {MODE_LABELS.map((mode) => (
                <td key={mode}>{formatNumber(bin.modes?.[mode]?.games || 0)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function ResultsTable({ fits, mode }) {
  const rows = fits.filter((fit) => fit.mode === mode)
  return (
    <div className="research-table-wrap">
      <table className="research-table research-results-table">
        <thead>
          <tr>
            <th>Rating bin</th>
            <th>Coefficient</th>
            <th>95% interval</th>
            <th>Train games</th>
            <th>Test games</th>
            <th>Test MSE</th>
            <th>vs Lichess</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((fit) => {
            const interval = fit.confidence_interval_95
            return (
              <tr key={`${fit.rating_bin.key}-${fit.mode}`}>
                <td>{fit.rating_bin.label}</td>
                <td>{fit.status === 'fitted' ? coefficientText(fit.coefficient) : 'insufficient data'}</td>
                <td>{interval ? `${coefficientText(interval[0])} – ${coefficientText(interval[1])}` : '-'}</td>
                <td>{formatNumber(fit.samples?.training?.games || 0)}</td>
                <td>{formatNumber(fit.samples?.test?.games || 0)}</td>
                <td>{fit.test?.mse == null ? '-' : Number(fit.test.mse).toFixed(6)}</td>
                <td className={Number(fit.test?.improvement_percent) > 0 ? 'research-positive' : 'research-negative'}>
                  {fit.test?.improvement_percent == null ? '-' : `${Number(fit.test.improvement_percent).toFixed(2)}%`}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

export default function AccuracyCoefficient() {
  const [form, setForm] = useState(DEFAULT_FORM)
  const [overview, setOverview] = useState(null)
  const [experiments, setExperiments] = useState([])
  const [selected, setSelected] = useState(null)
  const [resultMode, setResultMode] = useState('all')
  const [decisionStrategy, setDecisionStrategy] = useState('rating_bins')
  const [decisionNotes, setDecisionNotes] = useState('')
  const [loadingPreview, setLoadingPreview] = useState(true)
  const [starting, setStarting] = useState(false)
  const [error, setError] = useState('')

  const configPayload = () => ({
    ...form,
    max_rating_gap: Number(form.max_rating_gap),
    opening_moves_excluded: Number(form.opening_moves_excluded),
    minimum_games_per_fit: Number(form.minimum_games_per_fit),
    cp_ceiling: Number(form.cp_ceiling),
    split_seed: Number(form.split_seed),
  })

  const loadExperiments = async () => {
    const payload = await fetchCoefficientExperiments()
    const rows = Array.isArray(payload.experiments) ? payload.experiments : []
    setExperiments(rows)
    return rows
  }

  const loadPreview = async () => {
    setLoadingPreview(true)
    try {
      const payload = await previewCoefficientDataset(configPayload())
      setOverview(payload)
      setError('')
    } catch (previewError) {
      setError(previewError.message || 'Unable to preview the research dataset.')
    } finally {
      setLoadingPreview(false)
    }
  }

  useEffect(() => {
    loadPreview()
    loadExperiments().then((rows) => {
      if (rows[0]) setSelected(rows[0])
    }).catch((loadError) => setError(loadError.message || 'Unable to load experiments.'))
  }, [])

  useEffect(() => {
    if (!selected || !['queued', 'running'].includes(selected.status)) return undefined
    const timer = window.setInterval(async () => {
      try {
        const payload = await fetchCoefficientExperiment(selected.id)
        setSelected(payload)
        if (!['queued', 'running'].includes(payload.status)) {
          await loadExperiments()
          await loadPreview()
        }
      } catch (pollError) {
        setError(pollError.message || 'Unable to refresh the experiment.')
      }
    }, 3000)
    return () => window.clearInterval(timer)
  }, [selected?.id, selected?.status])

  const toggleMode = (mode) => {
    setForm((current) => {
      const selectedModes = current.modes.includes(mode)
        ? current.modes.filter((item) => item !== mode)
        : [...current.modes, mode]
      return { ...current, modes: selectedModes.length ? selectedModes : [mode] }
    })
  }

  const updateNumber = (key, value) => setForm((current) => ({ ...current, [key]: value }))

  const startExperiment = async () => {
    setStarting(true)
    setError('')
    try {
      const started = await startCoefficientExperiment(configPayload())
      const payload = await fetchCoefficientExperiment(started.experiment_id)
      setSelected(payload)
      await loadExperiments()
    } catch (startError) {
      setError(startError.message || 'Unable to start coefficient research.')
    } finally {
      setStarting(false)
    }
  }

  const selectExperiment = async (experimentId) => {
    try {
      setSelected(await fetchCoefficientExperiment(experimentId))
      setError('')
    } catch (selectError) {
      setError(selectError.message || 'Unable to open the experiment.')
    }
  }

  const saveDecision = async (action) => {
    if (!selected?.id) return
    try {
      const payload = await recordCoefficientDecision(selected.id, {
        action,
        strategy: decisionStrategy,
        notes: decisionNotes,
      })
      setSelected(payload)
      await loadExperiments()
      setError('')
    } catch (decisionError) {
      setError(decisionError.message || 'Unable to record the decision.')
    }
  }

  const fits = selected?.result?.fits || []
  const progress = selected?.progress || {}
  const progressPercent = progress.total
    ? Math.min(100, (Number(progress.processed || 0) / Number(progress.total)) * 100)
    : 0
  const activeExperiment = experiments.some((item) => ['queued', 'running'].includes(item.status))
  const canStart = Boolean(overview?.resources?.safe_to_start) && !activeExperiment && !starting
  const globalFit = useMemo(
    () => fits.find((fit) => fit.mode === resultMode && fit.rating_bin?.key === 'all'),
    [fits, resultMode]
  )

  return (
    <div className="page-frame">
      <SideRail />
      <div className="home-shell">
        <Header />
        <main className="research-main">
          <section className="research-hero">
            <div className="section-head">
              <div>
                <p className="eyebrow">SUPERUSER RESEARCH</p>
                <h1>Chessism Coefficient</h1>
                <p className="research-subtitle">Fit Stockfish centipawns to observed game outcomes without changing live accuracy.</p>
              </div>
              <div className="research-baseline">
                <span>Lichess constant</span>
                <strong>0.00368208</strong>
              </div>
            </div>
            {error ? <div className="status-banner warn">{error}</div> : null}
            {overview?.resources?.analysis_active ? (
              <div className="status-banner warn">Stockfish analysis is active. Dataset inspection is available, but starting research is locked.</div>
            ) : (
              <div className="status-banner">Stockfish is idle. A coefficient experiment can run on the isolated research worker.</div>
            )}
          </section>

          <section className="research-panel">
            <div className="section-head">
              <div><p className="eyebrow">DATASET</p><h2>Experiment Configuration</h2></div>
              <div className="research-actions">
                <button className="btn btn-secondary btn-inline" type="button" onClick={loadPreview} disabled={loadingPreview}>
                  {loadingPreview ? 'Inspecting' : 'Refresh preview'}
                </button>
                <button className="btn btn-primary btn-inline" type="button" onClick={startExperiment} disabled={!canStart}>
                  {starting ? 'Queueing' : 'Run research'}
                </button>
              </div>
            </div>

            <div className="research-mode-row" aria-label="Included modes">
              {MODE_LABELS.map((mode) => (
                <button className={`research-toggle ${form.modes.includes(mode) ? 'active' : ''}`} type="button" key={mode} onClick={() => toggleMode(mode)}>
                  {mode}
                </button>
              ))}
            </div>

            <div className="research-config-grid">
              <label><span>Maximum rating gap</span><input type="number" min="0" max="1000" value={form.max_rating_gap} onChange={(event) => updateNumber('max_rating_gap', event.target.value)} /></label>
              <label><span>Opening moves excluded</span><input type="number" min="0" max="40" value={form.opening_moves_excluded} onChange={(event) => updateNumber('opening_moves_excluded', event.target.value)} /></label>
              <label><span>Minimum games / fit</span><input type="number" min="50" value={form.minimum_games_per_fit} onChange={(event) => updateNumber('minimum_games_per_fit', event.target.value)} /></label>
              <label><span>CP ceiling</span><input type="number" min="100" max="2000" value={form.cp_ceiling} onChange={(event) => updateNumber('cp_ceiling', event.target.value)} /></label>
              <label><span>Split seed</span><input type="number" min="1" value={form.split_seed} onChange={(event) => updateNumber('split_seed', event.target.value)} /></label>
            </div>

            <div className="research-summary-grid">
              <article><span>Eligible games</span><strong>{loadingPreview ? '-' : formatNumber(overview?.totals?.games || 0)}</strong></article>
              <article><span>Candidate positions</span><strong>{loadingPreview ? '-' : formatNumber(overview?.totals?.candidate_positions || 0)}</strong></article>
              <article><span>Weighting</span><strong>1 game = 1</strong></article>
              <article><span>Split</span><strong>70 / 15 / 15</strong></article>
            </div>
            <DatasetTable overview={overview} />
            <p className="research-note">{overview?.position_note}</p>
          </section>

          <section className="research-panel">
            <div className="section-head">
              <div><p className="eyebrow">EXPERIMENTS</p><h2>Saved Runs</h2></div>
            </div>
            <div className="research-history">
              {experiments.map((experiment) => (
                <button className={`research-history-item ${selected?.id === experiment.id ? 'active' : ''}`} type="button" key={experiment.id} onClick={() => selectExperiment(experiment.id)}>
                  <span>{experiment.created_at ? new Date(experiment.created_at).toLocaleString() : experiment.id}</span>
                  <strong>{experiment.status}</strong>
                  <small>{experiment.dataset_summary ? `${formatNumber(experiment.dataset_summary.eligible_games)} games` : 'waiting for dataset'}</small>
                </button>
              ))}
              {!experiments.length ? <p className="research-empty">No coefficient experiments yet.</p> : null}
            </div>

            {selected && ['queued', 'running'].includes(selected.status) ? (
              <div className="research-progress" aria-live="polite">
                <div><strong>{String(progress.phase || selected.status).toUpperCase()}</strong><span>{progress.detail || 'Waiting for the research worker.'}</span></div>
                <div className="research-progress-track"><div style={{ width: `${progressPercent}%` }} /></div>
              </div>
            ) : null}

            {selected?.status === 'failed' ? <div className="status-banner warn">{selected.error || 'Experiment failed.'}</div> : null}
          </section>

          {selected?.status === 'complete' ? (
            <section className="research-panel">
              <div className="section-head">
                <div><p className="eyebrow">RESULTS</p><h2>Coefficient Candidates</h2></div>
                <div className="research-mode-row compact">
                  {['all', ...MODE_LABELS].map((mode) => (
                    <button className={`research-toggle ${resultMode === mode ? 'active' : ''}`} type="button" key={mode} onClick={() => setResultMode(mode)}>{mode}</button>
                  ))}
                </div>
              </div>
              <div className="research-result-cards">
                <article><span>Global Chessism coefficient</span><strong>{coefficientText(globalFit?.coefficient)}</strong></article>
                <article><span>Global test improvement</span><strong>{globalFit?.test?.improvement_percent == null ? '-' : `${Number(globalFit.test.improvement_percent).toFixed(2)}%`}</strong></article>
                <article><span>Eligible games</span><strong>{formatNumber(selected.dataset_summary?.eligible_games || 0)}</strong></article>
                <article><span>Eligible positions</span><strong>{formatNumber(selected.dataset_summary?.eligible_position_appearances || 0)}</strong></article>
              </div>
              <CoefficientPlot fits={fits} mode={resultMode} />
              <ResultsTable fits={fits} mode={resultMode} />
              <p className="research-note">Artifact: {selected.result?.artifact_path || '-'}</p>
            </section>
          ) : null}

          {selected?.status === 'complete' ? (
            <section className="research-panel research-decision-panel">
              <div className="section-head"><div><p className="eyebrow">DECISION</p><h2>Production Direction</h2></div></div>
              <p className="research-note">This records the intended direction only. It does not recalculate or replace live accuracy.</p>
              <div className="research-strategy-row">
                <label><input type="radio" name="strategy" checked={decisionStrategy === 'global'} onChange={() => setDecisionStrategy('global')} /> One global Chessism coefficient</label>
                <label><input type="radio" name="strategy" checked={decisionStrategy === 'rating_bins'} onChange={() => setDecisionStrategy('rating_bins')} /> Coefficients by rating bin</label>
              </div>
              <textarea value={decisionNotes} onChange={(event) => setDecisionNotes(event.target.value)} placeholder="Research notes and reasons for the decision" />
              <div className="research-decision-actions">
                {Object.entries(DECISION_LABELS).map(([action, label]) => (
                  <button className={`btn ${action === 'replace' ? 'btn-primary' : 'btn-secondary'} btn-inline`} type="button" key={action} onClick={() => saveDecision(action)}>{label}</button>
                ))}
              </div>
              {selected.decision ? (
                <div className="status-banner">Recorded: {DECISION_LABELS[selected.decision]} · {selected.decision_config?.strategy === 'rating_bins' ? 'rating-bin coefficients' : 'global coefficient'}. Production unchanged.</div>
              ) : null}
            </section>
          ) : null}
        </main>
        <Footer />
      </div>
    </div>
  )
}
