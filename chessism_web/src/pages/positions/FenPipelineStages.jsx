import { useEffect, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import ProgressMeter from '../../components/progress/ProgressMeter.jsx'
import { formatNumber } from './positionPageSupport'
import { pipelineSteps, compactPipelineSteps, stepEstimate, graphIngestionActive } from './pipelineProgress.js'
import './fenIngestion.css'

function duration(seconds) {
  if (!Number.isFinite(seconds)) return '—'
  const value = Math.max(0, Math.floor(seconds))
  if (value < 60) return `${value}s`
  if (value < 3600) return `${Math.floor(value / 60)}m ${value % 60}s`
  return `${Math.floor(value / 3600)}h ${Math.floor((value % 3600) / 60)}m`
}

export default function FenPipelineStages({ jobState, ingestionTiming }) {
  const [now, setNow] = useState(Date.now() / 1000)
  const [details, setDetails] = useState(false)
  const steps = pipelineSteps(ingestionTiming, jobState)
  const summary = compactPipelineSteps(ingestionTiming, jobState)
  const running = ingestionTiming ? ingestionTiming.status === 'running' || graphIngestionActive(ingestionTiming)
    : steps.some(step => ['working', 'running'].includes(step.status))
  useEffect(() => { setDetails(false) }, [ingestionTiming?.run_id])
  useEffect(() => {
    if (!running) return undefined
    const timer = window.setInterval(() => setNow(Date.now() / 1000), 1000)
    return () => window.clearInterval(timer)
  }, [running])
  if (!steps.length) return <p className="fen-pipeline-empty">No ingestion run recorded.</p>
  return (
    <div className="fen-pipeline-overview">
      <div {...pageStyleTarget('ingestion.summary')} className="fen-pipeline-summary">
          {summary.map(step => {
            const working = running && ['working', 'running'].includes(step.status)
            const target = part => pageStyleTarget(`ingestion.${step.step}.${part}`)
            const status = working ? 'working'
              : ['working', 'running'].includes(step.status) ? 'stopped' : step.status
            const complete = ['complete', 'skipped'].includes(status)
            const unknown = !complete && !(step.total > 0)
            const heartbeatAge = step.heartbeat_at ? Math.max(0, now - step.heartbeat_at) : null
            const stale = working && heartbeatAge != null && heartbeatAge > 30
            const elapsed = (step.elapsed_seconds || 0) + (
              working && (step.elapsed_at || step.heartbeat_at) ? Math.max(0, now - (step.elapsed_at || step.heartbeat_at)) : 0
            )
            const estimate = stepEstimate(step)
            const label = complete ? (status === 'skipped' ? 'Not needed' : 'Complete')
              : step.currentLabel && step.currentStep !== 'neo4j_ingestion' ? step.currentLabel
                : status === 'pending' ? 'Waiting to start' : 'Synchronize games and evaluations'
            return (
              <article {...target('card')} key={step.step} data-ingestion-stage={step.step}
                className={`fen-pipeline-stage is-${status}`}>
                <div className="fen-pipeline-stage-head">
                  <span {...target('title')}>{step.label}</span>
                  <i {...target('light')} className="fen-pipeline-live-light" title={status} aria-label={status} />
                </div>
                <span {...target('text')} className="fen-pipeline-operation">{label}</span>
                <ProgressMeter {...target('track')}
                  className={`fen-pipeline-stage-track${unknown ? ' is-indeterminate' : ''}`}
                  fillProps={target('fill')} aria-label={`${step.label}: ${label}`}
                  aria-valuetext={unknown ? 'Completion percentage not yet available' : undefined}
                  max={step.total > 0 ? step.total : complete ? 1 : null}
                  value={complete ? step.total > 0 ? step.total : 1 : unknown ? undefined : step.processed}
                  indeterminateWidth={working ? 30 : 0} />
                <strong {...target('value')}>
                  {formatNumber(step.processed || 0)}
                  {step.total != null ? ` / ${formatNumber(step.total)}` : ''}
                  {' '}<span className="fen-pipeline-unit">{step.unit || 'items'}</span>
                </strong>
                <div {...target('text')} className="fen-pipeline-step-meta">
                  <span>{status === 'waiting' ? 'Between batches' : status}</span>
                  <span>{duration(elapsed)}</span>
                  {estimate != null && <span>~{duration(estimate)} left</span>}
                </div>
                {working && (
                  <small {...target('heartbeat')} className={stale ? 'is-stale' : ''}>
                    {stale ? 'Worker heartbeat delayed — checking status' : 'Worker active'}
                    {step.updated_at ? ` · progress ${duration(now - step.updated_at)} ago` : ''}
                  </small>
                )}
                {(!complete || step.step === 'neo4j') && step.detail && <small {...target('detail')} title={step.detail}>{step.detail}</small>}
              </article>
            )
          })}
      </div>
      <details {...pageStyleTarget('ingestion.details')} className="fen-pipeline-details" open={details}
        onToggle={event => setDetails(event.currentTarget.open)}>
        <summary {...pageStyleTarget('ingestion.details.toggle')}>Details ({steps.length} steps)</summary>
        {details && <div className="fen-pipeline-details-list">
          {steps.map(step => (
            <div {...pageStyleTarget('ingestion.details.row')} className="fen-pipeline-detail-row" key={step.step}>
              <span>{step.label || step.step}</span><span>{step.status}</span>
              <span>{formatNumber(step.processed || 0)}{step.total != null ? ` / ${formatNumber(step.total)}` : ''} {step.unit || 'items'}</span>
              <span>{duration(step.elapsed_seconds)}</span>
              {step.detail && <small>{step.detail}</small>}
            </div>
          ))}
        </div>}
      </details>
    </div>
  )
}
