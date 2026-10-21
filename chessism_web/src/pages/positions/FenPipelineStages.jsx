import { useEffect, useState } from 'react'
import { pageStyleTarget } from '../../appearance/styleCore.js'
import { formatNumber } from './positionPageSupport'
import { pipelineSteps, stepEstimate } from './pipelineProgress.js'
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
  const steps = pipelineSteps(ingestionTiming, jobState)
  const running = ingestionTiming ? ingestionTiming.status === 'running'
    : steps.some(step => ['working', 'running'].includes(step.status))
  useEffect(() => {
    if (!running) return undefined
    const timer = window.setInterval(() => setNow(Date.now() / 1000), 1000)
    return () => window.clearInterval(timer)
  }, [running])
  const groups = [...new Set(steps.map(step => step.group || 'Ingestion'))]
  if (!steps.length) return <p className="fen-pipeline-empty">No ingestion run recorded.</p>
  return (
    <div className="fen-pipeline-groups">
      {groups.map(group => (
        <div className="fen-pipeline-group" key={group}>
          <h3 {...pageStyleTarget('title')}>{group}</h3>
          {steps.filter(step => (step.group || 'Ingestion') === group).map(step => {
            const working = running && ['working', 'running'].includes(step.status)
            const status = working ? 'working'
              : ['working', 'running'].includes(step.status) ? 'stopped' : step.status
            const heartbeatAge = step.heartbeat_at ? Math.max(0, now - step.heartbeat_at) : null
            const stale = working && heartbeatAge != null && heartbeatAge > 30
            const percent = step.total > 0 ? Math.min(100, step.processed / step.total * 100) : null
            const elapsed = step.elapsed_seconds + (
              working && step.heartbeat_at ? Math.max(0, now - step.heartbeat_at) : 0
            )
            const estimate = stepEstimate(step)
            return (
              <article {...pageStyleTarget('card')} key={step.step}
                className={`fen-pipeline-stage is-${status}`}>
                <div className="fen-pipeline-stage-head">
                  <span>{step.label}</span>
                  <i className="fen-pipeline-live-light" title={status} aria-label={status} />
                </div>
                <strong {...pageStyleTarget('value')}>
                  {formatNumber(step.processed)}
                  {step.total != null ? ` / ${formatNumber(step.total)}` : ''}
                  {' '}<span className="fen-pipeline-unit">{step.unit || 'items'}</span>
                </strong>
                {percent != null && (
                  <div className="fen-pipeline-stage-track" role="progressbar"
                    aria-label={step.label} aria-valuemin={0} aria-valuemax={step.total}
                    aria-valuenow={step.processed}>
                    <div style={{ width: `${percent}%` }} />
                  </div>
                )}
                <div {...pageStyleTarget('text')} className="fen-pipeline-step-meta">
                  <span>{status === 'waiting' ? 'Between batches' : status}</span>
                  <span>{duration(elapsed)}</span>
                  {step.rate > 0 && <span>{formatNumber(Math.round(step.rate))} {step.unit || 'items'}/s</span>}
                  {estimate != null && <span>~{duration(estimate)} left</span>}
                </div>
                {working && (
                  <small {...pageStyleTarget('text')} className={stale ? 'is-stale' : ''}>
                    {stale ? 'Worker heartbeat delayed — checking status' : 'Worker active'}
                    {step.updated_at ? ` · progress ${duration(now - step.updated_at)} ago` : ''}
                  </small>
                )}
                {step.detail && <small {...pageStyleTarget('text')} title={step.detail}>{step.detail}</small>}
              </article>
            )
          })}
        </div>
      ))}
    </div>
  )
}
