import { useEffect, useState } from 'react'
import { elapsedLabel, timestamp } from './processTiming.js'

export default function ProcessTiming({ started, finished, active = false, pending = false, ...props }) {
  const [now, setNow] = useState(Date.now)
  useEffect(() => {
    if (!active || timestamp(started) == null || finished) return undefined
    setNow(Date.now())
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [active, started, finished])
  const value = timestamp(started)
  return <small {...props} className={`process-timing ${props.className || ''}`}>
    {value == null ? pending ? 'Not started' : 'Start time not recorded' : <>
      <span>Started <time dateTime={new Date(value).toISOString()}>{new Date(value).toLocaleString(undefined,
        { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' })}</time></span>
      {(active || finished) && <span> · {finished ? 'Duration' : 'Elapsed'} {elapsedLabel(started, finished, now)}</span>}
    </>}
  </small>
}
