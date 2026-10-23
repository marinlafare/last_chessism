import { useEffect, useState } from 'react'
import { styleTarget } from '../../appearance/styleCore.js'
import { fetchBackupVolume } from './backupsApi.js'
import { allocationDescription, backupVolumeLayout, formatBytes } from './backupVolume.js'
import './backupVolume.css'

const target = id => styleTarget(`storage.${id}`, 'backups')

export default function BackupVolume() {
  const [snapshot, setSnapshot] = useState(null), [error, setError] = useState('')
  const [hovered, setHovered] = useState(null), [focused, setFocused] = useState(null)
  const [dismissed, setDismissed] = useState(false)
  const highlight = dismissed ? null : hovered ?? focused
  useEffect(() => {
    const controller = new AbortController()
    let timer
    const load = async () => {
      let delay = 60000
      try {
        const data = await fetchBackupVolume({ signal: controller.signal })
        if (controller.signal.aborted) return
        setSnapshot(data); setError('')
        if (data.refreshing) delay = 2500
      } catch (failure) {
        if (controller.signal.aborted) return
        setError(failure.message || 'Unable to read backup-volume usage.')
      }
      if (!controller.signal.aborted) timer = window.setTimeout(load, delay)
    }
    load()
    return () => { controller.abort(); window.clearTimeout(timer) }
  }, [])
  const layout = snapshot?.available === false ? null : backupVolumeLayout(snapshot)
  const notice = error || snapshot?.error
  const pending = !layout && (!snapshot || snapshot.refreshing) && !notice
  const detail = !layout?.overcommitted && layout?.rows.find(row => row.id === highlight && row.size > 0)
  const measured = new Date(snapshot?.measured_at || '')
  const measuredAt = Number.isFinite(measured.getTime()) ? measured : null
  return <div className="backup-volume">
    <div className="section-head backup-volume-section-head">
      <div><p {...styleTarget('text', 'backups')} className="eyebrow">STORAGE</p><h2 {...styleTarget('title', 'backups')}>Protected Allocation</h2></div>
      <p {...target('measured')} className="backup-volume-measured" aria-busy={Boolean(snapshot?.refreshing)}
        title={snapshot?.refreshing ? 'Refreshing measurement…' : snapshot?.stale ? 'Showing the last measurement.' : undefined}>
        Last measured: {measuredAt ? <time dateTime={measuredAt.toISOString()}>{measuredAt.toLocaleString()}</time> : 'Not yet measured'}
      </p>
    </div>
    <div className="backup-volume-heading">
      <span>Backup volume <strong>{formatBytes(snapshot?.total_bytes)}</strong></span>
      <span>Free on disk <strong>{formatBytes(snapshot?.free_bytes)}</strong></span>
    </div>
    {notice ? <p className="status-banner warn" role="status">{notice}</p> : null}
    {layout?.overcommitted ? <p className="status-banner warn" role="status">
      Allocations and the protected reserve exceed disk capacity by {formatBytes(layout.overcommitted)}.
      A proportional allocation map cannot fit.
    </p> : null}
    {layout?.reserveShortfall ? <p className="status-banner warn" role="status">
      The protected free reserve is short by {formatBytes(layout.reserveShortfall)}.
    </p> : null}
    <div className="backup-volume-chart">
    <div {...target('disk')} className="backup-volume-disk" role="group"
      aria-label={`Backup volume capacity: ${formatBytes(snapshot?.total_bytes)}`} aria-busy={pending}>
      {layout && !layout.overcommitted ? layout.rows.filter(row => row.size > 0).map(row => {
        const description = allocationDescription(row)
        return <div key={row.id} className={`backup-volume-region is-${row.kind}`} tabIndex={0}
          data-allocation={row.id} data-highlighted={highlight === row.id} data-compact={row.percent < 12}
          role="img" aria-label={description}
          style={{ width: `${row.percent}%` }}
          onPointerEnter={() => { setHovered(row.id); setDismissed(false) }} onPointerLeave={() => setHovered(null)}
          onFocus={() => { setFocused(row.id); setDismissed(false) }} onBlur={() => setFocused(null)}
          onKeyDown={event => { if (event.key === 'Escape') setDismissed(true) }}>
          <div {...target(row.kind === 'application' || row.kind === 'backup' ? 'allocation' : row.kind)} className="backup-volume-box">
            {row.used !== undefined ? <span {...target(row.kind === 'other' ? 'other' : 'used')}
              className="backup-volume-fill" style={{ width: `${row.fillPercent}%` }} /> : null}
            <span className="backup-volume-label"><strong>{row.label}</strong>
              <span>{row.reserved ? `${formatBytes(row.used)} / ${formatBytes(row.reserved)}` : formatBytes(row.size)}</span>
            </span>
          </div>
        </div>
      }) : <span className="backup-volume-placeholder" role="status">
        {pending ? 'Measuring backup folders…' : layout?.overcommitted ? 'Allocation exceeds capacity' : 'Disk usage unavailable'}
      </span>}
    </div>
    <div {...target('tooltip')} className="backup-volume-tooltip" role="tooltip" hidden={!detail}>
      {detail ? allocationDescription(detail) : ''}
    </div>
    </div>
  </div>
}
