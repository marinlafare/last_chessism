import { useEffect, useState } from 'react'
import { styleTarget } from '../../appearance/styleCore.js'
import { fetchBackupVolume } from './backupsApi.js'
import { allocationDescription, backupVolumeLayout, formatBytes } from './backupVolume.js'
import './backupVolume.css'

const target = id => styleTarget(`storage.${id}`, 'backups')

export default function BackupVolume() {
  const [snapshot, setSnapshot] = useState(null), [error, setError] = useState('')
  const [highlight, setHighlight] = useState(null)
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
  return <div className="backup-volume">
    <div className="backup-volume-heading">
      <span>Backup volume <strong>{formatBytes(snapshot?.total_bytes)}</strong></span>
      <span>Free on disk <strong>{formatBytes(snapshot?.free_bytes)}</strong></span>
    </div>
    {notice ? <p className="status-banner warn" role="status">{notice}</p> : null}
    {layout?.overcommitted ? <p className="status-banner warn" role="status">
      Allocations and the protected reserve exceed disk capacity by {formatBytes(layout.overcommitted)}.
      A proportional allocation map cannot fit; amounts are listed below.
    </p> : null}
    {layout?.reserveShortfall ? <p className="status-banner warn" role="status">
      The protected free reserve is short by {formatBytes(layout.reserveShortfall)}.
    </p> : null}
    <div {...target('disk')} className="backup-volume-disk" role="group"
      aria-label={`Backup volume capacity: ${formatBytes(snapshot?.total_bytes)}`} aria-busy={pending}>
      {layout && !layout.overcommitted ? layout.rows.filter(row => row.size > 0).map(row => {
        const description = allocationDescription(row)
        return <div key={row.id} className={`backup-volume-region is-${row.kind}`} tabIndex={0}
          data-allocation={row.id} data-highlighted={highlight === row.id} data-compact={row.percent < 12}
          role="img" aria-label={description} title={description}
          style={{ width: `${row.percent}%` }}
          onMouseEnter={() => setHighlight(row.id)} onMouseLeave={() => setHighlight(null)}
          onFocus={() => setHighlight(row.id)} onBlur={() => setHighlight(null)}>
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
    {layout ? <ul className="backup-volume-key" aria-label="Disk allocations and usage">
      {layout.rows.map(row => <li key={row.id} tabIndex={0} title={allocationDescription(row)}
        data-highlighted={highlight === row.id}
        onMouseEnter={() => setHighlight(row.id)} onMouseLeave={() => setHighlight(null)}
        onFocus={() => setHighlight(row.id)} onBlur={() => setHighlight(null)}>
        <span {...target(row.kind === 'application' || row.kind === 'backup' ? 'used' : row.kind)}
          className={`backup-volume-swatch is-${row.kind}`} aria-hidden="true" />
        <span><strong>{row.label}</strong> {row.reserved
          ? <>{formatBytes(row.used)} used / {formatBytes(row.reserved)} allocated</>
          : <>{formatBytes(row.kind === 'backup' ? row.used : row.size)}{row.kind === 'backup' ? ' used · no published reservation' : ''}</>}</span>
      </li>)}
    </ul> : null}
    <p className="database-backups-storage-note">
      Blue = stored data. Outlined space = allocation, not a separate partition.
      {snapshot?.measured_at ? ` Measured ${new Date(snapshot.measured_at).toLocaleString()}.` : ''}
      {snapshot?.refreshing && layout ? ' Refreshing…' : ''}
      {snapshot?.stale && notice && layout ? ' Showing the last measurement.' : ''}
    </p>
  </div>
}
