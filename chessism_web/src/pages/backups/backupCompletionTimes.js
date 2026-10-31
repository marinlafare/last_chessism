const latestDate = values => {
  const timestamps = values
    .filter(Boolean)
    .map(value => new Date(value).getTime())
    .filter(Number.isFinite)
  return timestamps.length ? new Date(Math.max(...timestamps)).toISOString() : null
}

// Use completed records, not the start time of a new operation. Failed backups
// are not recovery points; a finished restore test may have passed or failed.
export function backupCompletionTimes(overview) {
  const backups = Array.isArray(overview?.backups) ? overview.backups : []
  const status = overview?.status || {}
  const test = overview?.restore_test || {}
  return {
    backup: latestDate([
      ...backups.map(backup => backup.completed_at),
      status.last_verified_at,
      status.status === 'success' ? status.completed_at : null,
    ]),
    test: latestDate([
      ...backups.map(backup => backup.restore_tested_at),
      ['success', 'failed'].includes(test.status) ? test.completed_at : null,
    ]),
  }
}
