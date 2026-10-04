import { getJson, requestJson } from '../../services/apiClient'

export const fetchDatabaseBackups = ({ signal } = {}) => (
  getJson('/backups/database', { signal })
)

export const queueDatabaseBackup = () => (
  requestJson('/backups/database', { method: 'POST' })
)

export const queueDatabaseRestoreTest = () => (
  requestJson('/backups/database/test', { method: 'POST' })
)

export const fetchBackupJob = (jobId, { signal } = {}) => (
  getJson(`/jobs/${encodeURIComponent(jobId)}`, { signal })
)
