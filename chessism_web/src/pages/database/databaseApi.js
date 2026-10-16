import { getJson } from '../../services/apiClient'

export const fetchDatabaseSchema = ({ signal } = {}) => getJson('/database/schema', { signal })
export const fetchDatabasePreview = (table, { signal } = {}) => getJson(`/database/preview?table=${encodeURIComponent(table)}`, { signal })
