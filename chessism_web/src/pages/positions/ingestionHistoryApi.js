import { getJson } from '../../services/apiClient'

export const fetchIngestionHistoryDays = (page, timezone, { signal } = {}) => (
  getJson(`/fens/pipeline/timings/history/days?${new URLSearchParams({ limit: 20, offset: page * 20, timezone })}`, { signal })
)

export async function fetchIngestionHistoryDay(day, timezone, { signal } = {}) {
  const runs = new Map()
  let offset = 0
  let hasMore = true
  while (hasMore) {
    const payload = await getJson(`/fens/pipeline/timings/history?${new URLSearchParams({
      limit: 50, offset, day, timezone,
    })}`, { signal })
    for (const run of payload.runs) runs.set(run.run_id, run)
    offset += payload.runs.length
    hasMore = payload.has_more && payload.runs.length > 0
  }
  return { day, runs: [...runs.values()] }
}

export const fetchIngestionHistoryRun = (runId, { signal } = {}) => (
  getJson(`/fens/pipeline/timings/history/${encodeURIComponent(runId)}`, { signal })
)
