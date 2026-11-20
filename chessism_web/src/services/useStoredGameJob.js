import { useCallback, useMemo, useSyncExternalStore } from 'react'
import { parseStoredJob, storedJobSnapshot, subscribeStoredJob } from './gameJobService.js'

export function useStoredGameJob(storageKey) {
  const subscribe = useCallback(listener => subscribeStoredJob(storageKey, listener), [storageKey])
  // The serialized snapshot is stable. React rechecks it after subscribing, which
  // also covers a write between rendering and installing the subscription.
  const snapshot = useSyncExternalStore(subscribe, () => storedJobSnapshot(storageKey), () => null)
  return useMemo(() => parseStoredJob(snapshot), [snapshot])
}
