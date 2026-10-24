function pause(milliseconds, signal) {
  return new Promise((resolve, reject) => {
    const abort = () => {
      clearTimeout(timer)
      reject(new DOMException('Request aborted', 'AbortError'))
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', abort)
      resolve()
    }, milliseconds)
    if (signal?.aborted) abort()
    else signal?.addEventListener('abort', abort, { once: true })
  })
}

export async function pollScoreDistribution(request, { signal, onProgress } = {}) {
  while (true) {
    if (signal?.aborted) throw new DOMException('Request aborted', 'AbortError')
    const data = await request()
    if (data.status !== 'calculating') return data
    onProgress?.(data.progress)
    // One calculation is shared server-side; polls only read its progress.
    const delay = Math.max(250, Math.min(5000, Number(data.retry_after_seconds || 1) * 1000))
    await pause(delay, signal)
  }
}
