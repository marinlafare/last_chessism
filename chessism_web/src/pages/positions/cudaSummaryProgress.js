// This meter describes the current stage, never a fabricated whole-run percent.
const number = value => Number(value || 0).toLocaleString()
const count = (done, total, unit) => `${number(done)}${total > 0 ? ` / ${number(total)}` : ''} ${unit}`

export function cudaSummaryProgress(run) {
  const phase = run?.phase || 'Queued'
  const [title, ...detail] = phase.split(' — ')
  const result = { phase: title, value: undefined, max: null, detail: detail.join(' — '), style: 'cuda.progress.count', moving: false }
  if (!run) return result
  if (run.status === 'complete') return { ...result, phase: 'Complete', detail: '', value: 1, max: 1 }
  if (run.status !== 'running' || /waiting|queued/i.test(phase)) return result
  result.moving = true
  const timings = run.timings || {}
  const publishing = /publishing|refreshing/i.test(phase)
  const show = (detail, value, max, style = result.style) => {
    const known = Number.isFinite(value) && Number.isFinite(max) && max > 0
    return { ...result, phase: detail ? phase.split(' — ')[0] : phase, detail,
      value: known ? value : undefined, max: known ? max : null, style }
  }

  if (/ply map/i.test(phase)) {
    const state = timings.ply_map
    return state && !state.complete && !state.prepared
      ? show(`${number(state.captured_games)} games captured`, undefined, null, 'cuda.progress.ply_map') : result
  }
  if (/position frequencies|repeated positions/i.test(phase)) {
    const state = timings.position_frequency
    if (!state || state.complete) return result
    const style = 'cuda.progress.position_frequency'
    if (!state.prepared) return show(`${number(state.rows)} occurrences prepared`, undefined, null, style)
    if (publishing) return show(count(state.published, state.players?.length, 'players'), state.published, state.players?.length, style)
    return show(count(state.shards, state.shard_total, 'groups calculated'), state.shards, state.shard_total, style)
  }
  if (/salience/i.test(phase)) {
    const state = timings.salience
    if (!state || state.complete || publishing) return result
    const style = 'cuda.progress.salience'
    if (!state.prepared) return show(`${number(state.games)} games · ${number(state.rows)} early positions`, undefined, null, style)
    if (/reducing/i.test(phase) || (state.shard_total > 0 && state.shards >= state.shard_total)) {
      // Shard completion is not game reduction/publication completion. Keep
      // that remaining work visible even before the next worker tick reports.
      if (state.reduced) return { ...result, phase: 'Publishing salience', detail: '' }
      return { ...show(count(state.reduced_games, state.games, 'games finalized'), Number(state.reduced_games || 0), state.games, style),
        phase: 'Finalizing salience' }
    }
    return show(count(state.shards, state.shard_total, 'groups calculated'), state.shards, state.shard_total, style)
  }
  if (/metadata|behavioral|ending/i.test(phase)) {
    const state = timings.metadata
    if (!state || state.complete || publishing) return result
    if (!state.ready) return show(`${number(state.rows)} player-games prepared`)
    return show(count(state.computed, state.chunks?.length, 'input chunks calculated'), state.computed, state.chunks?.length)
  }
  if (/preparing|loading|inputs ready/i.test(phase)) {
    const state = timings.input_snapshot
    return state ? show(count(state.games, run.total_games, 'games prepared'), state.games, run.total_games) : result
  }
  if (publishing || /complete|changed|finaliz/i.test(phase)) return result
  if (Number(run.total_games) > 0) {
    const changed = Number(run.superseded_games || 0)
    return show(count(run.processed_games, run.total_games, 'games processed') + (changed ? ` · ${number(changed)} changed` : ''),
      Number(run.processed_games || 0) + changed, Number(run.total_games))
  }
  return result
}
