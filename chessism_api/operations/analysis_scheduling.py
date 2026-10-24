"""Balance each analysis pass across the existing independent engine workers.

The UI batch size is an upper limit. Pick a smaller dispatch size when needed
to spread the pass across whole waves of workers. For example, 5,000 FENs with
four workers and a 1,000-FEN limit becomes eight batches of 625, not five of
1,000. Workers still claim the next batch dynamically as they become available.
This balances position counts, not engine time; difficult positions can still
leave a shorter tail. It does not change nodes, engine threads or cooldowns.
"""


def balanced_analysis_batch_size(
    total_fens: int,
    requested_size: int,
    *,
    concurrency: int,
    service_limit: int,
) -> int:
    if min(requested_size, concurrency, service_limit) < 1:
        raise ValueError('Batch size, concurrency and service limit must be positive.')
    limit = min(requested_size, service_limit)
    if total_fens <= 0 or concurrency == 1:
        return limit
    wave_capacity = limit * concurrency
    waves = (total_fens + wave_capacity - 1) // wave_capacity
    slots = waves * concurrency
    return max(1, min(limit, (total_fens + slots - 1) // slots))
