# FEN batch-upload benchmark

All four benchmark scripts live here, separately from the reusable
`../stockfish_batch/` worker package:

- `benchmark.py`: runs the four sequential upload-mode comparisons inside one VM.
- `benchmark_dataset.py`: prepares a bounded, read-only local database sample.
- `render_benchmark.py`: renders a bounded Batch job; never submits it.
- `benchmark_audit.py`: monitors/audits the fixed, already-completed
  `upload-benchmark-001` job without creating or deleting cloud resources.

From `stockfish-batch-worker/`, display any command's help without starting work:

```bash
.venv/bin/python -m benchmark_fen_batches.benchmark --help
.venv/bin/python -m benchmark_fen_batches.benchmark_dataset --help
.venv/bin/python -m benchmark_fen_batches.render_benchmark --help
.venv/bin/python -m benchmark_fen_batches.benchmark_audit --help
```

Direct script invocation also works, for example
`python benchmark_fen_batches/render_benchmark.py --help`.
Dependencies remain in `../requirements.txt`; job rendering reuses the shared
`../scripts/render_job.py` helper and `../batch/smoke-test.json` template.

New Docker builds include only this package's initializer and runtime
`benchmark.py`, not the host-side database/export/audit tools. The normal worker
entry point is unchanged. New benchmark images run
`python -m benchmark_fen_batches.benchmark`.

The already-published benchmark image is unchanged and still contains the old
`stockfish_batch.benchmark` entry point. The renderer recognizes that exact image
digest and preserves its original command, so historical job audits continue to
work. Reorganizing these files does not rebuild/publish an image or launch a job.

Existing input/results/audit evidence stays in `../out/upload-benchmark-001/`
(gitignored). The completed cloud job and stored artifacts are not modified.
See the [worker README](../README.md#bounded-upload-benchmark) for test bounds,
measurements, limitations and safety checks.
