# Controlled offline sequence test

`run.py` explicitly enqueues one fixed 10,000-FEN selection as two sequential
5,000-FEN tasks, one normal 16-vCPU Spot VM at a time. It atomically inserts the
small initial cycle plan; the existing production controller selects/claims real
unscored FENs, uploads, starts Google Workflows, imports and cleans normally.
Normal UI splitting uses up to 1,000,000 FENs per VM per cycle. There is no
synthetic FEN or result data.

Use one unique 32-hex `--job-id` and retain it. `--enqueue` authorizes paid work;
without it the command only reads local status. Repeating the same enqueue is
idempotent and refuses any different pre-existing plan.

After Google acknowledgement, stop the repository's Compose application while
the first task is active. Do not stop Google jobs or the Google workflow. Use
gcloud from the host to observe both cycles. Restart with `docker compose start`
after the cloud sequence succeeds. Local import/verification and cleanup must
finish before calling the entire test successful. Never use `down -v`.

This exercises offline handoff, not deliberate Spot preemption. Existing
preemption/application retry policy and five-minute analysis watchdog remain.
FEN count is bounded; it is not a monetary or total-runtime cap.
