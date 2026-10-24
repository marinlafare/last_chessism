# Local billing panel

System Status → **gcloud bill** reads the existing standard BigQuery billing
export for `chessism-production`. It shows this and last month's **usage-date**
costs in UTC, credits, net costs, services and the ten most recent exported days.
It is not a final invoice, a live quote, or an all-project account balance.
Missing export rows are displayed as missing data, never zero spending.

The `cloud-billing` Compose service is separate from `cloud-controller`; it has
no Docker socket, database credentials, published port, or compute-management
loop. The existing superadmin-only API proxies its `/billing` endpoint. Host
gcloud credentials are mounted read-only, copied into private tmpfs after
dropping privileges, and refreshed using the existing expiry-aware provider.
Credentials never reach the browser/API container. No new IAM grants are made.

There is one hard-coded SELECT with project/date filters and a partition bound.
No user SQL, project ID or table name is accepted. Each read is limited to
100 MiB billed bytes and asks BigQuery for a 30-second job timeout. Results are
aggregated with decimal arithmetic; currencies are never added together. Partial
query results are refused. Successful reads are shared/cached for one hour;
failures have a five-minute cooldown and retain clearly marked stale data.
No scheduler or frontend polling submits billing queries. Expanding the panel
or clicking Check for updates reads the shared cache and refreshes only if due.
Closing the panel aborts the browser request, but a query already in flight can
finish and populate the cache. Restarting this local service clears its cache.

Start/update only these services (never the analysis controller for a UI change):

```sh
docker compose build chessism-api cloud-billing
docker compose up -d --no-deps chessism-api cloud-billing
```

The reader needs the existing gcloud login to have `bigquery.jobs.create` in the
project and read access to the billing-export table. Errors do not grant access
or modify IAM automatically. To diagnose locally:

```sh
docker compose ps cloud-billing
docker compose logs --tail=30 cloud-billing
PYTHONPATH=stockfish-batch-worker .venv-cloud/bin/python -m unittest discover -s tests -p test_cloud_billing.py
```

References: [billing cost/credit queries](https://docs.cloud.google.com/billing/docs/how-to/bq-examples),
[query cost limits](https://docs.cloud.google.com/bigquery/docs/best-practices-costs),
[billing export latency](https://docs.cloud.google.com/billing/docs/how-to/export-data-bigquery).
