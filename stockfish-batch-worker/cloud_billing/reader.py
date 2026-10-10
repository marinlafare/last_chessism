"""Fixed read-only billing query. No user SQL, IAM edits, or compute operations."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import threading
import time
from uuid import uuid4

import httpx

from cleaning_job.cloud import AccessTokens

PROJECT = 'chessism-production'
TABLE = PROJECT + '.billing_export.gcp_billing_export_v1_016A6E_CC7B3B_9890D4'
LOCATION = 'US'
MAX_BYTES = 100 * 1024 * 1024
CACHE_SECONDS = 3600
ERROR_COOLDOWN = 300
SQL = f"""
SELECT CAST(DATE(usage_start_time, 'UTC') AS STRING) AS day,
       COALESCE(service.description, 'Other') AS service,
       currency,
       CAST(SUM(CAST(cost AS NUMERIC)) AS STRING) AS cost,
       CAST(SUM(IFNULL((SELECT SUM(CAST(c.amount AS NUMERIC))
                       FROM UNNEST(credits) AS c), 0)) AS STRING) AS credits,
       FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(usage_end_time), 'UTC') AS usage_through,
       FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(export_time), 'UTC') AS exported_at
FROM `{TABLE}`
WHERE project.id = @project
  AND usage_start_time >= TIMESTAMP(@start)
  AND usage_start_time < TIMESTAMP(@end)
  AND (_PARTITIONTIME >= TIMESTAMP(@start) OR _PARTITIONTIME IS NULL)
GROUP BY day, service, currency
ORDER BY day DESC, service, currency
"""


class BillingError(RuntimeError):
    pass


def periods(now):
    month = now.date().replace(day=1)
    previous = (month - timedelta(days=1)).replace(day=1)
    following = (month + timedelta(days=32)).replace(day=1)
    return [dict(id=month.strftime('%Y-%m'), start=month.isoformat(), end=following.isoformat()),
            dict(id=previous.strftime('%Y-%m'), start=previous.isoformat(), end=month.isoformat())]


def summarize(rows, now):
    """Keep decimal sums exact and separate currencies; zero records is not a zero bill."""
    result = []
    for period in periods(now):
        selected = [row for row in rows if period['start'] <= row['day'] < period['end']]
        currencies = []
        for currency in sorted({row['currency'] for row in selected}):
            matching = [row for row in selected if row['currency'] == currency]
            def totals(items):
                cost = sum((Decimal(row['cost']) for row in items), Decimal(0))
                credits = sum((Decimal(row['credits']) for row in items), Decimal(0))
                return dict(cost=str(cost), credits=str(credits), net=str(cost + credits))
            services = [dict(service=name, **totals([row for row in matching if row['service'] == name]))
                        for name in sorted({row['service'] for row in matching})]
            services.sort(key=lambda row: Decimal(row['net']), reverse=True)
            days = [dict(day=day, **totals([row for row in matching if row['day'] == day]))
                    for day in sorted({row['day'] for row in matching}, reverse=True)]
            currencies.append(dict(currency=currency, **totals(matching), services=services, days=days))
        result.append(dict(**period, currencies=currencies, has_data=bool(selected),
                           usage_through=max((row['usage_through'] for row in selected if row['usage_through']), default=None),
                           exported_at=max((row['exported_at'] for row in selected if row['exported_at']), default=None)))
    return dict(project=PROJECT, source=TABLE, timezone='UTC', fetched_at=now.isoformat(),
                next_refresh_at=(now + timedelta(seconds=CACHE_SECONDS)).isoformat(),
                max_bytes_billed=MAX_BYTES, periods=result)


class BillingReader:
    def __init__(self, tokens=None):
        self.tokens = tokens or AccessTokens('gcloud')

    def request(self, client, path, *, body=None, params=None):
        token, _ = self.tokens.get()
        for attempt in range(2):
            try:
                response = client.request('POST' if body is not None else 'GET',
                    'https://bigquery.googleapis.com/bigquery/v2/' + path,
                    headers={'Authorization': 'Bearer ' + token}, json=body, params=params)
                if response.status_code == 401 and attempt == 0:
                    token, _ = self.tokens.get(force=True, rejected_token=token)
                    continue
                if response.status_code == 403:
                    raise BillingError('Billing access denied. Check BigQuery access for the local gcloud login.')
                if response.status_code == 404:
                    raise BillingError('The configured billing export was not found.')
                if response.status_code != 200:
                    raise BillingError('Billing query failed or exceeded the 100 MiB scan limit; no partial totals are shown.')
                data = response.json()
                if data.get('errors'):
                    raise BillingError('Billing query returned an error; no partial totals are shown.')
                return data
            except (httpx.HTTPError, ValueError):
                raise BillingError('Could not read the billing export. Try again later.') from None

    def read(self):
        now = datetime.now(timezone.utc)
        ranges = periods(now)
        body = dict(query=SQL, useLegacySql=False, useQueryCache=True, location=LOCATION,
                    maximumBytesBilled=str(MAX_BYTES), timeoutMs=10000, jobTimeoutMs='30000',
                    requestId=str(uuid4()), maxResults=10000, parameterMode='NAMED',
                    labels={'purpose': 'chessism-billing-panel'}, queryParameters=[
                        dict(name=name, parameterType={'type': 'STRING'}, parameterValue={'value': value})
                        for name, value in [('project', PROJECT), ('start', ranges[1]['start']), ('end', ranges[0]['end'])]])
        with httpx.Client(timeout=15, follow_redirects=False, trust_env=False) as client:
            page = self.request(client, f'projects/{PROJECT}/queries', body=body)
            deadline = time.monotonic() + 35
            while not page.get('jobComplete'):
                ref = page.get('jobReference', {})
                if not ref.get('jobId') or time.monotonic() > deadline:
                    raise BillingError('Billing query is not ready. Try again later.')
                time.sleep(1)
                page = self.request(client, f'projects/{PROJECT}/queries/{ref["jobId"]}',
                                    params={'location': LOCATION, 'timeoutMs': 1000, 'maxResults': 10000})
            # Aggregated two-month results are small. Refuse a truncated total.
            if page.get('pageToken') or int(page.get('totalRows', 0)) > len(page.get('rows', [])):
                raise BillingError('Billing response exceeded the display limit; no partial totals are shown.')
            fields = [field['name'] for field in page.get('schema', {}).get('fields', [])]
            rows = [dict(zip(fields, [cell['v'] for cell in row['f']])) for row in page.get('rows', [])]
        return summarize(rows, now)


class BillingCache:
    """One shared query per hour on demand, including across concurrent visitors."""
    def __init__(self, reader=None, clock=time.monotonic):
        self.reader = reader or BillingReader()
        self.clock = clock
        self.lock = threading.Lock()
        self.data, self.deadline, self.error, self.retry_at = None, 0, None, 0

    def get(self):
        with self.lock:
            now = self.clock()
            if self.data is not None and now < self.deadline:
                return {**self.data, 'stale': False}
            if now >= self.retry_at:
                try:
                    data = self.reader.read()
                    self.data, self.deadline, self.error = data, self.clock() + CACHE_SECONDS, None
                except Exception as exc:
                    # Never return raw auth/subprocess/HTTP exceptions or tokens.
                    self.error = str(exc) if isinstance(exc, BillingError) else 'Billing unavailable; check the local gcloud login.'
                    self.retry_at = self.clock() + ERROR_COOLDOWN
            if self.error:
                if self.data is not None:
                    return {**self.data, 'stale': True, 'warning': self.error}
                raise BillingError(self.error)
            return {**self.data, 'stale': False}
