"""Billing panel tests use only mocked data/HTTP; no credentials or paid queries."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import unittest
from unittest.mock import Mock, patch

import httpx

from cloud_billing.reader import (BillingCache, BillingError, BillingReader, CACHE_SECONDS,
                                  MAX_BYTES, PROJECT, SQL, periods, summarize)

NOW = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)


def record(cost='0.1', credits='-0.02', day='2026-10-09', currency='USD', service='Compute Engine'):
    return dict(day=day, service=service, currency=currency, cost=cost, credits=credits,
                usage_through='2026-10-09T11:00:00Z', exported_at='2026-10-10T03:00:00Z')


class BillingTests(unittest.TestCase):
    def test_month_boundaries_and_exact_credits(self):
        self.assertEqual(periods(datetime(2027, 1, 1, tzinfo=timezone.utc))[1]['id'], '2026-12')
        data = summarize([record(), record('0.2', '-0.01'), record('10', '-10', service='Cloud Run'),
                          record('5', '0', currency='MXN'), record(day='2026-09-30')], NOW)
        usd = next(row for row in data['periods'][0]['currencies'] if row['currency'] == 'USD')
        self.assertEqual(usd['cost'], '10.3')
        self.assertEqual(usd['credits'], '-10.03')
        self.assertEqual(usd['net'], '0.27')
        self.assertEqual(usd['services'][0]['net'], '0.27')
        self.assertEqual(usd['days'][0]['net'], '0.27')
        self.assertEqual(len(data['periods'][0]['currencies']), 2)
        self.assertTrue(data['periods'][1]['has_data'])

    def test_no_rows_is_unknown_not_a_zero_bill(self):
        data = summarize([], NOW)
        self.assertFalse(data['periods'][0]['has_data'])
        self.assertEqual(data['periods'][0]['currencies'], [])
        self.assertIsNone(data['periods'][0]['exported_at'])

    def test_one_cached_read_for_concurrent_visitors_and_hourly_expiry(self):
        clock = Mock(return_value=100)
        reader = Mock()
        reader.read.return_value = summarize([record()], NOW)
        cache = BillingCache(reader, clock)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: cache.get(), range(12)))
        self.assertTrue(all(not row['stale'] for row in results))
        self.assertEqual(reader.read.call_count, 1)
        clock.return_value = 100 + CACHE_SECONDS
        cache.get()
        self.assertEqual(reader.read.call_count, 2)

    def test_failed_refresh_retains_stale_data_and_does_not_leak_errors_or_hammer_google(self):
        clock, reader = Mock(return_value=0), Mock()
        reader.read.return_value = summarize([record()], NOW)
        cache = BillingCache(reader, clock)
        cache.get()
        clock.return_value = CACHE_SECONDS + 1
        reader.read.side_effect = RuntimeError('secret token must not escape')
        for _ in range(5):
            data = cache.get()
            self.assertTrue(data['stale'])
            self.assertNotIn('secret', data['warning'])
        self.assertEqual(reader.read.call_count, 2)
        fresh = BillingCache(reader)
        with self.assertRaisesRegex(BillingError, 'Billing unavailable'):
            fresh.get()

    def test_fixed_query_limits_and_project_filter(self):
        tokens = Mock()
        tokens.get.return_value = ('fake-token', None)
        client = Mock()
        client.request.return_value = httpx.Response(200, json={'jobComplete': True, 'totalRows': '0'})
        with patch('cloud_billing.reader.httpx.Client') as factory:
            factory.return_value.__enter__.return_value = client
            result = BillingReader(tokens).read()
        args, kwargs = client.request.call_args
        self.assertEqual(args, ('POST', f'https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries'))
        body = kwargs['json']
        self.assertEqual(body['maximumBytesBilled'], str(MAX_BYTES))
        self.assertEqual(body['jobTimeoutMs'], '30000')
        self.assertFalse(body['useLegacySql'])
        self.assertTrue(body['useQueryCache'])
        self.assertEqual(body['query'], SQL)
        self.assertIn('project.id = @project', SQL)
        self.assertIn('_PARTITIONTIME >= TIMESTAMP(@start)', SQL)
        self.assertIn('FROM UNNEST(credits)', SQL)
        self.assertEqual(body['queryParameters'][0]['parameterValue']['value'], PROJECT)
        self.assertFalse(result['periods'][0]['has_data'])

    def test_truncated_results_are_rejected_not_shown_as_complete_costs(self):
        reader = BillingReader(Mock())
        for response in [{'jobComplete': True, 'pageToken': 'more'}, {'jobComplete': True, 'totalRows': '12', 'rows': []}]:
            with patch.object(reader, 'request', return_value=response), self.assertRaisesRegex(BillingError, 'partial totals'):
                reader.read()

    def test_401_refreshes_once_but_access_denied_is_not_a_zero_bill(self):
        tokens, client = Mock(), Mock()
        tokens.get.side_effect = [('old', None), ('new', None)]
        client.request.side_effect = [httpx.Response(401), httpx.Response(200, json={'jobComplete': True})]
        reader = BillingReader(tokens)
        self.assertTrue(reader.request(client, 'projects/test/queries', body={'requestId': 'same'})['jobComplete'])
        tokens.get.assert_called_with(force=True, rejected_token='old')
        self.assertEqual(client.request.call_args_list[0].kwargs['json'], client.request.call_args_list[1].kwargs['json'])
        tokens.get.side_effect = None
        tokens.get.return_value = ('token', None)
        client.request.side_effect = None
        client.request.return_value = httpx.Response(403, json={'error': 'DO NOT EXPOSE'})
        with self.assertRaisesRegex(BillingError, 'access denied'):
            reader.request(client, 'projects/test/queries')


if __name__ == '__main__':
    unittest.main()
