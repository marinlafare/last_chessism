"""Synthetic HTTP envelopes only. Never contacts Google or deletes real data."""
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock, patch
import threading
import unittest
from urllib.parse import parse_qs, urlsplit, unquote
import requests

from cleaning_job.cloud import Cloud, CleanupError
from cleaning_job.batch_transport import BatchTransport, encode_batch, decode_batch, retry_delay
from cleaning_job.deletion import remove_objects


def objects(count=3):
    return [{'name': f'results/test/part {i}.json', 'generation': str(100+i), 'size': '1'} for i in range(count)]


def reply(statuses, *, ids=None):
    parts = []
    for ident, status in zip(ids or [f'delete-{i}' for i in range(len(statuses))], statuses):
        parts.append(f'--response\r\nContent-Type: application/http\r\nContent-ID: <response-{ident}>\r\n\r\n'
                     f'HTTP/1.1 {status} Test\r\n\r\n')
    return 200, {'Content-Type': 'multipart/mixed; boundary=response'}, (''.join(parts)+'--response--\r\n').encode()


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.cloud = Cloud()
        self.cloud.tokens = Mock()
        self.cloud.tokens.get.return_value = ('test-token', None)
        self.abort = Mock(spec=threading.Event)
        self.abort.is_set.return_value = False
        self.abort.wait.return_value = False
        self.transport = BatchTransport(self.cloud, self.abort)
        self.addCleanup(self.transport.close)

    def test_exact_generation_and_encoding_and_batch_limits(self):
        raw, content_type, identities = encode_batch(objects())
        self.assertIn('multipart/mixed;', content_type)
        lines = [line for line in raw.decode().split('\r\n') if line.startswith('DELETE ')]
        self.assertEqual(len(lines), 3)
        for item, line in zip(objects(), lines):
            target = urlsplit(line.split(' ')[1])
            self.assertEqual(parse_qs(target.query), {'generation': [item['generation']],
                'ifGenerationMatch': [item['generation']]})
            self.assertTrue(unquote(target.path).endswith('/'+item['name']))
        for group in [[], objects(101), [{'name': 'test', 'generation': ''}],
                      [{'name': 'test', 'generation': 1}]]:
            with self.assertRaises(ValueError): encode_batch(group)

    def test_each_response_is_matched_and_missing_duplicate_unknown_are_rejected(self):
        _, _, identities = encode_batch(objects())
        _, headers, raw = reply([404, 204, 503], ids=['delete-2', 'delete-0', 'delete-1'])
        parsed = decode_batch(headers['Content-Type'], raw, identities)
        self.assertEqual(parsed['delete-0'][0], 204)
        for ids in [['delete-0'], ['delete-0', 'delete-0', 'delete-2'], ['delete-0', 'delete-1', 'delete-9']]:
            _, headers, raw = reply([204]*len(ids), ids=ids)
            with self.assertRaises(ValueError): decode_batch(headers['Content-Type'], raw, identities)

    def test_partial_success_retries_only_failed_objects(self):
        self.transport.post = Mock(side_effect=[reply([204, 503, 404]), reply([204])])
        ack = Mock()
        self.transport.remove(objects(), ack)
        self.assertEqual([o['name'] for o in ack.call_args_list[0].args[0]], [objects()[0]['name'], objects()[2]['name']])
        second = self.transport.post.call_args_list[1].args[0].decode()
        self.assertEqual(second.count('DELETE '), 1)
        self.assertIn('generation=101&ifGenerationMatch=101', second)
        self.assertEqual(sum(len(c.args[0]) for c in ack.call_args_list), 3)

    def test_permission_and_generation_conflicts_preserve_success_but_do_not_retry(self):
        for code in [403, 412]:
            with self.subTest(code=code):
                self.transport.post = Mock(return_value=reply([204, code]))
                ack = Mock()
                with self.assertRaises(CleanupError): self.transport.remove(objects(2), ack)
                self.assertEqual(self.transport.post.call_count, 1)
                self.assertEqual(ack.call_args.args[0], objects(1))

    def test_lost_response_and_broken_envelope_retry_pinned_objects(self):
        self.transport.post = Mock(side_effect=[requests.Timeout(),
            (200, {'Content-Type': 'text/html'}, b'bad'), reply([204])])
        ack = Mock()
        self.transport.remove(objects(1), ack)
        self.assertEqual(self.transport.post.call_count, 3)
        ack.assert_called_once_with(objects(1))
        for call in self.transport.post.call_args_list:
            self.assertIn(b'generation=100&ifGenerationMatch=100', call.args[0])

    def test_outer_missing_is_not_success_and_auth_refresh_is_bounded(self):
        self.transport.post = Mock(return_value=(404, {}, b''))
        ack = Mock()
        with self.assertRaises(CleanupError): self.transport.remove(objects(1), ack)
        ack.assert_not_called()
        self.transport.post = Mock(side_effect=[reply([401]), reply([204])])
        self.transport.remove(objects(1), ack)
        self.cloud.tokens.get.assert_called_with(force=True, rejected_token='test-token')

    def test_retry_budget_and_retry_after_are_bounded(self):
        self.transport.post = Mock(return_value=reply([429]))
        with self.assertRaisesRegex(CleanupError, 'budget'): self.transport.remove(objects(1), Mock())
        self.assertEqual(self.transport.post.call_count, 4)
        self.assertGreaterEqual(retry_delay(0, ['4']), 4)
        with self.assertRaises(CleanupError): retry_delay(0, ['31'])

    def test_progress_failure_is_not_retried_as_a_network_failure(self):
        self.transport.post = Mock(return_value=reply([204]))
        ack = Mock(side_effect=ValueError('durable acknowledgement failed'))
        with self.assertRaisesRegex(ValueError, 'acknowledgement'):
            self.transport.remove(objects(1), ack)
        self.assertEqual(self.transport.post.call_count, 1)
        ack.assert_called_once()

    def test_abort_does_not_start_another_request(self):
        self.abort.is_set.return_value = True
        self.transport.post = Mock()
        with self.assertRaisesRegex(CleanupError, 'interrupted'):
            self.transport.remove(objects(1), Mock())
        self.transport.post.assert_not_called()

    def test_pool_reuses_one_session_on_a_worker_without_hidden_retries(self):
        sessions = []
        class Session:
            def __init__(self): self.closed = False; self.adapters = []; sessions.append(self)
            def mount(self, prefix, adapter): self.adapters.append(adapter)
            def close(self): self.closed = True
            def post(self, url, **kwargs):
                self.options = kwargs
                code, headers, body = reply([204])
                response = Mock(status_code=code, headers=headers)
                response.iter_content.return_value = [body]
                response.__enter__ = Mock(return_value=response)
                response.__exit__ = Mock(return_value=False)
                return response
        transport = BatchTransport(self.cloud, self.abort, session_factory=Session)
        try:
            transport.remove(objects(1), Mock())
            transport.remove(objects(1), Mock())
            self.assertEqual(len(sessions), 1)
            self.assertEqual(sessions[0].adapters[0].max_retries.total, 0)
            self.assertFalse(sessions[0].options['allow_redirects'])
        finally: transport.close()
        self.assertTrue(sessions[0].closed)


class QueueTests(unittest.TestCase):
    def test_rolling_batches_do_not_wait_for_the_slowest_batch(self):
        items, entered, callbacks = objects(301), threading.Event(), []
        plan = {'sha256': 'a'*64, 'objects': items}
        class Transport:
            requests = retries = 0
            def __init__(self, *args): pass
            def close(self): pass
            def remove(self, group, acknowledge):
                if group[0]['generation'] == '100':
                    if not entered.wait(2): raise AssertionError('Queue waited for slow batch')
                if group[0]['generation'] == '300': entered.set()
                acknowledge(group)
        with patch('cleaning_job.deletion.BatchTransport', Transport):
            result = remove_objects(Cloud(), plan, items, concurrency=2,
                timings=SimpleNamespace(cleanup_progress=lambda *args: callbacks.append(args)))
        self.assertEqual(result['removed'], 301)
        self.assertEqual(callbacks[-1][-1], 301)

    def test_resume_counts_absent_versions_and_rejects_new_generations(self):
        items = objects()
        cloud = SimpleNamespace(delete_object=Mock())
        plan = {'sha256': 'a'*64, 'objects': items}
        callbacks = []
        remove_objects(cloud, plan, items[1:], timings=SimpleNamespace(cleanup_progress=lambda *args: callbacks.append(args)))
        self.assertEqual(callbacks[0][-1], 1)
        self.assertEqual(callbacks[-1][-1], 3)
        cloud.delete_object.reset_mock()
        with self.assertRaises(CleanupError):
            remove_objects(cloud, plan, [{**items[0], 'generation': '999'}])
        cloud.delete_object.assert_not_called()


if __name__ == '__main__': unittest.main()
