"""Offline read-only retry policy: no network, credentials or database."""
import threading
import unittest
from unittest.mock import Mock
from google.api_core import exceptions as google
from requests import exceptions as requests
from chessism_api.operations.cloud_analysis.read_recovery import RecoveringStorage, transient_read_error


class ReadRecoveryTests(unittest.TestCase):
    def test_transient_errors_only(self):
        for error in [TimeoutError(), requests.ReadTimeout(), requests.ConnectionError(),
                      google.ServiceUnavailable('temporary'), google.TooManyRequests('rate'),
                      google.RetryError('deadline', requests.ReadTimeout())]:
            self.assertTrue(transient_read_error(error), type(error))
        for error in [ValueError('checksum'), PermissionError(), requests.SSLError(),
                      google.Forbidden('denied'), google.NotFound('missing'),
                      google.PreconditionFailed('generation changed'), RuntimeError('unknown')]:
            self.assertFalse(transient_read_error(error), type(error))

    def test_retries_same_read_and_records_recovery_without_replaying_mutations(self):
        storage, events = Mock(), []
        storage.read.side_effect = [requests.ReadTimeout('secret-token'), TimeoutError(), b'ok']
        adapter = RecoveringStorage(storage, lambda **event: events.append(event), threading.Event(), delays=(0, 0))
        self.assertEqual(adapter.read('gs://bucket/batches/000000.json', 123), b'ok')
        self.assertEqual([e['kind'] for e in events], ['retry', 'retry', 'recovered'])
        self.assertEqual(len({e['episode_id'] for e in events}), 1)
        self.assertNotIn('secret-token', str(events))
        self.assertTrue(all(c.args == ('gs://bucket/batches/000000.json', 123) for c in storage.read.call_args_list))
        storage.create.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError): adapter.create('gs://bucket/object', b'data')
        storage.create.assert_called_once()

    def test_exhaustion_and_permanent_failures_remain_manual(self):
        for error, expected in [(TimeoutError(), 3), (ValueError('corrupt'), 1), (PermissionError(), 1)]:
            storage, events = Mock(), []
            storage.read.side_effect = error
            adapter = RecoveringStorage(storage, lambda **event: events.append(event), threading.Event(), delays=(0, 0))
            with self.assertRaises(type(error)): adapter.read('gs://bucket/contract.json')
            self.assertEqual(storage.read.call_count, expected)
            if expected == 3: self.assertEqual(events[-1]['kind'], 'exhausted')
            else: self.assertEqual(events, [])

    def test_shutdown_interrupts_backoff_without_another_read(self):
        storage, stopping = Mock(), threading.Event()
        storage.read.side_effect = TimeoutError()
        adapter = RecoveringStorage(storage, lambda **event: stopping.set(), stopping, delays=(40,))
        with self.assertRaises(InterruptedError): adapter.read('gs://bucket/contract.json')
        storage.read.assert_called_once()


if __name__ == '__main__': unittest.main()
