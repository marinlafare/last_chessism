"""Offline CPU validation equivalence, responsiveness, and timing."""
import asyncio
import hashlib
import json
import time
import unittest
from chessism_api.operations.cloud_analysis import runtime, input_validation as validation
from stockfish_batch.checkpoints import encode, parse_input

BASE = 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq -'


class InputValidationTests(unittest.TestCase):
    def test_matches_existing_wire_validation(self):
        fens=[BASE,BASE+' 0 1','8/8/8/8/8/8/4k3/6K1 w - -']
        expected=[{'id':hashlib.sha256(f.encode()).hexdigest(),'fen':f} for f in fens]
        self.assertEqual(validation.validate(fens),parse_input(b''.join(map(encode,expected)),len(fens)))
        for bad in [None,'invalid','8/8/8/8/8/8/8/8 w - -',BASE+' invalid counters','x'*257]:
            with self.subTest(fen=bad),self.assertRaises((ValueError,TypeError)): validation.validate([bad])
        with self.assertRaisesRegex(ValueError,'Duplicate'):validation.validate([BASE,BASE])


class InputWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_validator_matches_old_results_without_blocking_event_loop(self):
        fens=[BASE+f' {i} {i+1}' for i in range(50_000)]
        # Warm the same four-worker pool used by subsequent reservation pages.
        await validation.prepare(fens[:40_000])
        beats=0
        async def heartbeat():
            nonlocal beats
            while True:
                beats+=1
                await asyncio.sleep(.01)
        task=asyncio.create_task(heartbeat())
        try:
            started=time.perf_counter();actual=await validation.prepare(fens);parallel=time.perf_counter()-started
        finally:
            task.cancel()
            try:await task
            except asyncio.CancelledError:pass
        self.assertGreater(beats,5)
        started=time.perf_counter()
        old=[{'id':hashlib.sha256(f.encode()).hexdigest(),'fen':f} for f in fens]
        expected=parse_input(b''.join(map(encode,old)),len(fens))
        serial=time.perf_counter()-started
        self.assertEqual(actual,expected)
        print(json.dumps({'synthetic_fens':len(fens),'old_serial_seconds':round(serial,3),
            'parallel_seconds':round(parallel,3),'responsive_heartbeat_ticks':beats,'identical':True}),flush=True)
        validation.shutdown()
