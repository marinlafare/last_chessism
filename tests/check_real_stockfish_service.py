"""Explicit isolated smoke test using the pinned engine; no DB, Redis or network."""
import asyncio
import importlib
import unittest
from unittest.mock import patch

import chess
from analysis_failure_test_support import stockfish_analysis_module


class RealStockfishServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_search_mate_invalid_position_and_cancelled_process_replacement(self):
        service = stockfish_analysis_module()
        engines = importlib.import_module('operations.engine')
        pool = engines.EnginePool(1)
        self.addAsyncCleanup(pool.shutdown)
        mate = '7k/6Q1/6K1/8/8/8/8/8 b - -'
        with patch.object(service,'engine_pool',pool):
            result = await service.analyze_fens_endpoint(service.AnalysisRequest(
                fens=[chess.STARTING_FEN,mate,'invalid'],nodes_limit=100000))
            lines = result[0]['analysis']
            self.assertEqual(len(lines),4)
            self.assertTrue(all(isinstance(line['score'],int) for line in lines))
            self.assertEqual(result[1]['analysis']['score'],10000)
            self.assertFalse(result[2]['is_valid'])
            task = asyncio.create_task(service.analyze_fens_endpoint(service.AnalysisRequest(
                fens=[chess.STARTING_FEN],nodes_limit=100000000)))
            try:
                async def wait_busy():
                    while not pool.status()['workers']['busy']:
                        await asyncio.sleep(.001)
                await asyncio.wait_for(wait_busy(),2)
                transport = pool._slots[0].transport
                await asyncio.sleep(.01)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                self.assertIsNotNone(transport.get_returncode())
                self.assertEqual(pool.status()['workers']['busy'],0)
                self.assertIsNone(pool._slots[0].engine)
                result = await service.analyze_fens_endpoint(service.AnalysisRequest(fens=[mate]))
                self.assertEqual(result[0]['analysis']['score'],10000)
                self.assertTrue(pool.status()['ok'])
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task,return_exceptions=True)


if __name__ == '__main__':
    unittest.main(verbosity=2)
